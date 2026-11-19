# Setup: instance, bucket, secrets, stack

This guide takes an empty AWS account to a running recorder that uploads one object per stream per hour to
S3. Everything is driven from `infra/terraform/` and `deploy/`. Nothing here runs on its own: you apply the
Terraform, you set the secrets, you start the stack.

What gets created:

| Resource | Details |
|---|---|
| EC2 instance | `t4g.small` (2 vCPU, 2 GB, Graviton), Amazon Linux 2023 arm64, 50 GB gp3 (encrypted), IMDSv2 only, default VPC |
| Security group | SSH (22) from one CIDR; all egress |
| S3 bucket | versioning on, public access blocked, TLS only, SSE-S3; `raw/` moves to STANDARD_IA after 30 days; overwritten versions expire after 90 days |
| IAM role / instance profile | `s3:ListBucket` on the bucket, `s3:GetObject` + `s3:PutObject` on `raw/*`, `curated/*` and `reports/*`, `ssm:GetParameter*` under `/book-recorder/*`, `cloudwatch:PutMetricData` restricted to the `BookRecorder` namespace (needed for the alarms) |
| SSM parameters (SecureString) | `/book-recorder/kalshi/key-id`, `/book-recorder/kalshi/private-key`, `/book-recorder/hf/token`, `/book-recorder/polygon/rpc-url` (optional), `/book-recorder/ghcr/token` (optional), created with the placeholder `UNSET` |
| SNS topic + email subscription | receives the alarms |
| CloudWatch alarms | health check failed or missing for 5 min; root disk above 80 %; EC2 status check failed |

Rough cost at list price (us-east-1 / eu-west-1): t4g.small ≈ $12/month, 50 GB gp3 ≈ $4/month, plus S3
storage and requests, a few cents of CloudWatch custom metrics, and egress. The instance runs with
`cpu_credits = "unlimited"`, which only costs extra if the 24 h average CPU stays above the t4g baseline.

## 0. Prerequisites

- Terraform ≥ 1.6 and AWS CLI v2 on your machine, with credentials for the target account
  (`aws sts get-caller-identity` should print it).
- An EC2 key pair in the target region (EC2 console → Key pairs, or
  `aws ec2 create-key-pair --key-name book-recorder --query KeyMaterial --output text > book-recorder.pem`).
- Your public IP for SSH (`curl -s https://checkip.amazonaws.com`), used as `/32`.
- The container image on GHCR: push to `main` and the `image` workflow builds
  `ghcr.io/sudden-stop/book-recorder` for amd64 and arm64. If the package is private, either make it public
  (GitHub → Packages → book-recorder → Package settings → Change visibility) or create a classic token with
  only `read:packages` for step 3.

**Region.** Pick a region from which both venues' APIs answer. Polymarket blocks some countries, and this
repo's earlier work could not reach it from Italy at all, so check from the instance after step 2
(the commands are in step 5) before relying on it. The region cannot change without recreating everything.

## 1. Configure and apply the Terraform

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`: `region`, `key_name`, `allowed_ssh_cidr`, `alert_email`. Optional: `instance_type`
(an x86 type such as `t3.small` picks the x86_64 AMI automatically on first create), `root_volume_gb`, `bucket_name`
(default `book-recorder-<account>-<region>`), `host_id`, `image`.

```bash
terraform init
```

```bash
terraform plan -out tf.plan
```

Read the plan: about 20 resources, all new, no secrets in it (parameters show the value as sensitive).

```bash
terraform apply tf.plan
```

Then:

- Accept the SNS confirmation email (subject "AWS Notification - Subscription Confirmation"); until then no
  alarm mail arrives.
- `terraform output` prints the SSH command, bucket and parameter names.
- The state file `terraform.tfstate` is local and git-ignored. Keep it safe (or move it to an S3 backend, see
  `versions.tf`). It does not contain secret values, because Terraform never sees them.

The health alarm goes to ALARM a few minutes after apply, because the recorder is not running yet. That is
expected and clears in step 4.

## 2. Wait for first-boot setup

User data installs Docker, the compose plugin (v2.29.7, checksum-verified) and chrony, writes
`/opt/book-recorder/` (compose file, scripts, `.env`), installs the systemd units and enables them. It does
**not** start the recorder.

```bash
ssh -i book-recorder.pem ec2-user@<public_dns>
```

```bash
sudo tail -n 5 /var/log/book-recorder-setup.log
```

The last line should be `book-recorder host setup done`. Also check:

```bash
docker compose version && chronyc tracking | head -n 4 && cat /opt/book-recorder/.env
```

`chronyc tracking` should show `Reference ID ... (169.254.169.123)`, the Amazon Time Sync Service.

## 3. Set the secrets (from your machine)

Values go straight from your machine to SSM; they never enter the repo, Terraform, or shell history if you
read them from files. Use the same region as the Terraform.

Kalshi key id and private key (the PEM file Kalshi let you download when you created the key):

```bash
aws ssm put-parameter --region <region> --overwrite --type SecureString --name /book-recorder/kalshi/key-id --value "<kalshi-key-id>"
```

```bash
aws ssm put-parameter --region <region> --overwrite --type SecureString --name /book-recorder/kalshi/private-key --value file://kalshi-private-key.pem
```

Hugging Face token (write access to the private dataset repo):

```bash
aws ssm put-parameter --region <region> --overwrite --type SecureString --name /book-recorder/hf/token --value file://hf-token.txt
```

Polygon RPC URL with its key (for example an Alchemy `https://polygon-mainnet.g.alchemy.com/v2/<key>`). With it
the nightly run checks every Polymarket hour of the `activity` feed against the on-chain fills
(`reports/chain/`, see `pipeline/src/book_pipeline/chain_check.py`). Without it that check is off. Alchemy's free
plan is enough (10-block `eth_getLogs` ranges; about 4,300 calls a day, some 8M of its 30M monthly compute units):

```bash
aws ssm put-parameter --region <region> --overwrite --type SecureString --name /book-recorder/polygon/rpc-url --value file://polygon-rpc.txt
```

Only if the GHCR images are private:

```bash
aws ssm put-parameter --region <region> --overwrite --type SecureString --name /book-recorder/ghcr/token --value file://ghcr-token.txt
```

Notes:

- `file://` keeps the PEM's line breaks. A file with a trailing newline is fine.
- Standard parameters hold up to 4 KB; an RSA-2048 or RSA-4096 PEM fits.
- Any parameter left at `UNSET` is skipped: without the Kalshi pair the recorder runs Polymarket only,
  without the HF token the publish step is unavailable.
- Re-running `terraform apply` never overwrites these values (`ignore_changes = [value]`).

## 4. Start the stack (on the instance)

```bash
sudo systemctl start book-recorder
```

This runs `bin/fetch-secrets.sh` (SSM → `recorder.secrets.env`, `pipeline.secrets.env`,
`secrets/kalshi.pem`, optional `docker login ghcr.io`), pulls the image and runs
`docker compose up -d recorder`. The unit is enabled, so it comes back after a reboot; the container itself
has `restart: always`.

```bash
cd /opt/book-recorder && docker compose ps && docker compose logs --tail 50 recorder
```

```bash
curl -s localhost:8080/healthz
```

The container turns `healthy` after the first writes (the healthcheck has a 2 minute start period).

## 5. Check it end to end

From the instance, the venues are reachable:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://gamma-api.polymarket.com/markets?limit=1
```

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://external-api.kalshi.com/trade-api/v2/exchange/status
```

Local segments are being written:

```bash
sudo find /var/lib/book-recorder/data -name '*.ndjson.zst' -mmin -10
```

After the first hour closes (a few minutes past the hour), objects appear in S3 (from your machine):

```bash
aws s3 ls --recursive s3://<bucket>/raw/ | tail
```

Keys look like `raw/venue=polymarket/stream=<stream>/dt=2026-11-16/hour=13/rec-a.ndjson.zst`, with a manifest
line per object.

Metrics and alarms: `systemctl list-timers 'book-recorder*'` shows the per-minute metrics timer and the
nightly pipeline timer; the `book-recorder-health-check-failed` alarm should be back to OK within ~5 minutes.

## Day-to-day

| Task | Command (on the instance, in `/opt/book-recorder`) |
|---|---|
| New recorder image | `sudo systemctl restart book-recorder` (pulls `latest`), or pin a tag with `RECORDER_IMAGE=ghcr.io/sudden-stop/book-recorder:sha-abc1234` in `.env` |
| Rotated a secret | `sudo systemctl reload book-recorder` |
| Change recorder config | add `BR_*` variables to `.env` (see `recorder/src/config.rs`), then `sudo systemctl reload book-recorder` |
| Run the pipeline now (yesterday) | `sudo systemctl start book-recorder-pipeline` |
| Re-run one day | `sudo docker compose --profile nightly run --rm pipeline book-pipeline nightly --dt 2026-11-16` (add `--force` to redo hours already curated) |
| Pipeline logs | `journalctl -u book-recorder-pipeline -n 200`; the day's summary is `s3://<bucket>/reports/nightly/dt=<day>.json` |
| Stop recording | `sudo systemctl stop book-recorder` (60 s grace to flush the open hour) |

The pipeline container is capped at 1 GB of memory and 1.5 CPUs and runs at low CPU and IO priority at
02:30 UTC. It runs `book-pipeline nightly`: for each venue (polymarket, kalshi, cross) it curates every hour
of yesterday (UTC) that has raw data in the bucket into `curated/`, validates it, and writes reports under
`reports/`. Hours already curated are skipped, so a rerun after a crash picks up where it stopped. A failing
hour does not stop the others; it is listed in the summary and the job exits 1, which shows up as a failed
`book-recorder-pipeline` unit. Publishing to Hugging Face stays a manual `book-pipeline publish` step.

**Changing files under `deploy/`.** User data only runs on first boot, and Terraform ignores later changes to
it (so a new AMI or an edited script never restarts the recorder). Copy changed files by hand:

```bash
scp -i book-recorder.pem deploy/docker-compose.yml ec2-user@<public_dns>:/tmp/ && ssh -i book-recorder.pem ec2-user@<public_dns> 'sudo mv /tmp/docker-compose.yml /opt/book-recorder/ && sudo systemctl reload book-recorder'
```

To rebuild the host from scratch instead: `terraform apply -replace=aws_instance.recorder` (local segments
not yet uploaded are lost; the bucket and parameters stay).

**Bigger instance.** Set `instance_type` (e.g. `t4g.medium`) and apply; AWS stops and starts the instance,
the public IP changes, and the stack comes back by itself. Moving between arm64 and x86 (e.g. to `t3.small`)
also needs a new AMI, which Terraform ignores on purpose: apply with `-replace=aws_instance.recorder`.

## Tearing down

`terraform destroy` removes everything except a non-empty bucket, which AWS refuses to delete. That is
deliberate: empty it yourself first if you really mean to delete the data.

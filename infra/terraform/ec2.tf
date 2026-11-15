# Pick the AMI architecture from the instance type, so changing instance_type is enough.
data "aws_ec2_instance_type" "this" {
  instance_type = var.instance_type
}

locals {
  arch = contains(data.aws_ec2_instance_type.this.supported_architectures, "arm64") ? "arm64" : "x86_64"

  deploy_dir = "${path.module}/../../deploy"

  # Files written onto the host by user data: host path => source under deploy/.
  host_files = {
    "/opt/book-recorder/docker-compose.yml"              = "docker-compose.yml"
    "/opt/book-recorder/bin/fetch-secrets.sh"            = "host/fetch-secrets.sh"
    "/opt/book-recorder/bin/report-metrics.sh"           = "host/report-metrics.sh"
    "/etc/systemd/system/book-recorder.service"          = "host/book-recorder.service"
    "/etc/systemd/system/book-recorder-metrics.service"  = "host/book-recorder-metrics.service"
    "/etc/systemd/system/book-recorder-metrics.timer"    = "host/book-recorder-metrics.timer"
    "/etc/systemd/system/book-recorder-pipeline.service" = "host/book-recorder-pipeline.service"
    "/etc/systemd/system/book-recorder-pipeline.timer"   = "host/book-recorder-pipeline.timer"
    "/etc/docker/daemon.json"                            = "host/docker-daemon.json"
  }

  # A Windows checkout with core.autocrlf=true has CRLF files; the Linux host needs LF.
  host_file_contents = {
    for dest, src in local.host_files : dest => replace(file("${local.deploy_dir}/${src}"), "\r\n", "\n")
  }

  # Non-secret runtime config; becomes /opt/book-recorder/.env (read by compose for
  # interpolation and passed to the containers).
  host_env = {
    AWS_REGION        = var.region
    BR_S3_BUCKET      = aws_s3_bucket.data.bucket
    BR_S3_REGION      = var.region
    BR_HOST_ID        = var.host_id
    SSM_PREFIX        = var.ssm_prefix
    RECORDER_IMAGE    = var.image
    PIPELINE_IMAGE    = var.pipeline_image
    METRICS_NAMESPACE = local.metrics_namespace
  }

  user_data = replace(templatefile("${path.module}/user_data.sh.tftpl", {
    files            = { for dest, content in local.host_file_contents : dest => base64encode(content) }
    host_env         = local.host_env
    compose_version  = "v2.29.7"
    compose_platform = local.arch == "arm64" ? "aarch64" : "x86_64"
  }), "\r\n", "\n")
}

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${local.arch}"
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "instance" {
  name        = "${var.project}-instance"
  description = "SSH from one CIDR; all egress."
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.instance.id
  description       = "SSH"
  cidr_ipv4         = var.allowed_ssh_cidr
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.instance.id
  description       = "All outbound (venues, S3, SSM, GHCR, Hugging Face)."
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_instance" "recorder" {
  ami                    = data.aws_ssm_parameter.al2023_ami.value
  instance_type          = var.instance_type
  key_name               = var.key_name
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  vpc_security_group_ids = [aws_security_group.instance.id]
  user_data              = local.user_data
  monitoring             = false

  # Burstable (t*) types only: "unlimited" avoids throttling when the nightly job spends credits
  # (small extra charge only if the 24 h average exceeds the baseline).
  dynamic "credit_specification" {
    for_each = startswith(var.instance_type, "t") ? [1] : []
    content {
      cpu_credits = "unlimited"
    }
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_tokens = "required"
    # 2 hops so containers (bridge network) can reach IMDSv2 for the instance role.
    http_put_response_hop_limit = 2
    http_endpoint               = "enabled"
  }

  tags = {
    Name = var.project
  }

  lifecycle {
    # A new AL2023 release or an edited deploy/ file must not stop or replace the running
    # recorder. Ship compose/script changes with scp (docs/setup.md); taint to rebuild.
    ignore_changes = [ami, user_data]
  }
}

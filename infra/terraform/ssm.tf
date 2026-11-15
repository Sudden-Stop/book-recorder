# Secrets live in SSM Parameter Store as SecureString (default aws/ssm KMS key).
# Terraform creates each parameter with a placeholder and then ignores its value, so the real
# secret is set out of band (aws ssm put-parameter --overwrite, see docs/setup.md) and never
# appears in code or in a plan. It does end up in the state file only if you import or change
# the value through Terraform, which this setup never does.

locals {
  placeholder = "UNSET"

  secrets = {
    "kalshi/key-id"      = "Kalshi API key id (BR_KALSHI_KEY_ID)."
    "kalshi/private-key" = "Kalshi RSA private key, PEM (written to a file for BR_KALSHI_PRIVATE_KEY_FILE)."
    "hf/token"           = "Hugging Face write token for the private dataset (HF_TOKEN)."
    "ghcr/token"         = "Optional: GitHub token with read:packages, only needed while the GHCR images are private."
  }
}

resource "aws_ssm_parameter" "secret" {
  for_each = local.secrets

  name        = "${var.ssm_prefix}/${each.key}"
  description = each.value
  type        = "SecureString"
  value       = local.placeholder

  lifecycle {
    ignore_changes = [value]
  }
}

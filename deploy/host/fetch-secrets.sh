#!/bin/bash
# Reads the SecureString parameters under $SSM_PREFIX (instance role) and writes them where
# docker-compose.yml expects them. A parameter still holding the Terraform placeholder
# ("UNSET") is skipped, so e.g. the recorder runs Polymarket-only until the Kalshi key is set.
set -euo pipefail

dir=/opt/book-recorder
# shellcheck disable=SC1091
set -a; source "$dir/.env"; set +a
: "${SSM_PREFIX:?}" "${AWS_REGION:?}"

get() {
  aws ssm get-parameter --region "$AWS_REGION" --with-decryption \
    --name "$SSM_PREFIX/$1" --query Parameter.Value --output text 2>/dev/null || echo UNSET
}
is_set() { [[ -n "$1" && "$1" != "UNSET" ]]; }

umask 077
mkdir -p "$dir/secrets"

kalshi_id=$(get kalshi/key-id)
kalshi_key=$(get kalshi/private-key)
hf_token=$(get hf/token)
ghcr_token=$(get ghcr/token)

rec_env="$dir/recorder.secrets.env.tmp"
: > "$rec_env"
if is_set "$kalshi_id" && is_set "$kalshi_key"; then
  printf '%s\n' "$kalshi_key" > "$dir/secrets/kalshi.pem"
  # The container runs as uid 10001 and mounts secrets/ read-only.
  chown 10001:10001 "$dir/secrets/kalshi.pem"
  chmod 0400 "$dir/secrets/kalshi.pem"
  {
    echo "BR_KALSHI_KEY_ID=$kalshi_id"
    echo "BR_KALSHI_PRIVATE_KEY_FILE=/run/secrets/kalshi.pem"
  } >> "$rec_env"
  echo "fetch-secrets: Kalshi credentials present"
else
  rm -f "$dir/secrets/kalshi.pem"
  echo "fetch-secrets: Kalshi credentials not set; Kalshi stays disabled"
fi
mv "$rec_env" "$dir/recorder.secrets.env"

pipe_env="$dir/pipeline.secrets.env.tmp"
: > "$pipe_env"
if is_set "$hf_token"; then
  echo "HF_TOKEN=$hf_token" >> "$pipe_env"
else
  echo "fetch-secrets: Hugging Face token not set; publish step will be unavailable"
fi
mv "$pipe_env" "$dir/pipeline.secrets.env"

# Docker resolves 10001 inside the container; the files themselves stay root-only on the host.
chmod 0600 "$dir/recorder.secrets.env" "$dir/pipeline.secrets.env"

if is_set "$ghcr_token"; then
  # Any GitHub user name works with a token; the token decides access.
  printf '%s' "$ghcr_token" | docker login ghcr.io -u token --password-stdin >/dev/null
  echo "fetch-secrets: logged in to ghcr.io"
fi

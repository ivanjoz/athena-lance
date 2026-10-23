#!/usr/bin/env bash
# Builds the service for Lambda (arm64, provided.al2023) and creates or updates the
# control and ingest functions (ingest also serves queries). Every value comes from config.toml.
#
#   deploy/deploy.sh            build + deploy the two functions
#   deploy/deploy.sh --no-build deploy the last build
set -euo pipefail
# The cross-link to arm64 (zig) opens more files than the default limit allows.
ulimit -n 65536

root="$(cd "$(dirname "$0")/.." && pwd)"
cfg="$root/config.toml"
toml() { python3 -c 'import sys,tomllib; d=tomllib.load(open(sys.argv[1],"rb"))
for k in sys.argv[2].split("."): d=d[k]
print(d)' "$cfg" "$1"; }

export AWS_PROFILE="$(toml aws.profile)" AWS_REGION="$(toml aws.region)"
role="$(toml lambda.role_arn)"
out="$root/target/lambda-deploy"

if [[ "${1:-}" != "--no-build" ]]; then
  # lambda-dev: thin LTO + parallel codegen + incremental. The "lambda" profile (fat LTO, one
  # codegen unit) links on a single core for 20+ minutes and needs more RAM than a dev machine has.
  (cd "$root" && cargo lambda build --profile lambda-dev --arm64 -p lance-service --bin bootstrap)
fi
bin="$root/target/lambda/bootstrap/bootstrap"
[[ -x "$bin" ]] || { echo "missing $bin" >&2; exit 1; }

rm -rf "$out" && mkdir -p "$out/pkg"
cp "$bin" "$out/pkg/bootstrap"
cp "$cfg" "$out/pkg/config.toml"
(cd "$out/pkg" && zip -q -9 "$out/function.zip" bootstrap config.toml)
echo "package: $(du -h "$out/function.zip" | cut -f1)"
code_bucket="$(toml lambda.code_bucket)" code_key="$(toml lambda.code_key)"
aws s3 cp --only-show-errors "$out/function.zip" "s3://$code_bucket/$code_key"
code="S3Bucket=$code_bucket,S3Key=$code_key"

env_json='Variables={LANCE_CONFIG=/var/task/config.toml,RUST_LOG=info}'

deploy() { # name memory timeout ephemeral
  local name="$1" mem="$2" timeout="$3" eph="$4"
  if aws lambda get-function --function-name "$name" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$name" --s3-bucket "$code_bucket" --s3-key "$code_key" \
      --architectures arm64 --query 'LastUpdateStatus' --output text
    aws lambda wait function-updated-v2 --function-name "$name"
    aws lambda update-function-configuration --function-name "$name" --role "$role" \
      --memory-size "$mem" --timeout "$timeout" --ephemeral-storage "Size=$eph" \
      --environment "$env_json" --query 'LastUpdateStatus' --output text
  else
    aws lambda create-function --function-name "$name" --runtime provided.al2023 --architectures arm64 \
      --handler bootstrap --role "$role" --code "$code" \
      --memory-size "$mem" --timeout "$timeout" --ephemeral-storage "Size=$eph" \
      --environment "$env_json" --query 'State' --output text
  fi
  aws lambda wait function-updated-v2 --function-name "$name"
  echo "deployed $name"
}

deploy "$(toml lambda.control_function)" "$(toml lambda.control.memory_mb)" "$(toml lambda.control.timeout_seconds)" 512
deploy "$(toml lambda.ingest_function)" "$(toml lambda.ingest.memory_mb)" "$(toml lambda.ingest.timeout_seconds)" "$(toml lambda.ingest.ephemeral_mb)"

#!/usr/bin/env bash
# One-time setup of the resources around the functions (idempotent):
#   - bucket policy letting the Lambda role open S3 Express sessions on the data bucket
#     (the shared role's IAM policies grant s3:* but not s3express:CreateSession)
#   - TTL on the ledger table, so batch records expire (partition records have no ttl)
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cfg="$root/config.toml"
toml() { python3 -c 'import sys,tomllib; d=tomllib.load(open(sys.argv[1],"rb"))
for k in sys.argv[2].split("."): d=d[k]
print(d)' "$cfg" "$1"; }

export AWS_PROFILE="$(toml aws.profile)" AWS_REGION="$(toml aws.region)"
bucket="$(toml storage.bucket)"
role="$(toml lambda.role_arn)"
account="$(toml aws.account_id)"
region="$(toml aws.region)"
ledger="$(toml ledger.dynamodb_table)"

policy=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "LanceServiceRole",
    "Effect": "Allow",
    "Principal": {"AWS": "$role"},
    "Action": "s3express:CreateSession",
    "Resource": "arn:aws:s3express:$region:$account:bucket/$bucket"
  }]
}
EOF
)
aws s3api put-bucket-policy --bucket "$bucket" --policy "$policy"
echo "bucket policy set on $bucket"

status=$(aws dynamodb describe-time-to-live --table-name "$ledger" --query 'TimeToLiveDescription.TimeToLiveStatus' --output text)
if [[ "$status" == "DISABLED" ]]; then
  aws dynamodb update-time-to-live --table-name "$ledger" --time-to-live-specification "Enabled=true,AttributeName=ttl" >/dev/null
  echo "TTL enabled on $ledger"
else
  echo "TTL on $ledger: $status"
fi

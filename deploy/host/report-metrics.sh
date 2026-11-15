#!/bin/bash
# Publishes HealthCheck (1/0) and DiskUsedPercent to CloudWatch; run every minute by
# book-recorder-metrics.timer. The alarms in infra/terraform/monitoring.tf read these.
set -uo pipefail

# shellcheck disable=SC1091
set -a; source /opt/book-recorder/.env; set +a

if curl -fsS -m 5 -o /dev/null http://127.0.0.1:8080/healthz; then health=1; else health=0; fi
disk=$(df --output=pcent / | tail -n 1 | tr -dc '0-9')

aws cloudwatch put-metric-data --region "$AWS_REGION" --namespace "${METRICS_NAMESPACE:-BookRecorder}" \
  --metric-data \
  "MetricName=HealthCheck,Value=$health,Unit=Count,Dimensions=[{Name=Host,Value=$BR_HOST_ID}]" \
  "MetricName=DiskUsedPercent,Value=$disk,Unit=Percent,Dimensions=[{Name=Host,Value=$BR_HOST_ID}]"

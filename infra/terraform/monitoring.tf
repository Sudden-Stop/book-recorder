resource "aws_sns_topic" "alerts" {
  name = "${var.project}-alerts"
}

# AWS mails a confirmation link; alarms are not delivered until it is accepted.
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Published every minute by deploy/host/report-metrics.sh (systemd timer on the host):
#   HealthCheck     1 if http://localhost:8080/healthz answered 200, else 0
#   DiskUsedPercent root filesystem usage
# Missing data counts as failing, so a dead host or a stopped timer also alarms.
resource "aws_cloudwatch_metric_alarm" "health" {
  alarm_name          = "${var.project}-health-check-failed"
  alarm_description   = "Recorder /healthz failed or was not reported for ${var.health_alarm_minutes} minutes on ${var.host_id}."
  namespace           = local.metrics_namespace
  metric_name         = "HealthCheck"
  dimensions          = { Host = var.host_id }
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = var.health_alarm_minutes
  datapoints_to_alarm = var.health_alarm_minutes
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "disk" {
  alarm_name          = "${var.project}-disk-usage"
  alarm_description   = "Root filesystem above ${var.disk_alarm_percent}% on ${var.host_id}."
  namespace           = local.metrics_namespace
  metric_name         = "DiskUsedPercent"
  dimensions          = { Host = var.host_id }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.disk_alarm_percent
  treat_missing_data  = "missing"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# EC2's own status check (free, no agent): catches a hung or unreachable instance.
resource "aws_cloudwatch_metric_alarm" "status_check" {
  alarm_name          = "${var.project}-status-check-failed"
  alarm_description   = "EC2 status check failing on ${aws_instance.recorder.id}."
  namespace           = "AWS/EC2"
  metric_name         = "StatusCheckFailed"
  dimensions          = { InstanceId = aws_instance.recorder.id }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "missing"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

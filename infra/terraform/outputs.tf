output "instance_id" {
  value = aws_instance.recorder.id
}

output "public_ip" {
  description = "Changes if the instance is stopped and started (no Elastic IP)."
  value       = aws_instance.recorder.public_ip
}

output "ssh" {
  value = "ssh -i <path-to-${var.key_name}.pem> ec2-user@${aws_instance.recorder.public_dns}"
}

output "architecture" {
  value = local.arch
}

output "bucket" {
  value = aws_s3_bucket.data.bucket
}

output "ssm_parameters" {
  description = "Set these out of band (docs/setup.md)."
  value       = sort([for p in aws_ssm_parameter.secret : p.name])
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

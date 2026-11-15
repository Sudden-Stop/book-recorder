variable "region" {
  description = "AWS region for every resource (instance, bucket, parameters, alarms)."
  type        = string
}

variable "project" {
  description = "Name prefix for resources and tags."
  type        = string
  default     = "book-recorder"
}

variable "instance_type" {
  description = "EC2 instance type. Graviton (t4g/m7g/c7g) gets an arm64 AMI, anything else x86_64; the image is built for both. Switching architecture needs -replace=aws_instance.recorder (the AMI is in ignore_changes)."
  type        = string
  default     = "t4g.small"
}

variable "root_volume_gb" {
  description = "Size of the gp3 root volume in GB."
  type        = number
  default     = 50

  validation {
    condition     = var.root_volume_gb >= 20
    error_message = "root_volume_gb must be at least 20."
  }
}

variable "key_name" {
  description = "Name of an existing EC2 key pair in this region, used for SSH."
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR allowed to reach port 22, e.g. \"203.0.113.7/32\". Keep it as narrow as possible."
  type        = string

  validation {
    condition     = can(cidrhost(var.allowed_ssh_cidr, 0))
    error_message = "allowed_ssh_cidr must be a valid IPv4 CIDR such as 203.0.113.7/32."
  }
}

variable "bucket_name" {
  description = "S3 bucket name. Null means \"<project>-<account id>-<region>\"."
  type        = string
  default     = null
}

variable "raw_ia_after_days" {
  description = "Days after which objects under raw/ move to STANDARD_IA."
  type        = number
  default     = 30
}

variable "noncurrent_version_expiry_days" {
  description = "Days after which overwritten (noncurrent) object versions are deleted. Bounds the cost of versioning."
  type        = number
  default     = 90
}

variable "ssm_prefix" {
  description = "SSM Parameter Store path prefix holding the secrets; the instance can read only below it."
  type        = string
  default     = "/book-recorder"

  validation {
    condition     = startswith(var.ssm_prefix, "/") && !endswith(var.ssm_prefix, "/")
    error_message = "ssm_prefix must start with \"/\" and must not end with \"/\"."
  }
}

variable "host_id" {
  description = "Recorder host id; becomes the object name in S3 (<host>.ndjson.zst) and the alarm dimension."
  type        = string
  default     = "rec-a"
}

variable "image" {
  description = "Recorder container image."
  type        = string
  default     = "ghcr.io/sudden-stop/book-recorder:latest"
}

variable "pipeline_image" {
  description = "Pipeline container image (nightly job)."
  type        = string
  default     = "ghcr.io/sudden-stop/book-recorder-pipeline:latest"
}

variable "alert_email" {
  description = "Email address subscribed to the alarm topic. AWS sends a confirmation mail that must be accepted."
  type        = string
}

variable "disk_alarm_percent" {
  description = "Root filesystem usage (percent) that triggers the disk alarm."
  type        = number
  default     = 80
}

variable "health_alarm_minutes" {
  description = "Consecutive minutes of failed (or missing) health checks before the alarm fires."
  type        = number
  default     = 5
}

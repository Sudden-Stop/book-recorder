terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }

  # State is local by default (terraform.tfstate in this directory, git-ignored).
  # To share it, add an S3 backend here before the first apply, e.g.:
  # backend "s3" {
  #   bucket = "<state-bucket>"
  #   key    = "book-recorder/terraform.tfstate"
  #   region = "<region>"
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}

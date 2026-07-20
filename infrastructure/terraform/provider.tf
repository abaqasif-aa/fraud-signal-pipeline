terraform {
  # Minimum Terraform version required
  required_version = ">= 1.0"

  required_providers {
    aws = {
      # Official HashiCorp AWS provider
      source  = "hashicorp/aws"
      # ~> 5.0 means: use 5.x but not 6.x
      # Prevents breaking changes from major version upgrades
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  # All resources will be created in this region
  # Terraform reads credentials from ~/.aws/credentials
  # or environment variables AWS_ACCESS_KEY_ID etc.
  region = "us-east-1"
}

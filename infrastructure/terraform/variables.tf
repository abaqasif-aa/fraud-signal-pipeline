variable "aws_region" {
  description = "AWS region where all resources will be created"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name — used as prefix for all resource names"
  type        = string
  default     = "fraud-signal-pipeline"
}

variable "bucket_name" {
  description = "S3 bucket that holds all pipeline data"
  type        = string
  default     = "fraud-signal-pipeline-371971792187-us-east-1-an"
}

variable "glue_database" {
  description = "Glue Data Catalog database name"
  type        = string
  default     = "fraud_pipeline_tf"
}

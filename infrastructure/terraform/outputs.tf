output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.fraud_pipeline.name
}

output "glue_crawler_name" {
  description = "Glue crawler name"
  value       = aws_glue_crawler.silver_crawler.name
}

output "glue_job_name" {
  description = "Glue ETL job name"
  value       = aws_glue_job.silver_to_gold.name
}

output "glue_role_arn" {
  description = "IAM role ARN used by Glue"
  value       = aws_iam_role.glue_role.arn
}

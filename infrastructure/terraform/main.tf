# ════════════════════════════════════════════════════════════
# IAM ROLE
# ════════════════════════════════════════════════════════════
# Glue needs an IAM role to:
#   - Read Silver data from S3
#   - Write Gold data to S3
#   - Write logs to CloudWatch
#   - Access the Glue Data Catalog
#
# This is the code equivalent of the auto-created role
# AWSGlueServiceRole-fraud-pipeline from yesterday.
# We're now declaring it explicitly so it's reproducible.

resource "aws_iam_role" "glue_role" {
  name        = "GlueServiceRole-${var.project}-tf"
  description = "IAM role for Glue crawler and ETL jobs - managed by Terraform"

  # Trust policy: which AWS service can assume this role
  # Without this Glue cannot use this role at all
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    project     = var.project
    environment = "dev"
    managed_by  = "terraform"
  }
}

# AWS managed policy - gives Glue access to:
#   - CloudWatch Logs (job logs)
#   - Glue Data Catalog (read/write table definitions)
#   - Basic S3 access for Glue internals
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 full access - needed to read Silver and write Gold
# In production this would be a scoped policy on your
# specific bucket only, not all S3 buckets
resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ════════════════════════════════════════════════════════════
# GLUE CRAWLER
# ════════════════════════════════════════════════════════════
# Auto-discovers schema and partitions from S3 and syncs
# them to the Glue Data Catalog.
#
# Key lessons from manual setup encoded here:
#   1. _delta_log/** must be excluded - otherwise crawler
#      creates separate tables per partition instead of
#      one unified table with partition keys
#   2. CRAWL_NEW_FOLDERS_ONLY - only crawl new partitions
#      after first run, much cheaper than full re-crawl
#   3. LOG delete behavior - don't drop tables if S3
#      data is deleted, just log it
#
# Yesterday: built manually, had to delete and recreate
#            twice to get exclusion pattern right
# Today: correct config from the start, in code

resource "aws_glue_crawler" "silver_crawler" {
  name          = "${var.project}-silver-crawler-tf"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.fraud_pipeline.name
  description   = "Crawls Silver Delta Lake table - excludes _delta_log"

  # Point crawler at Silver transactions S3 path
  s3_target {
    path = "s3://${var.bucket_name}/silver/transactions/"

    # Critical for Delta Lake:
    # Without _delta_log/** exclusion the crawler sees:
    #   _delta_log/         → creates table (wrong)
    #   event_date=2026-06-27/ → creates separate table (wrong)
    #   event_date=2026-07-05/ → creates separate table (wrong)
    #
    # With exclusion it correctly sees:
    #   event_date=2026-06-27/ → partition 1 of silver_transactions
    #   event_date=2026-07-05/ → partition 2 of silver_transactions
    exclusions = [
      "_delta_log/**",
      "_SUCCESS",
      "*.crc"
    ]
  }

  # Only crawl new S3 folders since last run
  # Avoids re-scanning all existing partitions every time
  # Cost: ~$0.007 per run vs ~$0.05 for full re-crawl
  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  # What to do when schema or partitions change
  schema_change_policy {
    # Update table definition if new columns appear
    update_behavior = "LOG"
    # If S3 data is deleted, log it - don't drop the table
    delete_behavior = "LOG"
  }

  # No schedule - on demand only
  # Uncomment to run hourly in production:
  # schedule = "cron(0 * * * ? *)"

  tags = {
    project    = var.project
    managed_by = "terraform"
  }

  # Must wait for IAM role and policies to exist first
  # Otherwise Glue can't validate the role during crawler creation
  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy_attachment.glue_s3
  ]
}

# ════════════════════════════════════════════════════════════
# GLUE ETL SCRIPT - UPLOAD TO S3
# ════════════════════════════════════════════════════════════
# Glue jobs reference a Python script stored in S3.
# Terraform uploads the local script file to S3 here.
#
# This is the production pattern:
#   Script lives in Git (version controlled)
#       ↓
#   Terraform uploads to S3 on deploy
#       ↓
#   Glue job reads from S3 when it runs
#
# The etag is an MD5 hash of the file content.
# If you change the script and run terraform apply,
# the hash changes → Terraform detects the difference
# → re-uploads the new version automatically.
# No manual steps needed when you update the script.

resource "aws_s3_object" "glue_script" {
  bucket = var.bucket_name
  key    = "glue-scripts/fraud_silver_to_gold_summary.py"
  source = "${path.module}/../aws/glue/jobs/fraud_silver_to_gold_summary.py"
  etag   = filemd5("${path.module}/../aws/glue/jobs/fraud_silver_to_gold_summary.py")
}

# ════════════════════════════════════════════════════════════
# GLUE ETL JOB
# ════════════════════════════════════════════════════════════
# The ETL job that reads Silver → aggregates → writes Gold.
# Equivalent to what you built in Visual ETL yesterday.
#
# Yesterday: built by dragging nodes in Glue Studio
# Today: declared as code - same result, fully reproducible
#
# Key settings:
#   glue_version 4.0  = Spark 3.3 under the hood
#   G.1X workers      = 1 DPU each, 4 vCPU, 16GB RAM
#   2 workers         = minimum, fine for 559K records
#   timeout 60 mins   = job killed if it runs longer

resource "aws_glue_job" "silver_to_gold" {
  name        = "fraud-silver-to-gold-summary-tf"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Aggregates Silver transactions to Gold fraud summary by date and category"

  command {
    name            = "glueetl"
    # Points to the script we uploaded above
    script_location = "s3://${var.bucket_name}/glue-scripts/fraud_silver_to_gold_summary.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  default_arguments = {
    "--job-language"                     = "python"
    # Temp storage Glue uses for shuffle spill
    "--TempDir"                          = "s3://${var.bucket_name}/glue-tmp/"
    # Send metrics to CloudWatch
    "--enable-metrics"                   = "true"
    # Stream logs to CloudWatch in real time
    "--enable-continuous-cloudwatch-log" = "true"
    # Disable bookmarks - we want full re-run each time
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  # Kill the job if it runs longer than 60 minutes
  timeout = 60

  tags = {
    project    = var.project
    managed_by = "terraform"
  }

  # Must wait for role, policies and script to exist first
  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy_attachment.glue_s3,
    aws_s3_object.glue_script
  ]
}

# ════════════════════════════════════════════════════════════
# GLUE DATA CATALOG DATABASE
# ════════════════════════════════════════════════════════════
# A Glue database is just a namespace - a logical container
# for table definitions. It has no storage of its own.
# Think of it like a schema in PostgreSQL.

resource "aws_glue_catalog_database" "fraud_pipeline" {
  name        = var.glue_database
  description = "Fraud signal pipeline - Silver and Gold Delta Lake tables"

  tags = {
    project    = var.project
    managed_by = "terraform"
  }
}

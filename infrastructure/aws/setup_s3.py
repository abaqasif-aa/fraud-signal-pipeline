"""
S3 Bucket Setup Script
========================
Creates the S3 bucket and folder structure if they don't exist.
Safe to run multiple times — idempotent.

Usage:
    python3 infrastructure/aws/setup_s3.py

Why this exists:
    The bucket was originally created via AWS console.
    This script makes it reproducible — anyone cloning the repo
    can run this once and have the exact same S3 structure.
    This is infrastructure-as-code for the storage layer.
"""

import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────
AWS_REGION  = "us-east-1"
AWS_ACCOUNT = "371971792187"
BUCKET_NAME = f"fraud-signal-pipeline-{AWS_ACCOUNT}-{AWS_REGION}-an"

# S3 folder structure — mirrors the pipeline layers
PREFIXES = [
    "bronze/transactions/",     # raw Kafka events
    "silver/transactions/",     # cleaned + DQ flagged
    "gold/fraud_signals/",      # ML scored records
    "checkpoints/bronze/",      # Spark streaming checkpoints
    "checkpoints/silver/",
    "checkpoints/dlq/",
    "dlq/transactions/",        # dead letter queue
    "quality/results/",         # GE validation results
    "athena-results/",          # Athena query output
    "firehose/transactions/",   # Kinesis Firehose raw path
    "model-registry/",          # MLflow model audit records
    "_manifests/triggers/",     # Delta Lake symlink triggers
]


def create_bucket(s3, bucket_name: str, region: str) -> bool:
    """
    Create S3 bucket if it doesn't exist.
    Returns True if created, False if already existed.
    """
    try:
        # Check if bucket already exists
        s3.head_bucket(Bucket=bucket_name)
        log.info(f"Bucket already exists: s3://{bucket_name}")
        return False

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "404":
            # Bucket doesn't exist — create it
            # us-east-1 doesn't use LocationConstraint
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        "LocationConstraint": region
                    }
                )
            log.info(f"Created bucket: s3://{bucket_name}")
            return True
        else:
            raise


def enable_versioning(s3, bucket_name: str):
    """
    Enable versioning on the bucket.

    Why versioning?
      Delta Lake writes to _delta_log/ frequently.
      Versioning protects against accidental deletion of log entries
      which would corrupt the Delta table.
      Cost impact: minimal at this data volume.
    """
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )
    log.info("Versioning enabled")


def set_lifecycle_policy(s3, bucket_name: str):
    """
    Set lifecycle policy to control costs.

    Rules:
      1. Athena results expire after 7 days
         (query outputs are temporary — no need to keep forever)
      2. DLQ records expire after 90 days
         (enough time to investigate and replay)
      3. Old checkpoint versions expire after 30 days
         (only need recent checkpoints for recovery)
    """
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID":     "expire-athena-results",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "athena-results/"},
                    "Expiration": {"Days": 7},
                },
                {
                    "ID":     "expire-dlq-records",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "dlq/"},
                    "Expiration": {"Days": 90},
                },
                {
                    "ID":     "expire-old-checkpoints",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "checkpoints/"},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                },
            ]
        }
    )
    log.info("Lifecycle policy set")


def create_folder_structure(s3, bucket_name: str):
    """
    Create S3 prefixes (folders).
    S3 is flat — folders are just key prefixes ending in /.
    Creating them explicitly makes the structure visible
    in the AWS console without needing to upload data first.
    """
    for prefix in PREFIXES:
        s3.put_object(Bucket=bucket_name, Key=prefix)
    log.info(f"Created {len(PREFIXES)} folder prefixes")


def verify_structure(s3, bucket_name: str):
    """List all top-level prefixes to confirm structure."""
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Delimiter="/"
    )
    prefixes = [
        p["Prefix"]
        for p in response.get("CommonPrefixes", [])
    ]
    log.info("Bucket structure:")
    for p in prefixes:
        log.info(f"  s3://{bucket_name}/{p}")


def main():
    log.info(f"Setting up S3 bucket: {BUCKET_NAME}")
    log.info(f"Region: {AWS_REGION}")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Step 1: Create bucket
    created = create_bucket(s3, BUCKET_NAME, AWS_REGION)

    # Step 2: Enable versioning
    enable_versioning(s3, BUCKET_NAME)

    # Step 3: Set lifecycle policies
    set_lifecycle_policy(s3, BUCKET_NAME)

    # Step 4: Create folder structure
    create_folder_structure(s3, BUCKET_NAME)

    # Step 5: Verify
    verify_structure(s3, BUCKET_NAME)

    log.info(f"\nS3 setup complete.")
    log.info(f"Bucket: s3://{BUCKET_NAME}")
    log.info(f"Console: https://s3.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}")


if __name__ == "__main__":
    main()

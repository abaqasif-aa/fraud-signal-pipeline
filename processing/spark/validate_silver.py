"""
Great Expectations: Silver Layer Validation
============================================================
Uses GE's checkpoint-free validation pattern — most stable
approach across GE 0.17.x and 0.18.x versions.

Full Spark DataFrame used for aggregation metrics.
Sample converted to pandas only for row-level expectations.
This gives us distributed compute for heavy checks (counts,
means) while avoiding GE API version conflicts.

Run:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               io.delta:delta-spark_2.12:3.1.0,\
               org.apache.hadoop:hadoop-aws:3.3.4 \
    /app/validate_silver.py
"""

import json
import logging
import uuid
from datetime import datetime, timezone

import boto3
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration
from great_expectations.dataset import SparkDFDataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────
S3_BUCKET   = "fraud-signal-pipeline-371971792187-us-east-1-an"
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/transactions"
RESULTS_KEY = "quality/results"
AWS_REGION  = "us-east-1"

DB_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "fraud_db",
    "user":     "fraud_user",
    "password": "fraud_pass",
}

MIN_PASS_RATE = 0.95


def build_spark_session() -> SparkSession:
    """Build SparkSession with Delta Lake and S3 support."""
    return (
        SparkSession.builder
        .appName("GEValidation_Spark")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_silver(spark: SparkSession):
    """Load full silver Delta Lake table."""
    log.info(f"Loading silver table: {SILVER_PATH}")
    df    = spark.read.format("delta").load(SILVER_PATH)
    total = df.count()
    log.info(f"Total records: {total:,}")
    return df, total


def run_validation(spark: SparkSession, df, batch_id: str) -> dict:
    """
    Run GE expectations using SparkDFDataset.

    SparkDFDataset wraps a Spark DataFrame and translates
    each GE expectation into a native Spark operation.
    This is the most stable GE+Spark pattern across versions.

    Heavy aggregations (means, counts) run on the full dataset.
    Row-level checks also run on Spark — no pandas conversion.
    """
    log.info("Wrapping DataFrame in SparkDFDataset...")

    # Cast boolean columns to double for mean calculations
    # GE SparkDFDataset cannot compute mean on boolean columns directly
    # double representation: True=1.0, False=0.0
    # mean of 0/1 column = rate (e.g. fraud_rate = avg(_is_fraud_label))
    from pyspark.sql.types import DoubleType
    df = df.withColumn("_is_fraud_label",F.col("_is_fraud_label").cast(DoubleType()))\
        .withColumn("dq_passed", F.col("dq_passed").cast(DoubleType()))\
        .withColumn("is_high_risk_country",F.col("is_high_risk_country").cast(DoubleType()))

    # SparkDFDataset is GE's native Spark wrapper
    # It translates expect_* calls into Spark SQL operations
    # that run distributed across all executors
    ge_df = SparkDFDataset(df)

    log.info("Running expectations on full Spark DataFrame...")

    # ── Required fields: non-null ──────────────────────────
    # Runs: SELECT COUNT(*) FROM silver WHERE col IS NULL
    for col in ["event_id", "event_timestamp", "user_id",
                "merchant_id", "amount", "is_online"]:
        ge_df.expect_column_values_to_not_be_null(column=col)
        log.info(f"  ✓ null check: {col}")

    # ── event_id: unique ───────────────────────────────────
    # Runs: SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver
    ge_df.expect_column_values_to_be_unique(column="event_id")
    log.info("  ✓ uniqueness check: event_id")

    # ── amount: within bounds ─────────────────────────────
    # Runs: SELECT COUNT(*) FROM silver
    #       WHERE amount < 0.01 OR amount > 50000
    ge_df.expect_column_values_to_be_between(
        column="amount",
        min_value=0.01,
        max_value=50000.0,
    )
    log.info("  ✓ range check: amount")

    # ── merchant_category: enum check ─────────────────────
    # Runs: SELECT COUNT(*) FROM silver
    #       WHERE merchant_category NOT IN (...)
    ge_df.expect_column_values_to_be_in_set(
        column="merchant_category",
        value_set=[
            "grocery", "electronics", "travel", "restaurant",
            "fuel", "healthcare", "entertainment", "retail",
            "subscription", "atm_withdrawal"
        ],
        mostly=0.999,
    )
    log.info("  ✓ enum check: merchant_category")

    # ── country_code: format check ─────────────────────────
    # Runs: SELECT COUNT(*) FROM silver
    #       WHERE country_code NOT RLIKE '^[A-Z]{2}$'
    ge_df.expect_column_values_to_match_regex(
        column="country_code",
        regex=r"^[A-Z]{2}$",
        mostly=0.99,
    )
    log.info("  ✓ regex check: country_code")

    # ── currency: enum check ───────────────────────────────
    ge_df.expect_column_values_to_be_in_set(
        column="currency",
        value_set=["USD", "EUR", "GBP", "CAD", "AUD"],
    )
    log.info("  ✓ enum check: currency")

    # ── amount mean: distribution check ───────────────────
    # Runs: SELECT AVG(amount) FROM silver
    ge_df.expect_column_mean_to_be_between(
        column="amount",
        min_value=10.0,
        max_value=500.0,
    )
    log.info("  ✓ mean check: amount")

    # ── fraud rate: ~2% ───────────────────────────────────
    # Runs: SELECT AVG(CAST(_is_fraud_label AS DOUBLE)) FROM silver
    ge_df.expect_column_mean_to_be_between(
        column="_is_fraud_label",
        min_value=0.01,
        max_value=0.05,
    )
    log.info("  ✓ distribution check: fraud rate")

    # ── DQ pass rate: >99% ────────────────────────────────
    ge_df.expect_column_mean_to_be_between(
        column="dq_passed",
        min_value=0.99,
        max_value=1.0,
    )
    log.info("  ✓ distribution check: dq_passed rate")

    # ── High-risk country rate: ~9% ───────────────────────
    ge_df.expect_column_mean_to_be_between(
        column="is_high_risk_country",
        min_value=0.05,
        max_value=0.15,
    )
    log.info("  ✓ distribution check: high-risk country rate")

    # validate() runs all expectations and returns results
    log.info("Finalising validation results...")
    results = ge_df.validate()

    return _parse_results(results, batch_id), results


def _parse_results(results, batch_id: str) -> dict:
    """Parse GE results into structured summary."""
    stats  = results.statistics
    passed = stats.get("successful_expectations", 0)
    failed = stats.get("unsuccessful_expectations", 0)
    total  = stats.get("evaluated_expectations", 0)
    pct    = stats.get("success_percent", 0.0)

    # Log failures with detail
    if not results.success:
        log.warning("── Failed expectations ──────────────────")
        for r in results.results:
            if not r.success:
                col = r.expectation_config.kwargs.get("column", "N/A")
                exp = r.expectation_config.expectation_type
                log.warning(f"  FAILED: {exp} on '{col}'")
                result_dict = r.result
                if "unexpected_percent" in result_dict:
                    log.warning(
                        f"    Unexpected %: "
                        f"{result_dict['unexpected_percent']:.3f}%"
                    )
                if "observed_value" in result_dict:
                    log.warning(
                        f"    Observed: {result_dict['observed_value']}"
                    )

    return {
        "run_id":               str(uuid.uuid4()),
        "run_timestamp":        datetime.now(timezone.utc).isoformat(),
        "batch_id":             batch_id,
        "expectations_run":     total,
        "expectations_passed":  passed,
        "expectations_failed":  failed,
        "pass_rate":            round(pct / 100.0, 4),
        "status":               "PASSED" if results.success else "FAILED",
        "success":              results.success,
    }


def upload_results_to_s3(summary: dict):
    """Upload JSON result to S3 for audit trail."""
    s3  = boto3.client("s3", region_name=AWS_REGION)
    key = (
        f"{RESULTS_KEY}/"
        f"{datetime.now().strftime('%Y/%m/%d')}/"
        f"{summary['batch_id']}.json"
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(summary, indent=2),
        ContentType="application/json",
    )
    log.info(f"Results uploaded: s3://{S3_BUCKET}/{key}")


def write_to_postgres(summary: dict, total_records: int):
    """Persist DQ run to dq_run_log table."""
    try:
        conn = psycopg2.connect(**DB_CONN)
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO dq_run_log (
                run_id, run_timestamp, batch_id,
                total_records, passed_records, failed_records,
                pass_rate, expectations_run, expectations_failed, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id) DO NOTHING
        """, (
            summary["run_id"],
            summary["run_timestamp"],
            summary["batch_id"],
            total_records,
            summary["expectations_passed"],
            summary["expectations_failed"],
            summary["pass_rate"],
            summary["expectations_run"],
            summary["expectations_failed"],
            summary["status"],
        ))
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"DQ run logged: {summary['run_id'][:8]}")
    except Exception as e:
        log.error(f"PostgreSQL write failed: {e}")


def main():
    log.info("Starting GE validation — SparkDFDataset")

    batch_id = f"ge_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df, total_records = load_silver(spark)
    summary, results  = run_validation(spark, df, batch_id)

    upload_results_to_s3(summary)
    write_to_postgres(summary, total_records)

    spark.stop()

    log.info("\n" + "=" * 50)
    log.info("VALIDATION SUMMARY")
    log.info("=" * 50)
    log.info(f"  Status:              {summary['status']}")
    log.info(f"  Pass rate:           {summary['pass_rate']:.1%}")
    log.info(f"  Expectations run:    {summary['expectations_run']}")
    log.info(f"  Expectations passed: {summary['expectations_passed']}")
    log.info(f"  Expectations failed: {summary['expectations_failed']}")
    log.info(f"  Total records:       {total_records:,}")
    log.info(f"  Batch ID:            {batch_id}")
    log.info("=" * 50)

    if summary["pass_rate"] < MIN_PASS_RATE:
        log.error(
            f"ALERT: Pass rate {summary['pass_rate']:.1%} below "
            f"threshold {MIN_PASS_RATE:.1%}."
        )

    return summary["success"]


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

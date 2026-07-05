"""
Spark Structured Streaming Job
================================
Reads transaction events from Kafka topic transactions.raw
Writes to Delta Lake on S3 in two layers:
  Bronze : raw events as-is
  Silver : cleaned, enriched, with DQ flags

Inline lightweight validation runs on every micro-batch.
Full GE suite runs hourly via Airflow (validate_silver.py).
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType,
    StructField, StructType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────
KAFKA_BROKER  = "kafka:29092"
KAFKA_TOPIC   = "transactions.raw"
S3_BUCKET     = "fraud-signal-pipeline-371971792187-us-east-1-an"
BRONZE_PATH   = f"s3a://{S3_BUCKET}/bronze/transactions"
SILVER_PATH   = f"s3a://{S3_BUCKET}/silver/transactions"
DLQ_PATH      = f"s3a://{S3_BUCKET}/dlq/transactions"
CHECKPOINT    = f"s3a://{S3_BUCKET}/checkpoints"
AWS_REGION    = "us-east-1"

# ── Alert thresholds ────────────────────────────────────────
# If any of these are breached in a single micro-batch
# we log a critical error (SNS alert added in Week 4)
MAX_NULL_RATE       = 0.01   # >1% null required fields = alert
MAX_BAD_AMOUNT_RATE = 0.01   # >1% out-of-range amounts = alert
MAX_INVALID_CAT_RATE= 0.001  # >0.1% invalid categories = alert

VALID_CATEGORIES = {
    "grocery", "electronics", "travel", "restaurant",
    "fuel", "healthcare", "entertainment", "retail",
    "subscription", "atm_withdrawal"
}

# ── Schema ──────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("event_id",          StringType(),  False),
    StructField("event_timestamp",   StringType(),  False),
    StructField("user_id",           StringType(),  False),
    StructField("merchant_id",       StringType(),  False),
    StructField("merchant_category", StringType(),  True),
    StructField("amount",            DoubleType(),  False),
    StructField("currency",          StringType(),  True),
    StructField("country_code",      StringType(),  True),
    StructField("card_type",         StringType(),  True),
    StructField("is_online",         BooleanType(), False),
    StructField("device_type",       StringType(),  True),
    StructField("ip_address",        StringType(),  True),
    StructField("_is_fraud_label",   BooleanType(), True),
])


def build_spark_session() -> SparkSession:
    """Build SparkSession with Delta Lake and S3 support."""
    return (
        SparkSession.builder
        .appName("FraudSignalStreaming")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Read raw bytes from Kafka and parse JSON."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.reconnect.backoff.ms", "1000")
        .option("kafka.reconnect.backoff.max.ms", "10000")
        .option("kafka.request.timeout.ms", "60000")
        .load()
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("raw_value"),
        )
    )

    # Parse JSON payload using our schema
    parsed = (
        raw
        .withColumn("data",
                    F.from_json("raw_value", TRANSACTION_SCHEMA))
        .select("kafka_timestamp", "kafka_partition",
                "kafka_offset", "data.*")
        .withColumn("event_timestamp",
                    F.to_timestamp("event_timestamp"))
    )
    return parsed


def apply_silver_transforms(df):
    """
    Enrich bronze stream for silver layer.

    Adds derived columns and per-record DQ flags.
    These are the lightweight inline checks — run on every
    record in every micro-batch. Results stored as columns
    in the silver table so analysts can filter to dq_passed=true.

    Full distribution checks run hourly via validate_silver.py.
    """
    return (
        df
        # Partition column — determines S3 folder
        # event_date=2026-07-05/ → Athena only scans today's data
        .withColumn("event_date",
                    F.to_date("event_timestamp"))

        # Hour of day — useful ML feature for fraud detection
        # Fraud clusters at unusual hours (3am transactions)
        .withColumn("event_hour",
                    F.hour("event_timestamp"))

        # High-risk country flag — precomputed for ML training
        # Avoids recomputing this join at training time
        .withColumn("is_high_risk_country",
                    F.col("country_code").isin("NG", "RU", "UA", "BR"))

        # DQ flag 1: invalid merchant category
        # Catches schema drift — producer added new category
        # without updating data contract
        .withColumn("dq_invalid_category",
                    ~F.col("merchant_category").isin(*VALID_CATEGORIES))

        # DQ flag 2: amount outside contract bounds
        # min=$0.01 max=$50,000 per transaction_events_v1.yml
        .withColumn("dq_amount_out_of_range",
                    (F.col("amount") < 0.01) | (F.col("amount") > 50000.0))

        # DQ flag 3: missing required fields
        # from_json() returns null if field missing or type mismatch
        .withColumn("dq_missing_required",
                    F.col("event_id").isNull() |
                    F.col("user_id").isNull() |
                    F.col("amount").isNull())

        # Composite DQ flag — single column for easy filtering
        # WHERE dq_passed = true in ML training queries
        .withColumn("dq_passed",
                    ~(F.col("dq_invalid_category") |
                      F.col("dq_amount_out_of_range") |
                      F.col("dq_missing_required")))

        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pipeline_version", F.lit("1.0.0"))
    )


def validate_batch_metrics(batch_df, batch_id: int):
    """
    Batch-level alerting — runs after every micro-batch write.

    This is the foreachBatch callback. Unlike the per-record
    DQ flags above, this aggregates across the whole batch
    and alerts if any metric breaches a threshold.

    Two-level quality system:
      Level 1 (this function): batch-level thresholds → immediate alert
      Level 2 (validate_silver.py): full GE suite → hourly schedule

    Think of Level 1 as a smoke detector — catches fires fast.
    Level 2 is the full inspection — thorough but scheduled.
    """
    total = batch_df.count()

    # Skip empty batches — happens when no Kafka messages
    # arrived in the 30-second trigger window
    if total == 0:
        log.info(f"Batch {batch_id}: empty — skipping validation")
        return

    # Aggregate DQ metrics across the entire batch
    # These are the same flags we computed per-record above
    # but now we're checking their RATE across the batch
    metrics = batch_df.agg(
        # Null rate: what fraction of records are missing required fields
        F.round(
            F.sum(F.when(F.col("dq_missing_required"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("null_rate"),

        # Bad amount rate: what fraction have out-of-range amounts
        F.round(
            F.sum(F.when(F.col("dq_amount_out_of_range"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("bad_amount_rate"),

        # Invalid category rate: what fraction have unknown categories
        F.round(
            F.sum(F.when(F.col("dq_invalid_category"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("invalid_cat_rate"),

        # Overall DQ pass rate for this batch
        F.round(
            F.sum(F.when(F.col("dq_passed"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("pass_rate"),

        # Fraud rate — sanity check, should be ~2%
        F.round(
            F.sum(F.when(F.col("_is_fraud_label"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("fraud_rate"),
    ).collect()[0]

    # Log batch summary — visible in docker logs
    log.info(
        f"Batch {batch_id} | "
        f"records={total:,} | "
        f"pass_rate={metrics.pass_rate:.1%} | "
        f"fraud_rate={metrics.fraud_rate:.1%} | "
        f"null_rate={metrics.null_rate:.3%} | "
        f"bad_amount_rate={metrics.bad_amount_rate:.3%}"
    )

    # ── Threshold checks → alert if breached ──────────────
    # In Week 4 we wire these to SNS. For now they log CRITICAL
    # which is visible in docker logs and CloudWatch.

    if metrics.null_rate > MAX_NULL_RATE:
        log.critical(
            f"ALERT batch {batch_id}: null rate {metrics.null_rate:.1%} "
            f"exceeds threshold {MAX_NULL_RATE:.1%}. "
            f"Check producer for missing required fields."
        )

    if metrics.bad_amount_rate > MAX_BAD_AMOUNT_RATE:
        log.critical(
            f"ALERT batch {batch_id}: bad amount rate "
            f"{metrics.bad_amount_rate:.1%} exceeds threshold "
            f"{MAX_BAD_AMOUNT_RATE:.1%}. "
            f"Check producer for amount validation."
        )

    if metrics.invalid_cat_rate > MAX_INVALID_CAT_RATE:
        log.critical(
            f"ALERT batch {batch_id}: invalid category rate "
            f"{metrics.invalid_cat_rate:.1%} exceeds threshold "
            f"{MAX_INVALID_CAT_RATE:.1%}. "
            f"Possible schema drift — new category in producer."
        )

    # Fraud rate sanity check
    # If fraud rate spikes above 10% or drops to 0%, generator is broken
    if metrics.fraud_rate > 0.10 or metrics.fraud_rate == 0.0:
        log.warning(
            f"WARNING batch {batch_id}: unusual fraud rate "
            f"{metrics.fraud_rate:.1%}. Expected ~2%."
        )


def write_silver_with_validation(silver_stream, silver_path: str):
    """
    Write silver stream using foreachBatch.

    foreachBatch gives us access to each micro-batch as a
    static DataFrame — we can write it to Delta Lake AND
    run batch-level validation in the same callback.

    This is more powerful than .writeStream directly because:
      - We can write to multiple destinations per batch
      - We can run arbitrary logic after the write
      - We can inspect the data before committing
    """
    def write_and_validate(batch_df, batch_id):
        """
        Called by Spark after every micro-batch.
        batch_df : static DataFrame for this batch
        batch_id : sequential integer (0, 1, 2, ...)
        """
        if batch_df.count() == 0:
            return

        # Step 1: Write to Delta Lake silver
        # mode="append" — never overwrite existing data
        # partitionBy event_date — Athena partition pruning
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            # mergeSchema: if new columns appear in the DataFrame
            # that don't exist in the Delta table yet, add them
            # automatically instead of rejecting the write.
            # This handles pipeline evolution without data loss.
            .option("mergeSchema", "true")
            .option("path", silver_path)
            .save()
        )

        # Step 2: Run batch-level validation AFTER write
        # If validation fails we log an alert but don't
        # roll back — data is already in silver with DQ flags
        validate_batch_metrics(batch_df, batch_id)

    return (
        silver_stream.writeStream
        .foreachBatch(write_and_validate)
        .option("checkpointLocation", f"{CHECKPOINT}/silver")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading from Kafka...")
    stream = read_kafka_stream(spark)

    log.info("Applying silver transforms...")
    silver = apply_silver_transforms(stream)

    # Bronze: simple append write — no validation needed
    # Bronze is immutable raw data, validation happens on silver
    log.info(f"Writing bronze to {BRONZE_PATH}")
    bronze_query = (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/bronze")
        .option("path", BRONZE_PATH)
        # mergeSchema allows new columns to be added automatically
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Silver: write via foreachBatch so we can validate each batch
    log.info(f"Writing silver to {SILVER_PATH}")
    silver_query = write_silver_with_validation(silver, SILVER_PATH)

    log.info("Streaming active. Awaiting termination...")
    log.info(f"  Bronze : {BRONZE_PATH}")
    log.info(f"  Silver : {SILVER_PATH}")
    log.info(f"  Batch validation runs every 30 seconds")
    log.info(f"  Full GE suite: run validate_silver.py manually or via Airflow")

    try:
        bronze_query.awaitTermination()
        silver_query.awaitTermination()
    except KeyboardInterrupt:
        bronze_query.stop()
        silver_query.stop()
        spark.stop()
        log.info("Stopped.")


if __name__ == "__main__":
    main()

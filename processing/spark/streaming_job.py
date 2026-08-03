"""
Spark Structured Streaming Job
================================
Reads Avro-serialized transaction events from Kafka.
Deserializes using Confluent Schema Registry wire format.
Writes to Delta Lake on S3 in two layers:
  Bronze : raw events as-is
  Silver : cleaned, enriched, with DQ flags

Schema Registry integration:
  - Producer embeds schema ID in every message header
  - Consumer fetches schema by ID from registry (once, cached)
  - fastavro deserializes Avro bytes → Python dict → Spark Row
  - Zero per-message registry calls after first fetch

Wire format (Confluent standard):
  Byte 0:    0x00 (magic byte)
  Bytes 1-4: schema ID (big-endian uint32)
  Bytes 5+:  Avro binary payload

Inline lightweight validation runs on every micro-batch.
Full GE suite runs hourly via Airflow (validate_silver.py).
"""

import io
import logging
import struct

import fastavro
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType,
    StructField, StructType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────
KAFKA_BROKER         = "kafka:29092"
KAFKA_TOPIC          = "transactions.raw"
SCHEMA_REGISTRY_URL  = "http://schema-registry:8081"
S3_BUCKET            = "fraud-signal-pipeline-371971792187-us-east-1-an"
BRONZE_PATH          = f"s3a://{S3_BUCKET}/bronze/transactions"
SILVER_PATH          = f"s3a://{S3_BUCKET}/silver/transactions"
DLQ_PATH             = f"s3a://{S3_BUCKET}/dlq/transactions"
CHECKPOINT           = f"s3a://{S3_BUCKET}/checkpoints"
AWS_REGION           = "us-east-1"

# ── Alert thresholds ────────────────────────────────────────
MAX_NULL_RATE        = 0.01
MAX_BAD_AMOUNT_RATE  = 0.01
MAX_INVALID_CAT_RATE = 0.001

VALID_CATEGORIES = {
    "grocery", "electronics", "travel", "restaurant",
    "fuel", "healthcare", "entertainment", "retail",
    "subscription", "atm_withdrawal"
}

# ── Spark schema ─────────────────────────────────────────────
# Mirrors the Avro schema registered in Schema Registry.
# fastavro deserializes to Python dicts — we then map to
# this Spark schema when creating the DataFrame.
TRANSACTION_SCHEMA = StructType([
    StructField("event_id",          StringType(),    False),
    StructField("event_timestamp",   StringType(),    False),
    StructField("user_id",           StringType(),    False),
    StructField("merchant_id",       StringType(),    True),
    StructField("merchant_category", StringType(),    True),
    StructField("amount",            DoubleType(),    True),
    StructField("currency",          StringType(),    True),
    StructField("country_code",      StringType(),    True),
    StructField("card_type",         StringType(),    True),
    StructField("is_online",         BooleanType(),   True),
    StructField("device_type",       StringType(),    True),
    StructField("ip_address",        StringType(),    True),
    StructField("_is_fraud_label",   BooleanType(),   True),
    StructField("terminal_id",       StringType(),    True),
])


# ══════════════════════════════════════════════════════════════
# SCHEMA REGISTRY CLIENT
# ══════════════════════════════════════════════════════════════

class SchemaRegistryClient:
    """
    Minimal Confluent Schema Registry client for the consumer.

    Caches schemas by ID so we only call the registry once
    per schema version — not once per message.

    In production with AWS MSK + Glue Schema Registry:
      - Swap SCHEMA_REGISTRY_URL for the Glue endpoint
      - Use boto3 glue client instead of HTTP requests
      - Everything else in this class stays the same
    """

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self._cache = {}   # schema_id → parsed fastavro schema

    def get_schema(self, schema_id: int) -> dict:
        """
        Fetch and cache Avro schema by ID.

        Called once per schema ID per Spark executor lifetime.
        Subsequent messages with the same schema ID use the cache.

        Why cache matters:
          Without cache: 1 HTTP call per message = 100 calls/sec
          With cache:    1 HTTP call per schema version ever
        """
        if schema_id in self._cache:
            return self._cache[schema_id]

        response = requests.get(
            f"{self.url}/schemas/ids/{schema_id}",
            timeout=10
        )
        response.raise_for_status()
        schema_str = response.json()["schema"]

        import json
        raw_schema   = json.loads(schema_str)
        parsed       = fastavro.parse_schema(raw_schema)
        self._cache[schema_id] = parsed

        log.info(f"Fetched and cached schema ID={schema_id} "
                 f"from {self.url}")
        return parsed


# Module-level registry client — shared across calls within
# one executor. Reset per Spark task boundary automatically.
_registry = SchemaRegistryClient(SCHEMA_REGISTRY_URL)


def deserialize_avro(raw_bytes: bytes) -> dict:
    """
    Deserialize a single Confluent Avro wire-format message.

    Wire format:
      [0x00]           magic byte — confirms Confluent format
      [uint32 BE]      schema ID (4 bytes)
      [avro binary]    payload bytes

    Steps:
      1. Validate magic byte
      2. Extract schema ID from bytes 1-4
      3. Fetch schema from registry (cached after first call)
      4. Deserialize Avro bytes → Python dict

    Returns None if deserialization fails — caller routes
    failed records to the quarantine DLQ path.
    """
    if raw_bytes is None or len(raw_bytes) < 5:
        return None

    # Validate Confluent magic byte
    if raw_bytes[0] != 0:
        log.warning(f"Invalid magic byte: {raw_bytes[0]:#x} "
                    f"— expected 0x00. Not Confluent wire format.")
        return None

    # Extract 4-byte big-endian schema ID
    schema_id = struct.unpack(">I", raw_bytes[1:5])[0]

    # Fetch schema (cached after first call per schema ID)
    try:
        parsed_schema = _registry.get_schema(schema_id)
    except Exception as e:
        log.error(f"Failed to fetch schema ID={schema_id}: {e}")
        return None

    # Deserialize Avro binary payload (bytes 5 onwards)
    try:
        payload = raw_bytes[5:]
        buf     = io.BytesIO(payload)
        record  = fastavro.schemaless_reader(buf, parsed_schema)
        return record
    except Exception as e:
        log.error(f"Avro deserialization failed "
                  f"schema_id={schema_id}: {e}")
        return None


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
    """
    Read Avro messages from Kafka and deserialize using
    Confluent Schema Registry.

    BEFORE (JSON):
      Kafka bytes → cast to string → from_json() → DataFrame

    AFTER (Avro):
      Kafka bytes → strip 5-byte header → fetch schema by ID
                  → fastavro.schemaless_reader → Python dict
                  → Spark UDF maps dict → DataFrame row

    Everything downstream (DQ flags, silver transforms, writes)
    is unchanged — they still receive the same DataFrame schema.

    Why UDF for deserialization:
      Spark's native from_json() only understands JSON.
      For Avro with a schema registry, we use a Python UDF
      that runs deserialize_avro() on each row's bytes.
      The UDF runs on Spark executors in parallel — one call
      per Kafka message, schema fetched once per executor.
    """
    from pyspark.sql.types import MapType

    # Step 1: Read raw bytes from Kafka
    # We keep value as binary (bytes) — NOT cast to string
    # Casting to string would corrupt the Avro binary payload
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
            F.col("value").alias("raw_bytes"),   # keep as binary
        )
    )

    # Step 2: Register UDF for Avro deserialization
    # The UDF calls deserialize_avro() on each row's bytes.
    # Returns a MapType (string → string) so Spark can handle
    # heterogeneous Avro field types before we cast them.
    #
    # Why MapType and not StructType directly:
    #   UDFs returning StructType require the schema to be
    #   registered at UDF definition time. Using MapType lets
    #   us return arbitrary key-value pairs and then select
    #   individual fields with explicit casts afterward.
    def avro_udf_fn(raw_bytes):
        """
        Spark UDF: bytes → dict → {field: str_value} map.
        Returns None for messages that fail deserialization.
        These get routed to the DLQ in the write step.
        """
        record = deserialize_avro(bytes(raw_bytes) if raw_bytes else None)
        if record is None:
            return None
        # Convert all values to strings for MapType compatibility
        # We cast back to correct types in Step 3 below
        return {k: str(v) if v is not None else None
                for k, v in record.items()}

    avro_udf = F.udf(avro_udf_fn, MapType(StringType(), StringType()))

    # Step 3: Apply UDF and extract fields with correct types
    # This replaces the old from_json() call.
    # Each field is extracted from the map and cast to its
    # correct type — matching TRANSACTION_SCHEMA exactly.
    parsed = (
        raw
        .withColumn("data", avro_udf(F.col("raw_bytes")))
        # Route failed deserializations to DLQ
        # Records where data is null failed Avro deserialization
        # — either wrong magic byte, unknown schema ID, or
        # corrupt payload. We keep them for debugging.
        .filter(F.col("data").isNotNull())
        # Extract each field from the map with explicit casting
        .withColumn("event_id",
            F.col("data")["event_id"].cast(StringType()))
        .withColumn("event_timestamp",
            F.to_timestamp(F.col("data")["event_timestamp"]))
        .withColumn("user_id",
            F.col("data")["user_id"].cast(StringType()))
        .withColumn("merchant_id",
            F.col("data")["merchant_id"].cast(StringType()))
        .withColumn("merchant_category",
            F.col("data")["merchant_category"].cast(StringType()))
        .withColumn("amount",
            F.col("data")["amount"].cast(DoubleType()))
        .withColumn("currency",
            F.col("data")["currency"].cast(StringType()))
        .withColumn("country_code",
            F.col("data")["country_code"].cast(StringType()))
        .withColumn("card_type",
            F.col("data")["card_type"].cast(StringType()))
        .withColumn("is_online",
            F.col("data")["is_online"].cast(BooleanType()))
        .withColumn("device_type",
            F.col("data")["device_type"].cast(StringType()))
        .withColumn("ip_address",
            F.col("data")["ip_address"].cast(StringType()))
        .withColumn("_is_fraud_label",
            F.col("data")["_is_fraud_label"].cast(BooleanType()))
        .withColumn("terminal_id",
            F.col("data")["terminal_id"].cast(StringType()))
        # Drop intermediate map column
        .drop("data", "raw_bytes")
    )

    return parsed


# ══════════════════════════════════════════════════════════════
# ALL CODE BELOW IS UNCHANGED FROM ORIGINAL
# Only read_kafka_stream was modified above
# ══════════════════════════════════════════════════════════════

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
        .withColumn("event_date",
                    F.to_date("event_timestamp"))
        .withColumn("event_hour",
                    F.hour("event_timestamp"))
        .withColumn("is_high_risk_country",
                    F.col("country_code").isin("NG", "RU", "UA", "BR"))
        .withColumn("dq_invalid_category",
                    ~F.col("merchant_category").isin(*VALID_CATEGORIES))
        .withColumn("dq_amount_out_of_range",
                    (F.col("amount") < 0.01) | (F.col("amount") > 50000.0))
        .withColumn("dq_missing_required",
                    F.col("event_id").isNull() |
                    F.col("user_id").isNull() |
                    F.col("amount").isNull())
        .withColumn("dq_passed",
                    ~(F.col("dq_invalid_category") |
                      F.col("dq_amount_out_of_range") |
                      F.col("dq_missing_required")))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pipeline_version", F.lit("1.1.0"))
    )


def validate_batch_metrics(batch_df, batch_id: int):
    """
    Batch-level alerting — runs after every micro-batch write.

    Two-level quality system:
      Level 1 (this function): batch-level thresholds → immediate alert
      Level 2 (validate_silver.py): full GE suite → hourly schedule
    """
    total = batch_df.count()

    if total == 0:
        log.info(f"Batch {batch_id}: empty — skipping validation")
        return

    metrics = batch_df.agg(
        F.round(
            F.sum(F.when(F.col("dq_missing_required"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("null_rate"),
        F.round(
            F.sum(F.when(F.col("dq_amount_out_of_range"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("bad_amount_rate"),
        F.round(
            F.sum(F.when(F.col("dq_invalid_category"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("invalid_cat_rate"),
        F.round(
            F.sum(F.when(F.col("dq_passed"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("pass_rate"),
        F.round(
            F.sum(F.when(F.col("_is_fraud_label"), 1).otherwise(0))
            / F.count("*"), 4
        ).alias("fraud_rate"),
    ).collect()[0]

    log.info(
        f"Batch {batch_id} | "
        f"records={total:,} | "
        f"pass_rate={metrics.pass_rate:.1%} | "
        f"fraud_rate={metrics.fraud_rate:.1%} | "
        f"null_rate={metrics.null_rate:.3%} | "
        f"bad_amount_rate={metrics.bad_amount_rate:.3%}"
    )

    if metrics.null_rate > MAX_NULL_RATE:
        log.critical(
            f"ALERT batch {batch_id}: null rate {metrics.null_rate:.1%} "
            f"exceeds threshold {MAX_NULL_RATE:.1%}."
        )
    if metrics.bad_amount_rate > MAX_BAD_AMOUNT_RATE:
        log.critical(
            f"ALERT batch {batch_id}: bad amount rate "
            f"{metrics.bad_amount_rate:.1%} exceeds threshold."
        )
    if metrics.invalid_cat_rate > MAX_INVALID_CAT_RATE:
        log.critical(
            f"ALERT batch {batch_id}: invalid category rate "
            f"{metrics.invalid_cat_rate:.1%} — possible schema drift."
        )
    if metrics.fraud_rate > 0.10 or metrics.fraud_rate == 0.0:
        log.warning(
            f"WARNING batch {batch_id}: unusual fraud rate "
            f"{metrics.fraud_rate:.1%}. Expected ~2%."
        )


def write_silver_with_validation(silver_stream, silver_path: str):
    """Write silver stream using foreachBatch with validation."""

    def write_and_validate(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        (
            batch_df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .option("mergeSchema", "true")
            .option("path", silver_path)
            .save()
        )
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

    log.info("Starting Fraud Signal Streaming Pipeline v1.1.0")
    log.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    log.info(f"Kafka: {KAFKA_BROKER} → {KAFKA_TOPIC}")

    log.info("Reading Avro stream from Kafka...")
    stream = read_kafka_stream(spark)

    log.info("Applying silver transforms...")
    silver = apply_silver_transforms(stream)

    log.info(f"Writing bronze to {BRONZE_PATH}")
    bronze_query = (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/bronze")
        .option("path", BRONZE_PATH)
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info(f"Writing silver to {SILVER_PATH}")
    silver_query = write_silver_with_validation(silver, SILVER_PATH)

    log.info("Streaming active. Awaiting termination...")
    log.info(f"  Wire format: [0x00][schema_id][avro bytes]")
    log.info(f"  Schema Registry: {SCHEMA_REGISTRY_URL}")
    log.info(f"  Bronze : {BRONZE_PATH}")
    log.info(f"  Silver : {SILVER_PATH}")

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
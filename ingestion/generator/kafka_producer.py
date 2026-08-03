"""
Kafka Producer with Confluent Schema Registry
===============================================
Produces Avro-serialized transaction events to Kafka.
Schema is registered in Confluent Schema Registry on startup
and validated on every message.

Local:       Schema Registry at http://schema-registry:8081
Production:  AWS Glue Schema Registry (same Avro schema, different endpoint)

Why Avro over JSON?
  - 3-5x smaller messages (binary format)
  - Schema enforced at producer side — bad messages rejected before Kafka
  - Schema evolution tracked in registry — consumers always know what to expect
  - Schema ID embedded in message header — consumer fetches schema by ID

Message format:
  [magic byte 0x0][4-byte schema ID][avro binary payload]
"""

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone

import requests
import fastavro
import io
import struct
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────
KAFKA_BROKER       = "kafka:29092"
KAFKA_TOPIC        = "transactions.raw"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
SUBJECT            = f"{KAFKA_TOPIC}-value"

# ── Avro Schema ─────────────────────────────────────────────
# This is the canonical schema — matches AWS Glue Schema Registry v4
AVRO_SCHEMA = {
    "type": "record",
    "name": "TransactionEvent",
    "namespace": "com.fraud.pipeline",
    "doc": "Card transaction event — data contract: transaction_events_v1.yml",
    "fields": [
        {"name": "event_id",          "type": "string"},
        {"name": "event_timestamp",   "type": "string"},
        {"name": "user_id",           "type": "string"},
        {"name": "merchant_id",       "type": ["null", "string"], "default": None},
        {"name": "merchant_category", "type": ["null", "string"], "default": None},
        {"name": "amount",            "type": ["null", "double"],  "default": None},
        {"name": "currency",          "type": ["null", "string"], "default": None},
        {"name": "country_code",      "type": ["null", "string"], "default": None},
        {"name": "card_type",         "type": ["null", "string"], "default": None},
        {"name": "is_online",         "type": ["null", "boolean"], "default": None},
        {"name": "device_type",       "type": ["null", "string"], "default": None},
        {"name": "ip_address",        "type": ["null", "string"], "default": None},
        {"name": "_is_fraud_label",   "type": ["null", "boolean"], "default": None},
        {"name": "terminal_id",       "type": ["null", "string"], "default": None},
    ]
}


class SchemaRegistryClient:
    """
    Minimal Confluent Schema Registry client.
    Handles schema registration and ID lookup.

    In production this would be the confluent-kafka SchemaRegistryClient
    or the AWS Glue Schema Registry SDK — same concepts, different endpoint.
    """

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self._schema_id = None

    def register_schema(self, subject: str, schema: dict) -> int:
        """
        Register schema under subject if not already registered.
        Returns schema ID — embedded in every Avro message header.

        Subject naming convention: {topic}-value
        This is the Confluent standard — one schema per topic value.
        """
        response = requests.post(
            f"{self.url}/subjects/{subject}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json={"schema": json.dumps(schema)},
            timeout=10
        )
        response.raise_for_status()
        schema_id = response.json()["id"]
        log.info(f"Schema registered: subject={subject} id={schema_id}")
        return schema_id

    def get_schema_id(self, subject: str, schema: dict) -> int:
        """
        Check if schema already registered, return its ID.
        Avoids re-registering on every producer restart.
        """
        try:
            response = requests.post(
                f"{self.url}/subjects/{subject}",
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                json={"schema": json.dumps(schema)},
                timeout=10
            )
            if response.status_code == 200:
                return response.json()["id"]
        except Exception:
            pass
        return self.register_schema(subject, schema)

    def check_compatibility(self, subject: str, schema: dict) -> bool:
        """
        Check if schema is compatible with latest registered version.
        Call this before registering to get a clear error message.
        """
        response = requests.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/latest",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json={"schema": json.dumps(schema)},
            timeout=10
        )
        if response.status_code == 404:
            return True  # no existing schema, always compatible
        result = response.json()
        return result.get("is_compatible", False)


def serialize_avro(record: dict, schema: dict, schema_id: int) -> bytes:
    """
    Serialize a Python dict to Confluent Avro wire format.

    Wire format:
      Byte 0:   magic byte 0x00 (Confluent schema registry marker)
      Bytes 1-4: schema ID as 4-byte big-endian integer
      Bytes 5+:  Avro binary-encoded payload

    The schema ID lets consumers fetch the exact schema from
    the registry without embedding the full schema in every message.
    This is the key efficiency win — messages are tiny.
    """
    parsed_schema = fastavro.parse_schema(schema)

    # Write Avro binary payload
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed_schema, record)
    avro_bytes = buf.getvalue()

    # Prepend magic byte + schema ID
    # struct.pack(">I", schema_id) = 4-byte big-endian unsigned int
    return b'\x00' + struct.pack(">I", schema_id) + avro_bytes


def generate_transaction(fraud_rate: float) -> dict:
    """Generate a synthetic transaction event."""
    import random

    CATEGORIES = [
        "grocery", "electronics", "travel", "restaurant",
        "fuel", "healthcare", "entertainment", "retail",
        "subscription", "atm_withdrawal"
    ]
    CURRENCIES  = ["USD", "EUR", "GBP", "CAD", "AUD"]
    COUNTRIES   = ["US", "GB", "CA", "AU", "DE", "FR", "NG", "RU", "UA", "BR"]
    CARD_TYPES  = ["visa", "mastercard", "amex", "discover"]
    DEVICES     = ["mobile", "desktop", "tablet"]

    is_fraud   = random.random() < fraud_rate
    is_online  = random.random() < 0.45

    # Fraud transactions: higher amounts, more online, more high-risk countries
    if is_fraud:
        amount = round(random.uniform(200, 5000), 2) \
            if random.random() < 0.7 \
            else round(random.uniform(0.01, 10), 2)
        country = random.choice(["NG", "RU", "UA", "BR"]) \
            if random.random() < 0.4 \
            else random.choice(COUNTRIES)
    else:
        amount  = round(random.uniform(5, 500), 2)
        country = random.choice(COUNTRIES)

    return {
        "event_id":          str(uuid.uuid4()),
        "event_timestamp":   datetime.now(timezone.utc).isoformat(),
        "user_id":           f"user_{random.randint(1000, 9999)}",
        "merchant_id":       f"merch_{random.randint(100, 999)}",
        "merchant_category": random.choice(CATEGORIES),
        "amount":            amount,
        "currency":          random.choice(CURRENCIES),
        "country_code":      country,
        "card_type":         random.choice(CARD_TYPES),
        "is_online":         is_online,
        "device_type":       random.choice(DEVICES) if is_online else None,
        "ip_address":        f"192.168.{random.randint(1,254)}.{random.randint(1,254)}" if is_online else None,
        "_is_fraud_label":   is_fraud,
        "terminal_id":       None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate",       type=float, default=100)
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    args = parser.parse_args()

    delay = 1.0 / args.rate

    # ── Step 1: Register schema in Schema Registry ──────────
    log.info(f"Connecting to Schema Registry: {SCHEMA_REGISTRY_URL}")
    registry = SchemaRegistryClient(SCHEMA_REGISTRY_URL)

    # Wait for schema registry to be ready
    for attempt in range(30):
        try:
            requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=3)
            log.info("Schema Registry ready")
            break
        except Exception:
            log.info(f"Waiting for Schema Registry... ({attempt+1}/30)")
            time.sleep(2)

    schema_id = registry.get_schema_id(SUBJECT, AVRO_SCHEMA)
    log.info(f"Using schema ID: {schema_id}")

    # ── Step 2: Connect to Kafka ─────────────────────────────
    log.info(f"Connecting to Kafka: {KAFKA_BROKER}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # No value_serializer — we handle Avro serialization manually
        # to embed the schema ID in the wire format
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",           # wait for all replicas (exactly-once foundation)
        retries=5,
        retry_backoff_ms=500,
    )

    # ── Step 3: Produce events ───────────────────────────────
    log.info(f"Producing at {args.rate} eps, fraud_rate={args.fraud_rate}")
    log.info(f"Schema Registry subject: {SUBJECT}")
    log.info(f"Kafka topic: {KAFKA_TOPIC}")

    count     = 0
    fraud_count = 0

    while True:
        try:
            event = generate_transaction(args.fraud_rate)

            # Serialize to Avro wire format with schema ID header
            avro_bytes = serialize_avro(event, AVRO_SCHEMA, schema_id)

            producer.send(
                KAFKA_TOPIC,
                key=event["event_id"],
                value=avro_bytes,
            )

            count += 1
            if event["_is_fraud_label"]:
                fraud_count += 1

            if count % 1000 == 0:
                fraud_rate_actual = fraud_count / count
                log.info(
                    f"Produced {count:,} events | "
                    f"fraud_rate={fraud_rate_actual:.2%} | "
                    f"schema_id={schema_id}"
                )

            time.sleep(delay)

        except KeyboardInterrupt:
            log.info("Stopping producer")
            producer.flush()
            break
        except Exception as e:
            log.error(f"Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()

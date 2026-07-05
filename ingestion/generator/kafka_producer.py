"""
Kafka Producer wrapper for the generator.
Reads events from generate_event() and sends to Kafka topic.
"""

import argparse
import json
import logging
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from generator import generate_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BROKER = "kafka:29092"   # internal Docker address
TOPIC        = "transactions.raw"


def ensure_topic(broker: str, topic: str):
    """Create topic if it doesn't exist."""
    admin  = AdminClient({"bootstrap.servers": broker})
    topics = admin.list_topics(timeout=10).topics
    if topic not in topics:
        fs = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        for t, f in fs.items():
            try:
                f.result()
                log.info(f"Created topic: {t}")
            except Exception as e:
                log.warning(f"Topic creation: {e}")
    else:
        log.info(f"Topic exists: {topic}")


def delivery_callback(err, msg):
    if err:
        log.error(f"Delivery failed: {err}")


def run(rate: int, fraud_rate: float, duration: int = None):
    """Produce events to Kafka."""
    # Wait for Kafka to be ready
    log.info("Waiting for Kafka...")
    time.sleep(10)

    ensure_topic(KAFKA_BROKER, TOPIC)

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms":         5,
        "compression.type":  "snappy",
    })

    log.info(f"Producing | rate={rate}/s | fraud={fraud_rate:.1%} | topic={TOPIC}")

    start    = time.time()
    emitted  = 0
    fraud    = 0
    interval = 1.0 / rate

    try:
        while True:
            if duration and (time.time() - start) > duration:
                break

            event = generate_event(fraud_rate=fraud_rate)
            producer.produce(
                topic    = TOPIC,
                key      = event["event_id"],
                value    = json.dumps(event),
                callback = delivery_callback,
            )
            producer.poll(0)
            emitted += 1

            if event["_is_fraud_label"]:
                fraud += 1

            # Log every 500 events
            if emitted % 500 == 0:
                elapsed = time.time() - start
                log.info(
                    f"Emitted: {emitted:,} | "
                    f"Fraud: {fraud} ({100*fraud/emitted:.1f}%) | "
                    f"Rate: {emitted/elapsed:.0f} eps"
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Stopping...")
    finally:
        producer.flush(timeout=10)
        log.info(f"Done | emitted={emitted:,} | fraud={fraud}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate",       type=int,   default=100)
    parser.add_argument("--fraud-rate", type=float, default=0.02)
    parser.add_argument("--duration",   type=int,   default=None)
    args = parser.parse_args()
    run(args.rate, args.fraud_rate, args.duration)

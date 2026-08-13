#!/bin/bash
# CDC Setup — Debezium PostgreSQL Connector
# ==========================================
# Registers the Debezium connector with Kafka Connect.
# Run this after starting the debezium container.
#
# Prerequisites:
#   docker compose up -d debezium postgres kafka
#   Wait ~30 seconds for Debezium to fully start
#
# PostgreSQL requirements (already in docker-compose.yml):
#   wal_level=logical
#   max_replication_slots=4
#   max_wal_senders=4
#
# What this creates:
#   Kafka topics:
#     fraud_db.public.fraud_signals  <- CDC events from fraud_signals table
#     fraud_db.public.dq_run_log     <- CDC events from dq_run_log table
#
#   Each CDC event contains:
#     before: row state before change (null for INSERT)
#     after:  row state after change (null for DELETE)
#     op:     c=INSERT, u=UPDATE, d=DELETE, r=READ(snapshot)
#     source: connector, table, txId, lsn (WAL position for replay)
#
# Production equivalent:
#   MSK Connect + Debezium connector on Amazon Aurora PostgreSQL
#   Same config, different hostname and credentials

set -e

DEBEZIUM_URL="http://localhost:8083"
CONNECTOR_NAME="fraud-postgres-connector"

echo "Waiting for Debezium to be ready..."
until curl -sf "$DEBEZIUM_URL/connectors" > /dev/null; do
    sleep 2
done
echo "Debezium ready."

# Check if connector already exists
if curl -sf "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "Connector $CONNECTOR_NAME already exists — deleting and recreating..."
    curl -X DELETE "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME"
    sleep 2
fi

echo "Registering PostgreSQL connector..."

curl -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$CONNECTOR_NAME\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"postgres\",
      \"database.port\": \"5432\",
      \"database.user\": \"fraud_user\",
      \"database.password\": \"fraud_pass\",
      \"database.dbname\": \"fraud_db\",
      \"database.server.name\": \"fraud_db\",
      \"topic.prefix\": \"fraud_db\",
      \"table.include.list\": \"public.fraud_signals,public.dq_run_log\",
      \"plugin.name\": \"pgoutput\",
      \"slot.name\": \"debezium_slot\",
      \"publication.name\": \"debezium_publication\",
      \"snapshot.mode\": \"initial\",
      \"decimal.handling.mode\": \"double\",
      \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"key.converter.schemas.enable\": \"false\",
      \"value.converter.schemas.enable\": \"false\"
    }
  }"

echo ""
echo "Checking connector status..."
sleep 3
curl -s "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool

echo ""
echo "CDC setup complete."
echo "Topics will appear at:"
echo "  fraud_db.public.fraud_signals"
echo "  fraud_db.public.dq_run_log"
echo ""
echo "Verify with:"
echo "  docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list | grep fraud_db"
echo "  docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic fraud_db.public.fraud_signals --from-beginning --max-messages 1"

FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    fastavro==1.9.0 \
    requests==2.31.0

COPY generator.py .
COPY kafka_producer.py .

CMD ["python3", "kafka_producer.py", "--rate", "100", "--fraud-rate", "0.02"]

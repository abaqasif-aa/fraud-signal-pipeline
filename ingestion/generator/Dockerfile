FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir confluent-kafka==2.3.0 boto3==1.34.0

# Copy all generator code
COPY generator.py .
COPY kafka_producer.py .

# Run the kafka producer directly
CMD ["python3", "kafka_producer.py", "--rate", "100", "--fraud-rate", "0.02"]

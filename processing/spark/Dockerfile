FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.1.0 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    great-expectations==0.18.19 \
    fastavro==1.9.0 \
    requests==2.31.0

WORKDIR /app

COPY streaming_job.py .
COPY validate_silver.py .

# Override entrypoint — jupyter/pyspark-notebook defaults to Jupyter
ENTRYPOINT []
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4", \
     "/app/streaming_job.py"]

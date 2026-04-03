FROM bitnami/spark:3.3.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    cassandra-driver==3.28.0 \
    pandas==2.0.3

# Create app and data directories
RUN mkdir -p /app /data

WORKDIR /app

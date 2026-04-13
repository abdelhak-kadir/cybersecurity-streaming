FROM python:3.9-bullseye

USER root

# Install Java 11 (Debian Bullseye has it)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download and install Spark
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    cassandra-driver==3.28.0 \
    pandas==2.0.3 \
    pyspark==3.3.0

# Create directories
RUN mkdir -p /app /data

WORKDIR /app

CMD ["/bin/bash"]
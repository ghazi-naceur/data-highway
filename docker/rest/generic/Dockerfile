FROM ubuntu:18.04

RUN apt-get update && apt-get install --no-install-recommends -y tzdata && \
 apt install --no-install-recommends -y openjdk-8-jdk openjdk-8-jre git wget curl

ARG DATA_HIGHWAY_VERSION=0.4

RUN mkdir -p /app/config && \
  mkdir -p /app/data/parquet/input && \
  mkdir -p /app/data/parquet/output && \
  mkdir -p /app/data/parquet/processed && \
  mkdir -p /app/data/avro/input && \
  mkdir -p /app/data/avro/output && \
  mkdir -p /app/data/avro/processed && \
  mkdir -p /app/data/orc/input && \
  mkdir -p /app/data/orc/output && \
  mkdir -p /app/data/orc/processed && \
  mkdir -p /app/data/csv/input && \
  mkdir -p /app/data/csv/output && \
  mkdir -p /app/data/csv/processed && \
  mkdir -p /app/data/json/input && \
  mkdir -p /app/data/json/output && \
  mkdir -p /app/data/json/processed && \
  mkdir -p /app/data/xlsx/input && \
  mkdir -p /app/data/xlsx/output && \
  mkdir -p /app/data/xlsx/processed && \
  mkdir -p /app/data/kafka/input && \
  mkdir -p /app/data/kafka/output && \
  mkdir -p /app/data/kafka/processed && \
  mkdir -p /app/data/elasticsearch/input && \
  mkdir -p /app/data/elasticsearch/output && \
  mkdir -p /app/data/elasticsearch/processed && \
  mkdir /app/jar && \
  cd /app/jar && \
  wget https://github.com/ghazi-naceur/data-highway/releases/download/v${DATA_HIGHWAY_VERSION}/data-highway-${DATA_HIGHWAY_VERSION}.jar

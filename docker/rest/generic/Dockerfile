FROM ubuntu:18.04

RUN apt-get update && apt-get install --no-install-recommends -y tzdata && \
 apt install --no-install-recommends -y openjdk-8-jdk openjdk-8-jre git wget curl

ARG DATA_HIGHWAY_VERSION=0.4-beta

RUN mkdir -p /app/config && \
  mkdir -p /app/data/spark/input && \
  mkdir -p /app/data/spark/output && \
  mkdir -p /app/data/spark/processed && \
  mkdir -p /app/data/kafka/input && \
  mkdir -p /app/data/kafka/output && \
  mkdir -p /app/data/kafka/processed && \
  mkdir -p /app/data/elasticsearch/input && \
  mkdir -p /app/data/elasticsearch/output && \
  mkdir -p /app/data/elasticsearch/processed && \
  mkdir /app/jar && \
  cd /app/jar && \
  wget https://github.com/ghazi-naceur/data-highway/releases/download/v${DATA_HIGHWAY_VERSION}/data-highway-${DATA_HIGHWAY_VERSION}.jar

# ARG SPARK_VERSION=2.4.6
# ENV SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-without-hadoop \
#  SPARK_HOME=/usr/spark-${SPARK_VERSION} \
#  PATH=$PATH:${SPARK_HOME}/bin
#
# RUN curl -sL --retry 3 \
#   "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
#   | gunzip \
#   | tar x -C /usr/ && \
#  mv /usr/$SPARK_PACKAGE $SPARK_HOME && \
#  chown -R root:root $SPARK_HOME && \
#  wget "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar" && \
#  mv spark-avro_2.12-$SPARK_VERSION.jar $SPARK_HOME/jars && \
#  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

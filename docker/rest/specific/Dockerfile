FROM ubuntu:18.04

RUN apt-get update && apt-get install --no-install-recommends -y tzdata && \
 apt install --no-install-recommends -y openjdk-8-jdk openjdk-8-jre wget curl

ARG DATA_HIGHWAY_VERSION=0.3-rc
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
  mkdir /app/jar

ARG SBT_VERSION=1.4.1
RUN  apt-get update  &&  apt-get -y install git && \
    mkdir /application && \
    cd /application && \
    git clone https://github.com/ghazi-naceur/data-highway.git && \
    cd data-highway && \
    git branch && \
    git fetch --all && \
    git checkout master

RUN cd /application/data-highway && \
    export PATH="/usr/local/sbt/bin:$PATH" && mkdir -p "/usr/local/sbt" && wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
    sbt sbtVersion && \
    sbt clean && \
    sbt compile && \
    sbt assembly

RUN mv /application/data-highway/target/scala-2.12/data-highway-${DATA_HIGHWAY_VERSION}.jar /app/jar && \
  rm -R /application

ARG SPARK_VERSION=2.4.6
ENV SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-without-hadoop \
 SPARK_HOME=/usr/spark-${SPARK_VERSION} \
 PATH=$PATH:${SPARK_HOME}/bin

RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ && \
 mv /usr/$SPARK_PACKAGE $SPARK_HOME && \
 chown -R root:root $SPARK_HOME && \
 wget "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar" && \
 mv spark-avro_2.12-$SPARK_VERSION.jar $SPARK_HOME/jars && \
 rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

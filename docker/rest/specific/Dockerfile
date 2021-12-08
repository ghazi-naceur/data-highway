FROM ubuntu:18.04

RUN apt-get update && apt-get install --no-install-recommends -y tzdata && \
 apt install --no-install-recommends -y openjdk-8-jdk openjdk-8-jre wget curl

ARG DATA_HIGHWAY_VERSION=0.6-rc
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
  mkdir -p /app/data/xml/input && \
  mkdir -p /app/data/xml/output && \
  mkdir -p /app/data/xml/processed && \
  mkdir -p /app/data/kafka/input && \
  mkdir -p /app/data/kafka/output && \
  mkdir -p /app/data/kafka/processed && \
  mkdir -p /app/data/elasticsearch/input && \
  mkdir -p /app/data/elasticsearch/output && \
  mkdir -p /app/data/elasticsearch/processed && \
  mkdir /app/jar

COPY data-highway-0.6-rc.jar /app/jar
WORKDIR /app/jar

# ARG SBT_VERSION=1.4.1
# RUN  apt-get update  &&  apt-get -y install git && \
#     mkdir /application && \
#     cd /application
#     && \
#     git clone https://github.com/ghazi-naceur/data-highway.git && \
#     cd data-highway && \
#     git branch && \
#     git fetch --all && \
#     git checkout feat-enhancing_optimized_route

# RUN cd /application/data-highway && \
#     export PATH="/usr/local/sbt/bin:$PATH" && mkdir -p "/usr/local/sbt" && wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && \
#     sbt sbtVersion && \
#     sbt clean && \
#     sbt compile && \
#     sbt assembly

# RUN mv /application/data-highway/target/scala-2.12/data-highway-${DATA_HIGHWAY_VERSION}.jar /app/jar && \
#   rm -R /application
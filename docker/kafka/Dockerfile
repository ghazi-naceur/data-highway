FROM ubuntu:latest
# Install tzdata to set timezone when installing jdk-8
RUN apt-get update && apt-get install -y tzdata

# Installing jdk-8
RUN apt install -y openjdk-8-jdk openjdk-8-jre
RUN apt-get -y install git
RUN apt install -y wget
RUN apt install -y curl

# Installing scala 2.12.12
RUN wget www.scala-lang.org/files/archive/scala-2.12.12.deb
RUN dpkg --install scala-2.12.12.deb

# Cloning data-highway project
RUN git clone https://github.com/ghazi-naceur/data-highway.git
RUN mkdir /application/ && cp -R data-highway /application/

# Installing sbt and compiling data-highway project
ARG SBT_VERSION=1.4.1
RUN \
  mkdir /working/ && \
  cd /working/ && \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  cd && \
  rm -r /working/ && \
  cd /application/data-highway && \
  sbt sbtVersion && \
  sbt clean && \
  sbt compile && \
  sbt assembly

# Cleanup data-highway project
RUN mkdir -p /app/config && \
  mkdir /app/jar && \
  mkdir -p /app/data/input && \
  mkdir -p /app/data/output && \
  mv /application/data-highway/target/scala-2.12/data-highway-assembly-0.1.jar /app/jar && \
  mv /application/data-highway/src/main/resources/application.conf /app/config && \
  mv /application/data-highway/src/main/resources/log4j2.properties /app/config && \
  mv /application/data-highway/src/test/resources/csv_to_avro-data/input/* /app/data/input && \
  mv /application/data-highway/src/test/resources/csv_to_avro-data/output/* /app/data/output && \
  rm -R /application
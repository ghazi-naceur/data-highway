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
  git fetch --all && \
  git branch -v -a  && \
  git checkout topic-creation-issue && \
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

# Installing Hadoop
ENV HADOOP_VERSION 3.1.3
ENV HADOOP_HOME /usr/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
RUN curl -sL --retry 3 \
  "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
  | gunzip \
  | tar -x -C /usr/ \
 && rm -rf $HADOOP_HOME/share/doc \
 && chown -R root:root $HADOOP_HOME

# Installing Spark
ENV SPARK_VERSION 3.0.1
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-without-hadoop
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ \
 && mv /usr/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R root:root $SPARK_HOME

RUN wget "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/2.4.0/spark-avro_2.12-2.4.0.jar" \
    && mv spark-avro_2.12-2.4.0.jar $SPARK_HOME/jars
FROM ubuntu:latest
# Install tzdata to set timezone when installing jdk-8
RUN apt-get update && apt-get install --no-install-recommends -y tzdata && \
 apt install --no-install-recommends -y openjdk-8-jdk openjdk-8-jre git wget curl && \
 wget www.scala-lang.org/files/archive/scala-2.12.12.deb && \
 dpkg --install scala-2.12.12.deb

# Installing sbt and compiling data-highway project
ARG SBT_VERSION=1.4.1
RUN curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get install --no-install-recommends sbt

# Cleanup data-highway project
RUN mkdir -p /app/config && \
  mkdir -p /app/data/spark/input && \
  mkdir -p /app/data/spark/output && \
  mkdir -p /app/data/spark/processed && \
  mkdir -p /app/data/kafka/input && \
  mkdir -p /app/data/kafka/output && \
  mkdir -p /app/data/kafka/processed && \
  mkdir /app/jar && \
  cd /app/jar && \
  wget https://github.com/ghazi-naceur/data-highway/releases/download/v0.1-rc/data-highway-0.1-rc.jar

# Installing Hadoop
ENV HADOOP_VERSION 3.1.3
ENV HADOOP_HOME /usr/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
RUN curl -sL --retry 3 "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
  | gunzip \
  | tar -x -C /usr/ && \
  rm -rf $HADOOP_HOME/share/doc && \
  chown -R root:root $HADOOP_HOME

# Installing Spark
ENV SPARK_VERSION 3.0.1
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-without-hadoop
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ && \
 mv /usr/$SPARK_PACKAGE $SPARK_HOME && \
 chown -R root:root $SPARK_HOME && \
 wget "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/2.4.0/spark-avro_2.12-2.4.0.jar" && \
 mv spark-avro_2.12-2.4.0.jar $SPARK_HOME/jars
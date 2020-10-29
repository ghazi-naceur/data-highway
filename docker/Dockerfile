FROM ubuntu:latest
RUN apt-get update && apt-get install -y tzdata
RUN apt-get -y install git
RUN apt install -y openjdk-8-jdk openjdk-8-jre
RUN apt install -y wget
RUN apt install -y curl

RUN wget www.scala-lang.org/files/archive/scala-2.12.12.deb
RUN dpkg --install scala-2.12.12.deb

RUN git clone https://github.com/ghazi-naceur/data-highway.git
RUN mkdir /application/ && cp -R data-highway /application/

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

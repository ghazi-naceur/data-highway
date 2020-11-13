FROM ubuntu:latest
# Install tzdata to set timezone when installing jdk-8
RUN apt-get update && apt-get install -y tzdata

# Installing jdk-8
RUN apt install -y openjdk-8-jdk openjdk-8-jre
RUN apt-get -y install git
RUN apt install -y wget
RUN apt install -y curl
RUN apt install -y zip
RUN curl -O http://packages.confluent.io/archive/6.0/confluent-community-6.0.0.zip
RUN unzip confluent-community-6.0.0.zip
RUN cd confluent-6.0.0
RUN curl -O http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz
#RUN tar -xvf confluent-hub-client-latest.tar.gz
RUN mkdir -p /confluent-hub-client
RUN tar -C /confluent-hub-client -xzf confluent-hub-client-latest.tar.gz
RUN pwd
RUN ls
RUN ls /confluent-hub-client
RUN export CONFLUENT_HUB=/confluent-hub-client
RUN export PATH=$PATH:$CONFLUENT_HUB/bin
RUN export CONFLUENT_HOME=/confluent-6.0.0
RUN export PATH="${CONFLUENT_HOME}/bin:$PATH"
RUN confluent local services start

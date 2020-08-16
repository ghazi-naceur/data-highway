#!/bin/sh
cd /d/databases/kafka_2.12-2.3.0/bin/windows
./kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic json-to-kafka-sp_kf_plugin-topic
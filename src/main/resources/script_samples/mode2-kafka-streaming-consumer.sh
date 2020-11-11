#!/bin/sh
kafka-console-consumer --bootstrap-server localhost:9092 --topic kafka-to-json-topic-out --from-beginning
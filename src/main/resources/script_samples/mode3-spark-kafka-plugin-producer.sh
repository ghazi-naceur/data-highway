#!/bin/sh
cd /d/databases/kafka_2.12-2.3.0/bin/windows
./kafka-console-producer.bat --broker-list localhost:9092 --topic json-to-kafka-streaming-topic 
{"id":7.0,"first_name":"Bordie","last_name":"Altham","email":"baltham6@hud.gov","gender":"Male","ip_address":"234.202.91.240"}

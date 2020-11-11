Yet another data converter.

You can convert your data to multiple data types.

**A- Getting started** : 
---

**1- Run data-highway locally** :
---

You can run data-highway locally/manually by :

1- Cloning the project :
````shell script
git clone https://github.com/ghazi-naceur/data-highway.git
````

2- Compiling the project :
````shell script
sbt clean; sbt compile; sbt assembly;
````

3- Take your generated jar file which will be under the folder : `data-highway/target/scala-2.12/data-highway-assembly-0.1.jar`
and put inside another folder, along with the `application.conf` and `log4j2.properties` (which are under the `resources` folder).

4- Modify the `application.conf` file using the **B- Conversions** section from this `readme` file.

5- Run data-highway application using the following commands :

If you are using Spark, run the following command :
````shell script
spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/the/path/to/application.conf" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/the/path/to/application.conf" \
      --files "/the/path/to/application.conf" \
      /the/path/to/data-highway-assembly-0.1.jar
````
If you are using pure Kafka (not the spark-kafka-plugin feature), run the following command :
````shell script
java -jar -Dconfig.file=/the/path/to/application.conf /the/path/to/data-highway-assembly-0.1.jar

````

**2- Run data-highway using Docker** :
---

You can find a `Dockerfile` sample under `docker` folder.
In order to run your Data-Highway container, specify your params in the following commands :

1- The `/the/path/to/your/mounted/input/volume/` folder that will contain your input data.

2- The `/the/path/to/your/mounted/output/volume/` folder that will contain the generated files issued after the conversion.

3- Run the following commands in the `docker` folder (the location of the Dockerfile) :
````shell script
cd docker
docker build -t data-highway:v1.0 .
docker run -tid \
  -v /the/path/to/your/mounted/input/volume/:/app/data/input \
  -v /the/path/to/your/mounted/output/volume/:/app/data/output \
  --name data-highway-container data-highway:v1.0
docker ps -a
docker images
docker exec -ti bungee-gum spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app/config/application.conf" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app/config/application.conf" \
      --files "/app/config/application.conf" \
      /app/jar/data-highway-assembly-0.1.jar
````

In order to change the configuration of the `application.conf` file, you can enter your `data-highway-container`, through the following command :
````shell script
docker exec -ti data-highway-container sh
```` 
Your `application.conf` file will be under `/app/config/application.conf` path, and you will be able to change configurations using the **B- Conversions** section from this `readme` file.

````shell script
# After entering your container
cd /app/config/
vim application.conf
````
Then, you can rerun your container using the same command : 
````shell script
docker exec -ti data-highway-container spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app/config/application.conf" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app/config/application.conf" \
      --files "/app/config/application.conf" \
      /app/jar/data-highway-assembly-0.1.jar
````

**B- Conversions** : 
---

**1- JSON conversion** :
---

**a- From Parquet to JSON** : 
````hocon
route {
  type = parquet-to-json
  in = "your-input-folder-containing-parquet-files"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
}
````

**b- From CSV to JSON** : 
````hocon
route {
  type = csv-to-json
  in = "your-input-folder-containing-csv-files"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
}
````

**c- From Avro to JSON** : 
````hocon
route {
  type = avro-to-json
  in = "your-input-folder-containing-avro-files"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
}
````

**2- Parquet conversion** :
---

**a- From JSON to Parquet** : 
````hocon
route {
  type = json-to-parquet
  in = "your-input-folder-containing-json-files"
  out = "your-output-folder-that-will-contain-your-generated-parquet-files"
}
````

**b- From CSV to Parquet** : 
````hocon
route {
  type = csv-to-parquet
  in = "your-input-folder-containing-csv-files"
  out = "your-output-folder-that-will-contain-your-generated-parquet-files"
}
````

**c- From Avro to Parquet** : 
````hocon
route {
  type = avro-to-parquet
  in = "your-input-folder-containing-avro-files"
  out = "your-output-folder-that-will-contain-your-generated-parquet-files"
}
````

**3- CSV conversion** :
---

**a- From JSON to CSV** : 
````hocon
route {
  type = json-to-csv
  in = "your-input-folder-containing-json-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

**b- From Parquet to CSV** : 
````hocon
route {
  type = parquet-to-csv
  in = "your-input-folder-containing-parquet-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

**c- From Avro to CSV** : 
````hocon
route {
  type = avro-to-csv
  in = "your-input-folder-containing-avro-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

**d- From XLSX to CSV** : 

Consist of converting the different sheets of an XLSX or XLS file to multiple csv files.
````hocon
route {
  type = xlsx-to-csv
  in = "your-input-folder-containing-xlsx-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

**4- Avro conversion** :
---

**a- From Parquet to Avro** :
```hocon
route {
  type = parquet-to-avro
  in = "your-input-folder-containing-parquet-files"
  out = "your-output-folder-that-will-contain-your-generated-avro-files"
}
```

**b- From Json to Avro** :
```hocon
route {
  type = json-to-avro
  in = "your-input-folder-containing-json-files"
  out = "your-output-folder-that-will-contain-your-generated-avro-files"
}
```

**c- From Csv to Avro** :
```hocon
route {
    type = csv-to-avro
  in = "your-input-folder-containing-csv-files"
  out = "your-output-folder-that-will-contain-your-generated-avro-files"
}
```

**5- Send data to Kafka** :
---

This mode is available using 3 types of channel : 

**a- Simple Kafka Producer** : 
````hocon
route {
  type = json-to-kafka
  in = "your-input-folder-containing-json-files"
  out = "your-output-kafka-topic"
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas", // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = simple-producer
  }
}
````

**b- Kafka Streaming** : 
````hocon
route {
  type = json-to-kafka
  in = "your-input-folder-containing-json-files"
  out = "your-output-kafka-topic"
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas", // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "kafka-streaming"
      stream-app-id = "your-streaming-app-name" // eg: "first-stream-app"
  }
}
````

**c- Spark Kafka Producer Plugin** :
````hocon
route {
  type = json-to-kafka
  in = "your-input-folder-containing-json-files"
  out = "your-output-kafka-topic"
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas", // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "spark-kafka-producer-plugin"
      use-stream = false
      intermediate-topic = "your-intermediate-topic" // Must be set once `use-stream = true`
      checkpoint-folder = "your-checkpoint-folder-related-to-the-intermediate-topic" // Must be set once `use-stream = true`. You must change its value everytime you change the `intermediate-topic`
  }
}
````

**6- Consume data from Kafka** :
---

**a- Simple Kafka Consumer** :
````hocon
route {
  type = kafka-to-file
  in = "topic-name"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
  data-type = {
    type = "the-desired-datatype-of-the-generated-files" // Optional field : accepted values are json, avro and kafka (default value, if not set). 
             // kafka value refer to "txt" extension set for the generated files.
  }
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas"  // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = simple-consumer
  }
  offset = "offset-to-consume-from" // accepted values : earliest, latest, none
  consumer-group = "your-consumer-group-name"
}
````

**b- Kafka Streaming** : 

````hocon
route {
  type = kafka-to-file
  in = "topic-name"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
  data-type = {
    type = "the-desired-datatype-of-the-generated-files" // Optional field : accepted values are json, avro and kafka (default value, if not set). 
             // kafka value refer to "txt" extension set for the generated files.
  }
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas"  // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "kafka-streaming"
      stream-app-id = "your-stream-app-name"
  }
  offset = "offset-to-consume-from" // accepted values : earliest, latest, none
  consumer-group = "your-consumer-group-name"
}
````

**c- Spark Kafka Consumer Plugin** :
````hocon
route {
  type = kafka-to-file
  in = "topic-name"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
  data-type = {
    type = "the-desired-datatype-of-the-generated-files" // Optional field : accepted values are json, avro and kafka (default value, if not set). 
             // kafka value refer to "txt" extension set for the generated files.
  }
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas"  // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "spark-kafka-consumer-plugin"
      use-stream = "true/false"
  }
  offset = "offset-to-consume-from" // accepted values : earliest, latest, none
  consumer-group = "your-consumer-group-name"
}
````
**3- Scheduling :** : 
---

Under the `data-highway/airflow/dag` folder, you will find an Airflow DAG sample, that runs your data-highway application with Airflow. 
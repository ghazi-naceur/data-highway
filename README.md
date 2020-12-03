Yet another data converter.

You can convert your data to multiple data types.

**Environment :**

- Docker 19.03.13
- Docker compose 1.25.0
- JDK 1.8
- sbt 2.12.x
- Spark 2.4.7
- Hadoop 3.1.3
- Confluent community 6.0.0

## Table of contents :
* [A- Getting started](#A--getting-started-)
    * [1- Run data-highway locally](#1--run-data-highway-locally-)
    * [2- Run data-highway using Docker](#2--run-data-highway-using-docker-)
        * [a- Spark application](#a--spark-application)
        * [b- Kafka application without Confluent Cluster](#b--kafka-application-without-confluent-cluster)
        * [c- Kafka application with Confluent Cluster](#c--kafka-application-with-confluent-cluster)
* [B- Conversions](#B--conversions-)
    * [1- JSON conversion](#1--json-conversion-)
        * [a- From Parquet to JSON](#a--from-parquet-to-json-)
        * [b- From CSV to JSON](#b--from-csv-to-json-)
        * [c- From Avro to JSON](#c--from-avro-to-json-)
    * [2- Parquet conversion](#2--parquet-conversion-)
        * [a- From JSON to Parquet](#a--from-json-to-parquet-)
        * [b- From CSV to Parquet](#b--from-csv-to-parquet-)
        * [c- From Avro to Parquet](#c--from-avro-to-parquet-)
    * [3- CSV conversion](#3--csv-conversion-)
        * [a- From JSON to CSV](#a--from-json-to-csv-)
        * [b- From Parquet to CSV](#b--from-parquet-to-csv-)
        * [c- From Avro to CSV](#c--from-avro-to-csv-)
        * [d- From XLSX (or XLS) to CSV](#d--from-xlsx-(or-xls)-to-csv-)
    * [4- Avro conversion](#4--avro-conversion-)
        * [a- From Parquet to Avro](#a--from-parquet-to-avro-)
        * [b- From Json to Avro](#b--from-json-to-avro-)
        * [c- From Csv to Avro](#c--from-csv-to-avro-)
    * [5- Send data to Kafka](#5--send-data-to-kafka-)
        * [a- Simple Kafka Producer](#a--simple-kafka-producer-)
        * [b- Kafka Streaming](#b--kafka-streaming-)
        * [c- Spark Kafka Producer Plugin](#c--spark-kafka-producer-plugin-)
    * [6- Consume data from Kafka](#6--consume-data-from-kafka-)
        * [a- Simple Kafka Consumer](#a--simple-kafka-consumer-)
        * [b- Kafka Streaming](#b--kafka-streaming-)
        * [c- Spark Kafka Consumer Plugin](#c--spark-kafka-consumer-plugin-)
* [C- Scheduling](#C--scheduling-)

# A- Getting started :

## 1- Run data-highway locally :


You can run data-highway locally/manually by :

1- Cloning the project :
````shell script
git clone https://github.com/ghazi-naceur/data-highway.git
````

2- Compiling the project :
````shell script
sbt clean; sbt compile; sbt assembly;
````

3- Move your generated jar file which will be under the folder : `data-highway/target/scala-2.12/data-highway-assembly-0.1.jar`
to your delivery folder, along with the `application.conf` and `log4j2.properties` files (which are located under the `resources` folder).

4- Modify the `application.conf` file using the **B- Conversions** section of this `readme` file.

5- Run **data-highway** application using the following commands :

If you are using Spark, run the following command with these routes : (See the **B- Conversions** section)
 * B-1- JSON Conversion
 * B-2- Parquet Conversion
 * B-3- CSV Conversion
 * B-4- Avro Conversion
 * B-5-c- Sending data to Kafka though **Spark Kafka Producer Plugin**
 * B-6-c- Consuming data from Kafka though **Spark Kafka Consumer Plugin**
 
````shell script
spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/the/path/to/application.conf" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/the/path/to/application.conf" \
      --files "/the/path/to/application.conf" \
      /the/path/to/data-highway-assembly-0.1.jar
````

If you are using 'pure' Kafka (not the spark-kafka-plugin feature), run instead the following command with these routes : (See the **B- Conversions** section)
 * B-5-a- Sending data to Kafka though **Simple Kafka Producer**
 * B-5-b- Sending data to Kafka though **Kafka Streaming**
 * B-6-a- Consuming data from Kafka though **Simple Kafka Consumer**
 * B-6-b- Consuming data from Kafka though **Kafka Streaming**
 
````shell script
java -jar -Dconfig.file=/the/path/to/application.conf -Dlog4j2.configuration=/the/path/to/log4j2.properties /the/path/to/data-highway-assembly-0.1.jar

````

## 2- Run data-highway using Docker :

### a- Spark application:

**Note** : The supported Spark applications are all routes for : (See **B- Conversions** section to configure the module)
  * B-1- JSON Conversion
  * B-2- Parquet Conversion
  * B-3- CSV Conversion
  * B-4- Avro Conversion
  * B-5-c- Sending data to Kafka though **Spark Kafka Producer Plugin**
  * B-6-c- Consuming data from Kafka though **Spark Kafka Consumer Plugin**
 
1- Clone the data-highway project

2- Set the input/output volumes and the path to your configuration file by modifying the `docker-compose.yml` file located under `data-highway/docker/spark`:
```yaml
app:
    build: .
    image: data-highway-spark:v1.0
    container_name: bungee-gum-spark
    volumes:
      - /the-path-to-input-data-located-in-your-host-machine/:/app/data/input
      - /the-path-to-the-generated-output-in-your-host-machine/:/app/data/output
      - /the-path-to-your-config-file/application.conf:/app/config/application.conf
      - /the-path-to-your-log-file/log4j2.properties:/app/config/log4j2.properties
    entrypoint: ["spark-submit",
                  "--packages", "org.apache.spark:spark-avro_2.12:2.4.0",
                  "--class", "io.oss.data.highway.App",
                  "--master", "local[*]",
                  "--conf", "spark.driver.extraJavaOptions=-Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties",
                  "--conf", "spark.executor.extraJavaOptions=-Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties",
                  "--files", "/app/config/application.conf,/app/config/log4j2.properties",
                  "/app/jar/data-highway-assembly-0.1.jar"]
```
3- Run the script `start.sh` located under the path `data-highway/docker/spark`

ps: You can find some input data samples under the test package, which you can use as an input for the input docker volume.

4- Data will be generated under the output volume declared in the Dockerfile for sections "B-1", "B-2", "B-3", "B-4" and "B-6-c".

In the section "B-5-c" case, data will be published to the output topic.

### b- Kafka application without Confluent Cluster:

**Note** : The supported Kafka applications are all routes for : (See **B- Conversions** section to configure the module)
 * B-5-a- Sending data to Kafka though **Simple Kafka Producer**
 * B-5-b- Sending data to Kafka though **Kafka Streaming**
 * B-6-a- Consuming data from Kafka though **Simple Kafka Consumer**
 * B-6-b- Consuming data from Kafka though **Kafka Streaming**
 
1- Clone the data-highway project

2- Set the input/output volumes and the path to your configuration file by modifying the `docker-compose.yml` file located under `data-highway/docker/data-highway`:
```yaml
  app:
    build: .
    image: data-highway-app:v1.0
    container_name: bungee-gum-app
    volumes:
      - /the-path-to-input-data-located-in-your-host-machine/:/app/data/input # Used for sections "B-5-a" and "B-5-b"
      - /the-path-to-the-generated-output-in-your-host-machine/:/app/data/output # Used for sections "B-6-a" and "B-6-b"
      - /the-path-to-your-config-file/application.conf:/app/config/application.conf
      - /the-path-to-your-log-file/log4j2.properties:/app/config/log4j2.properties
    entrypoint: [ "java", "-jar", "-Dconfig.file=/app/config/application.conf", "-Dlog4j2.configuration=/app/config/log4j2.properties", "/app/jar/data-highway-assembly-0.1.jar" ]
    network_mode: "host"
```

3- Run the script `start.sh` located under the path `data-highway/docker/data-highway`

4- In the case of sections "B-5-a" and "B-5-b", data will be published to the output topic.

In the case of sections "B-6-a" and "B-6-b", data will be saved in the provided output volume.

### c- Kafka application with Confluent Cluster:

**Note** : The supported Kafka applications are all routes for : (See **B- Conversions** section to configure the module)
 * B-5-a- Sending data to Kafka though **Simple Kafka Producer**
 * B-5-b- Sending data to Kafka though **Kafka Streaming**
 * B-6-a- Consuming data from Kafka though **Simple Kafka Consumer**
 * B-6-b- Consuming data from Kafka though **Kafka Streaming**
 
1- Clone the data-highway project

2- Set the input/output volumes and the path to your configuration file by modifying the `docker-compose.yml` file located under `data-highway/docker/kafka`:
```yaml
app:
    build: .
    image: data-highway-kafka:v1.0
    container_name: bungee-gum-kafka
    volumes:
      - /the-path-to-input-data-located-in-your-host-machine/:/app/data/input # Used for sections "B-5-a" and "B-5-b"
      - /the-path-to-the-generated-output-in-your-host-machine/:/app/data/output # Used for sections "B-6-a" and "B-6-b"
      - /the-path-to-your-config-file/application.conf:/app/config/application.conf
      - /the-path-to-your-log-file/log4j2.properties:/app/config/log4j2.properties
    entrypoint: ["java", "-jar", "-Dconfig.file=/app/config/application.conf", "-Dlog4j2.configuration=/app/config/log4j2.properties", "/app/jar/data-highway-assembly-0.1.jar"]
```

3- Run the script `start.sh` located under the path `data-highway/docker/kafka`

4- In the case of sections "B-5-a" and "B-5-b", data will be published to the output topic.

In the case of sections "B-6-a" and "B-6-b", data will be saved in the provided output volume.

# B- Conversions :

## 1- JSON conversion :

There are 3 provided conversions.

Update the `route` configuration in the `application.properties` file :

#### a- From Parquet to JSON :

````hocon
route {
  type = parquet-to-json
  in = "your-input-folder-containing-parquet-files"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
}
````

#### b- From CSV to JSON :

````hocon
route {
  type = csv-to-json
  in = "your-input-folder-containing-csv-files"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
}
````

#### c- From Avro to JSON : 

````hocon
route {
  type = avro-to-json
  in = "your-input-folder-containing-avro-files"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
}
````

## 2- Parquet conversion :

There are 3 provided conversions.

Update the `route` configuration in the `application.properties` file :

#### a- From JSON to Parquet :

````hocon
route {
  type = json-to-parquet
  in = "your-input-folder-containing-json-files"
  out = "your-output-folder-that-will-contain-your-generated-parquet-files"
}
````

#### b- From CSV to Parquet :

````hocon
route {
  type = csv-to-parquet
  in = "your-input-folder-containing-csv-files"
  out = "your-output-folder-that-will-contain-your-generated-parquet-files"
}
````

#### c- From Avro to Parquet :

````hocon
route {
  type = avro-to-parquet
  in = "your-input-folder-containing-avro-files"
  out = "your-output-folder-that-will-contain-your-generated-parquet-files"
}
````

## 3- CSV conversion :

There are 4 provided conversions.

Update the `route` configuration in the `application.properties` file :

#### a- From JSON to CSV :

````hocon
route {
  type = json-to-csv
  in = "your-input-folder-containing-json-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

#### b- From Parquet to CSV :

````hocon
route {
  type = parquet-to-csv
  in = "your-input-folder-containing-parquet-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

#### c- From Avro to CSV :

````hocon
route {
  type = avro-to-csv
  in = "your-input-folder-containing-avro-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

#### d- From XLSX (or XLS) to CSV :

It consists of converting the different sheets of an XLSX or XLS file to multiple csv files.

````hocon
route {
  type = xlsx-to-csv # This value is supported for both xlsx and xls files (no value with the name "xls-to-csv")
  in = "your-input-folder-containing-xlsx-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

## 4- Avro conversion :

There are 3 provided conversions.

Update the `route` configuration in the `application.properties` file :

#### a- From Parquet to Avro :

```hocon
route {
  type = parquet-to-avro
  in = "your-input-folder-containing-parquet-files"
  out = "your-output-folder-that-will-contain-your-generated-avro-files"
}
```

#### b- From Json to Avro :

```hocon
route {
  type = json-to-avro
  in = "your-input-folder-containing-json-files"
  out = "your-output-folder-that-will-contain-your-generated-avro-files"
}
```

#### c- From Csv to Avro :

```hocon
route {
    type = csv-to-avro
  in = "your-input-folder-containing-csv-files"
  out = "your-output-folder-that-will-contain-your-generated-avro-files"
}
```

## 5- Send data to Kafka :

This mode consists of publishing json files content into an output topic.

It is available using 4 types of routes :
   * a- Simple Kafka Producer 
   * b- Kafka Streaming 
   * c- Spark Kafka Producer Plugin :
        - without streaming
        - with streaming


#### a- Simple Kafka Producer :

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

#### b- Kafka Streaming :

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

#### c- Spark Kafka Producer Plugin :

````hocon
route {
  type = json-to-kafka
  in = "your-input-folder-containing-json-files"
  out = "your-output-kafka-topic"
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas", // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "spark-kafka-producer-plugin"
      use-stream = false/true
  }
}
````

## 6- Consume data from Kafka :

ps: This feature is experimental. Its main goal is to get some data samples from an input topic.

This mode consists of consuming and input topic and saving its content in files.

It is available using 4 types of routes :
   * a- Simple Kafka Consumer 
   * b- Kafka Streaming 
   * c- Spark Kafka Consumer Plugin :
        - without streaming
        - with streaming

#### a- Simple Kafka Consumer :

````hocon
route {
  type = kafka-to-file
  in = "topic-name"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
  data-type = {
    type = "the-desired-datatype-of-the-generated-files" // Optional field : accepted values are json, avro and kafka (default value, if not set). 
             // kafka value refer to "txt" extension, that will be set as an extension for the generated files.
  }
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas"  // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = simple-consumer
  }
  offset = "offset-to-consume-from" // accepted values : earliest, latest, none
  consumer-group = "your-consumer-group-name"
}
````

#### b- Kafka Streaming :

````hocon
route {
  type = kafka-to-file
  in = "topic-name"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
  data-type = {
    type = "the-desired-datatype-of-the-generated-files" // Optional field : accepted values are json, avro and kafka (default value, if not set). 
             // kafka value refer to "txt" extension, that will be set as an extension for the generated files.
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

#### c- Spark Kafka Consumer Plugin :

````hocon
route {
  type = kafka-to-file
  in = "topic-name"
  out = "your-output-folder-that-will-contain-your-generated-json-files"
  data-type = {
    type = "the-desired-datatype-of-the-generated-files" // Optional field : accepted values are json, avro and kafka (default value, if not set). 
             // kafka value refer to "txt" extension, that will be set as an extension for the generated files.
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

# C- Scheduling :

Under the `data-highway/airflow/dag` folder, you will find an Airflow DAG sample, that runs your data-highway application with Airflow. 
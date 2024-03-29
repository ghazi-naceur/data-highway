Yet another ETL.

Using **data-highway**, you can convert your data to multiple data types or send them to other tools.
The actual supported data types are : **JSON**, **CSV**, **PARQUET**, **AVRO**, **ORC** , **XML** and **XLSX**.
**Data Highway** interacts as well with **Cassandra**, **Elasticsearch**, **Postgres** and **Kafka**, using multiple modes.

For example, **Data Highway** allows you to :
  - interact with different technologies through a user-friendly **RESTful API**
  - produce **PARQUET**(avro, csv, json, orc or xlsx) files into a **Kafka** topic
  - index **AVRO**(parquet, csv, json or xlsx) files into an **Elasticsearch** index
  - insert **XLSX**(avro, csv, json or parquet) files into a **Cassandra** Table
  - convert **CSV**(avro, parquet, json, orc or xlsx) files to **JSON**(avro, csv, parquet, orc or xlsx)
  - convert or send files located in your **Local File System** or in **HDFS**

In short, **Data Highway** supports the following data flow :
![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/data-highway_data-flow.png?raw=true)

**Environment :**

- Docker 20.10.2
- Docker compose 1.25.0
- JDK 1.8
- Scala 2.12.12
- Spark 2.4.6
- Elasticsearch 7.10.2
- Cassandra 4.0.0
- Kafka 2.8.0
- PostgreSQL 12.8

## Table of contents :
* [A- Getting started](#A--getting-started)
    * [1- Run data-highway jar](#1--run-data-highway-jar)
    * [2- Run data-highway Docker Image](#2--run-data-highway-docker-image)
* [B- Routes](#B--routes)
    * [1- Query structure](#1--query-structure)
    * [2- Notes about data types](#2--notes-about-data-types)
      * [a- Note about CSV](#a--note-about-csv)
      * [b- Note about XML](#b--note-about-xml)
      * [c- Note about Parquet](#c--note-about-parquet)
      * [d- Note about ORC](#d--note-about-orc)
* [C- Samples](#C--samples)
  * [I- File as an input](#I--file-as-an-input)
      * [1- File to File](#1--file-to-file)
      * [2- File to Kafka](#2--file-to-kafka)
          * [a- File to Kafka using Kafka Producer](#a--file-to-kafka-using-kafka-producer)
          * [b- File to Kafka using Spark Kafka Connector](#b--file-to-kafka-using-spark-kafka-connector)
      * [3- File to Postgres](#3--file-to-postgres)
      * [4- File to Cassandra](#4--file-to-cassandra)
      * [5- File to Elasticsearch](#5--file-to-elasticsearch)
  * [II- Kafka as an input](#II--kafka-as-an-input)
      * [1- Kafka to File](#1--kafka-to-file)
          * [a- Consumer with continuous poll to File](#a--consumer-with-continuous-poll-to-file)
          * [b- Consumer with Kafka Streams to File](#b--consumer-with-kafka-streams-to-file)
          * [c- Spark Kafka Connector to File](#c--spark-kafka-connector-to-file)
      * [2- Kafka to Kafka](#2--kafka-to-kafka)
          * [a- Kafka Streams](#a--kafka-streams)
          * [b- Spark Kafka Streaming Connector](#b--spark-kafka-streaming-connector)
      * [3- Kafka to Postgres](#3--kafka-to-postgres)
          * [a- Consumer with continuous poll to Postgres](#a--consumer-with-continuous-poll-to-postgres)
          * [b- Consumer with Kafka Streams to Postgres](#b--consumer-with-kafka-streams-to-postgres)
          * [c- Spark Kafka Connector to Postgres](#c--spark-kafka-connector-to-postgres)
      * [4- Kafka to Cassandra](#4--kafka-to-cassandra)
          * [a- Consumer with continuous poll to Cassandra](#a--consumer-with-continuous-poll-to-cassandra)
          * [b- Consumer with Kafka Streams to Cassandra](#b--consumer-with-kafka-streams-to-cassandra)
          * [c- Spark Kafka Connector to Cassandra](#c--spark-kafka-connector-to-cassandra)
      * [5- Kafka to Elasticsearch](#5--kafka-to-elasticsearch)
          * [a- Consumer with continuous poll to Elasticsearch](#a--consumer-with-continuous-poll-to-elasticsearch)
          * [b- Consumer with Kafka Streams to Elasticsearch](#b--consumer-with-kafka-streams-to-elasticsearch)
          * [c- Spark Kafka Connector to Elasticsearch](#c--spark-kafka-connector-to-elasticsearch)
      * [6- Kafka Admin Operations](#6--kafka-admin-operations)
          * [a- Topic creation](#a--topic-creation)
          * [b- Topic deletion](#b--topic-deletion)
          * [c- Topic list](#c--topics-list)
  * [III- Postgres as an input](#III--postgres-as-an-input)  
      * [1- Postgres to File](#1--postgres-to-file)
      * [2- Postgres to Kafka](#2--postgres-to-kafka)
          * [a- Postgres to Kafka using Kafka Producer](#a--postgres-to-kafka-using-kafka-producer)
          * [b- Postgres to Kafka using Spark Kafka Connector](#b--postgres-to-kafka-using-spark-kafka-connector)
      * [3- Postgres to Postgres](#3--postgres-to-postgres)
      * [4- Postgres to Cassandra](#22--postgres-to-cassandra)
      * [5- Postgres to Elasticsearch](#5--postgres-to-elasticsearch)
  * [IV- Cassandra as an input](#IV--cassandra-as-an-input)
      * [1- Cassandra to File](#1--cassandra-to-file)
      * [2- Cassandra to Kafka](#2--cassandra-to-kafka)
          * [a- Cassandra to Kafka using Kafka Producer](#a--cassandra-to-kafka-using-kafka-producer)
          * [b- Cassandra to Kafka using Spark Kafka Connector](#b--cassandra-to-kafka-using-spark-kafka-connector)
      * [3- Cassandra to Postgres](#3--cassandra-to-postgres)
      * [4- Cassandra to Cassandra](#4--cassandra-to-cassandra)
      * [5- Cassandra to Elasticsearch](#5--cassandra-to-elasticsearch)   
  * [V- Elasticsearch as an input](#V--elasticsearch-as-an-input)      
      * [0- Elasticsearch Search Queries](#0--elasticsearch-search-queries)
      * [1- Elasticsearch to File](#1--elasticsearch-to-file)
      * [2- Elasticsearch to Kafka](#2--elasticsearch-to-kafka)
          * [a- Elasticsearch to Kafka using Kafka Producer](#a--elasticsearch-to-kafka-using-kafka-producer)
          * [b- Elasticsearch to Kafka using Spark Kafka Connector](#b--elasticsearch-to-kafka-using-spark-kafka-connector)
      * [3- Elasticsearch to Postgres](#3--elasticsearch-to-postgres)
      * [4- Elasticsearch to Cassandra](#4--elasticsearch-to-cassandra)
      * [5- Elasticsearch to Elasticsearch](#5--elasticsearch-to-elasticsearch)
      * [6- Elasticsearch Admin Operations](#6--elasticsearch-admin-operations)
          * [a- Index creation](#a--index-creation)
          * [b- Index mapping](#b--index-mapping)
          * [c- Index deletion](#c--index-deletion)    
      

* [D- Scheduling](#D--scheduling)

# A- Getting started:

## 1- Run data-highway jar:

1- Download the latest **zip** release file of **Data Highway** from the [data-highway releases](https://github.com/ghazi-naceur/data-highway/releases)

2- Unzip your **Data Highway** zip release file

3- Enter the unzipped folder 

4- Set the configurations in the **application.conf** and **logback.xml** files
  - **application.conf**:
```json
spark {
  app-name = your-app-name // The name you can give to your Spark app (you can change it as you want)
  master-url = "local[*]" // your spark app host (leave it as is)
  generate-success-file = false // Specify if we want to generate a SUCCESS file after processing data or not
  log-level = {
    type = warn // The spark log level (supported values: info, warn and error)
  }
}

elasticsearch {
  host = "your-elacticsearch-host" // eg: "http://localhost"
  port = "your-elasticsearch-port" // eg: "9200"
}

hadoop {
  host = "your-hadoop-host" // eg: "hdfs://localhost"
  port = "your-hadoop-port" // eg: "9000"
}

cassandra {
  host = "your-cassandra-host" // eg: "localhost"
  port = "your-cassandra-port" // eg: "9042" 
}

postgres {
  host = "your-postgres-host" // eg: "jdbc:postgresql://localhost"
  port = "your-postgres-port" // eg: "5432"
  user = "your-postgres-user" // eg: "admin" 
  password = "your-postgres-password" // eg: "admin"
}

app {
  tmp-work-dir = "/tmp/data-highway-playground" // Specify the temporary folder used to compute intermediate dataset.
                                                // You need want to specify a folder that exposes enough ACLs for reading and writing.
}
```
- **logback.xml**:
```xml
 <appender name="FILE" class="ch.qos.logback.core.FileAppender">
    <file>logs/data-highway.log</file> // Here you can specify the path of the generated log file
    .......
</appender>

<root level="info"> // Here you can specify the log level 
    <appender-ref ref="STDOUT" />
    <appender-ref ref="FILE" />
</root>
```

5- Run your **data-highway** instance by executing the **start.sh** script :
```shell
chmod +x start.sh
./start.sh
```
![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/data-highway-banner.png?raw=true "Data Highway Launch Banner")

6- Finally, convert or send data using the data-highway RESTful API. You can find some POSTMAN query samples in the following folder 
[REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman). You can import this collection of queries 
in your POSTMAN instance. You will find more samples here in this documentation in section [C- Samples](#C--samples-)

## 2- Run data-highway Docker Image:

1- Clone this repository

2- Set configurations in the **data-highway/src/main/resources/application.conf** and **data-highway/src/main/resources/logback.xml** files 
(to be mounted in the next step)

3- Specify your mounted volumes in the `docker-compose.yml` file located under `data-highway/docker/rest/generic` and 
specify your data-highway version (located in the release file name) :
```yaml
  app:
    build: .
    image: data-highway-app:v${version}
    ports:
      - "5555:5555"
    container_name: bungee-gum-app
    volumes:
      - /the-path-to-csv-input-data-located-in-your-host-machine/:/app/data/csv/input
      - /the-path-to-csv-output-data-located-in-your-host-machine/:/app/data/csv/output
      - /the-path-to-csv-processed-data-located-in-your-host-machine/:/app/data/csv/processed
      - ........
      - ........
      - /the-path-to-your-log4j-conf-file/logback.xml:/app/config/logback.xml
      - /the-path-to-your-app-config-file/application.conf:/app/config/application.conf
    network_mode: "host"
    entrypoint: ["java", "-cp", "/app/jar/data-highway-${version}.jar", "gn.oss.data.highway.IOMain", "-Dlog4j.configuration=/app/config/logback.xml", "-Dconfig.file=/app/config/application.conf"]
```
4- Run the `start.sh` script under `data-highway/docker/rest/generic` to generate your Data Highway Docker image.

5- Run your HTTP request. You can find POSTMAN queries samples here : [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).
You can find as well some data samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources/data).

# B- Routes:

## 1- Query structure:

**Data-Highway** channel is composed by types of routes :

  - **Conversion route**: It can be performed by invoking an HTTP POST request on the url `http://localhost:5555/conversion/route`, 
    with the following HTTP body, composed by at least 2 of these 4 elements: `input`, `output`, `storage` and `save-mode`:
  
    - `input`: represents the input entity. It can be of type: `file`, `elasticsearch`, `cassandra` or `kafka`.
      Each input type has its own properties.
    - `output`:  represents the output entity. It can be of type: `file`, `elasticsearch`, `cassandra` or `kafka`.
      Each output type has its own properties.
    - `storage`: It represents the file system storage to be used in this route. It can `local` to represent the Local File System
      or `hdfs` to represent the Hadoop Distributed File System. This property is optional, so it may not be present in some routes,
      depending on the types of the `input` and `output` entities.
    - `save-mode`: It represents the Spark Sava Mode to be used in this route. It can be `overwrite`, `append`, `error-if-exists` and 
      `ignore`. This property is optional, so it may not be present in some routes, depending on the types of the `input` and `output`
      entities.
  
  - **Admin Operation route**: It represents a non-conversion route. It can be performed by invoking the url `http://localhost:5555/conversion/query`.
      Currently, it contains index operations that can be performed in an Elasticsearch Cluster (`index-creation`, `index-mapping` and `index-deletion`).

When dealing with the `file` input entity, **Data Highway** will move all the processed files from the `input` folder to a `processed` folder, 
that will be created automatically in the base path of the `input` folder:

```shell
path/to/data/
        ├── input/
        ├────────dataset-1/person-1.json
        ├─────────────────/person-2.json
        ├─────────────────/person-3.json
        ├────────dataset-2/person-4.json
        ├─────────────────/person-5.json
        ├─────────────────/person-6.json
        .............
        .............
        ├── processed/
        ├────────dataset-1/person-1.json
        ├─────────────────/person-2.json
        ├─────────────────/person-3.json
        ├────────dataset-2/person-4.json
        ├─────────────────/person-5.json
        ├─────────────────/person-6.json
```

## 2- Notes about data types:

### a- Note about CSV:

If your input or output **File** type is **CSV**, you need to provide some additional information to ensure a successful processing
in much secure way. You need to configure your entity as follows:
```json
.....
"input": { // or "output"
      "type": "file",
      "data-type": {
        "type": "csv",
        "infer-schema": true, // infer schema when reading or writing the data as CSV. It can be "true" or "false"
        "header": true, // Specify that the first line of the CSV file is a header. It can be "true" or "false" 
        "separator": ";" // Specify the separator between fields in the CSV file
      },
      "path": "/path/to/data/csv/input"
},
......
```

### b- Note about XML:

If your input or output **File** type is **CSV**, you need to provide some additional properties:
```json
.........
"output": { // or input
  "type": "file",
  "data-type": {
    "type": "xml",
    "root-tag": "persons", // The root tag for the XML file
    "row-tag": "person" // The row tag for the XML record/row
  },
  "path": "/path/to/data/xml/output"
}
.........
```

### c- Note about Parquet:

You can set **compression** for these **output** data types as follows :
- **Parquet**: The supported compression types for parquet are **snappy** and **gzip**:
```json
...........
  "output": {
    "type": "file",
    "data-type": {
        "type": "parquet",
        "compression": {  // option field
            "type": "gzip" // supported values are: snappy' and 'gzip'
        }
    },
    "path": "/path/to/parquet/output"
  }
..........
```

### d- Note about ORC:

You can set **compression** for these **output** data types as follows :
- **ORC**: The supported compression types for orc are **snappy**, **lzo** and **zlib**:
```json
...........
  "output": {
    "type": "file",
    "data-type": {
        "type": "orc",
        "compression": { // option field
            "type": "snappy" // supported values are: snappy', 'lzo', and 'zlib'
        }
    },
    "path": "/path/to/orc/output"
  }
..........
```

# C- Samples:

The following section provides some samples that you can find here [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).
You can find some Postman query samples in the following link, and you can import them into your Postman instance 
[REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).

You can find as well some data samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources/data).

## I- File as an input

### 1- File to File:

This a one-time job.

This query will convert `parquet` files to `avro`. all the processed datasets will be moved under the folder `/path/to/data/processed`,
that will be created automatically.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": { // 1- The input section
      "type": "file",
      "data-type": {
        "type": "parquet" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/parquet/input"
    },
    "output": { // 2- The output section
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/avro/output"
    },
    "storage": { // 3- The storage section
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": { // 4- The save mode section
      "type": "overwrite" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 2- File to Kafka:

This a one-time job.

You can produce data to a Kafka topic using one of these 2 modes : `Pure Kafka Producer` or `Spark Kafka Plugin`. 

#### a- File to Kafka using Kafka Producer:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/avro/input"
    },
    "output": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode": {
        "type": "pure-kafka-producer",
        "brokers" : "broker-host:broker-port" // eg: localhost:9092
      }
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    }
  }
}
```

#### b- File to Kafka using Spark Kafka Connector:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "json" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/input"
    },
    "output": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode": {
        "type": "spark-kafka-plugin-producer",
        "brokers" : "broker-host:broker-port" // eg: localhost:9092
      }
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    }
  }
}
```

### 3- File to Postgres:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": {
      "type": "append"  // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 4- File to Cassandra:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": {
      "type": "append"  // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 5- File to Elasticsearch:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "json" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/input"
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-index",
      "bulk-enabled": false // supported values: 'true' and 'false'
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    }
  }
}
```

## II- Kafka as an input

### 1- Kafka to File:

You can save data in files by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or
1 Batch mode by `Spark Kafka Connector`.

#### a- Consumer with continuous poll to File:

This is a long-running job. 

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "consumer-group" : "your-consumer-group",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "json" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/input"
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}

```

#### b- Consumer with Kafka Streams to File:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-streams-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "stream-app-id" : "your-app-id",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "parquet" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/parquet/input"
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

#### c- Spark Kafka Connector to File:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "spark-kafka-plugin-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/avro/input"
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 2- Kafka to Kafka:

You can mirror topic by triggering one of these 2 Streaming modes using :`Kafka Streams` or `Spark Kafka Streaming Connector`.

#### a- Kafka Streams:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-input-kafka-topic",
      "kafka-mode": {
        "type": "pure-kafka-streams-producer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "stream-app-id" : "your-stream-app-id",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "kafka",
      "topic": "your-output-kafka-topic"
    }
  }
}
```

#### b- Spark Kafka Streaming Connector:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-input-kafka-topic",
      "kafka-mode" : {
        "type" : "spark-kafka-plugin-streams-producer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "kafka",
      "topic": "your-output-kafka-topic"
    }
  }
}

```

### 3- Kafka to Postgres:

You can insert data in Postgres by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or
1 Batch mode by `Spark Kafka Connector`.

#### a- Consumer with continuous poll to Postgres:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "consumer-group" : "your-consumer-group",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

#### b- Consumer with Kafka Streams to Postgres:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-streams-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "stream-app-id" : "your-stream-app-id",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

#### c- Spark Kafka Connector to Postgres:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "spark-kafka-plugin-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 4- Kafka to Cassandra:

You can insert data in Cassandra by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or 
1 Batch mode by `Spark Kafka Connector`.

#### a- Consumer with continuous poll to Cassandra:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "consumer-group" : "your-consumer-group",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

#### b- Consumer with Kafka Streams to Cassandra:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-streams-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "stream-app-id" : "your-stream-app-id",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

#### c- Spark Kafka Connector to Cassandra:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "spark-kafka-plugin-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 5- Kafka to Elasticsearch:

You can index data into Elasticsearch by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or
1 Batch mode by `Spark Kafka Connector`.

#### a- Consumer with continuous poll to Elasticsearch:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "consumer-group" : "your-consumer-group",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false // supported values: 'false' and 'true'
    }
  }
}
```

#### b- Consumer with Kafka Streams to Elasticsearch:

This is a long-running job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-streams-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "stream-app-id" : "your-stream-app-id",
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false // supported values: 'false' and 'true'
    }
  }
}
```

#### c- Spark Kafka Connector to Elasticsearch:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "spark-kafka-plugin-consumer",
        "brokers" : "your-broker-host:your-broker-port", // eg: localhost:9092
        "offset" : {
          "type": "earliest" // supported values: earliest, latest, none
        }
      }
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false // supported values: 'false' and 'true'
    }
  }
}
```

### 6- Kafka Admin Operations:

#### a- Topic creation:

`POST http://localhost:5555/conversion/query`
```json
{
  "query": {
    "type": "kafka-ops",
    "operation": {
      "type": "topic-creation",
      "topic-name": "the-topic-name",
      "broker-hosts": "broker-host:broker-port", // eg: localhost:9092
      "partitions": nb-of-partitions,
      "replication-factor": replication-factor
    }
  }
}
```

#### b- Topic deletion:

`POST http://localhost:5555/conversion/query`
```json
{
  "route": {
    "type": "kafka-ops",
    "operation": {
      "type": "topic-deletion",
      "topic-name": "the-topic-name",
      "broker-hosts": "broker-host:broker-port" // eg: localhost:9092
    }
  }
}
```

#### c- Topics list:

`POST http://localhost:5555/conversion/query`
```json
{
  "route": {
    "type": "kafka-ops",
    "operation": {
      "type": "topics-list",
      "broker-hosts": "broker-host:broker-port" // eg: localhost:9092
    }
  }
}
```

## III- Postgres as an input

### 1- Postgres to File:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "json" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/output"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 2- Postgres to Kafka:

You can produce data to a Kafka topic using one of these 2 modes : `Pure Kafka Producer` or `Spark Kafka Plugin`.

#### a- Postgres to Kafka using Kafka Producer:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "output": {
      "type": "kafka",
      "topic": "your-output-kafka-topic",
      "kafka-mode": {
        "type": "pure-kafka-producer",
        "brokers" : "your-broker-host:your-broker-port" // eg: localhost:9092
      }
    }
  }
}
```

#### b- Postgres to Kafka using Spark Kafka Connector:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "output": {
      "type": "kafka",
      "topic": "your-output-kafka-topic",
      "kafka-mode": {
        "type": "spark-kafka-plugin-producer",
        "brokers" : "your-broker-host:your-broker-port" // eg: localhost:9092
      }
    }
  }
}
```

### 3- Postgres to Postgres:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 4- Postgres to Cassandra:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-output-keyspace",
      "table": "your-output-table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 5- Postgres to Elasticsearch:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false // supported values: 'false' and 'true'
    }
  }
}
```

## IV- Cassandra as an input

### 1- Cassandra to File:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "json" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/output"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 2- Cassandra to Kafka:

You can produce data to a Kafka topic using one of these 2 modes : `Pure Kafka Producer` or `Spark Kafka Plugin`.

#### a- Cassandra to Kafka using Kafka Producer:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "output": {
      "type": "kafka",
      "topic": "your-output-kafka-topic",
      "kafka-mode": {
        "type": "pure-kafka-producer",
        "brokers" : "your-broker-host:your-broker-port" // eg: localhost:9092
      }
    }
  }
}
```

#### b- Cassandra to Kafka using Spark Kafka Connector:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "output": {
      "type": "kafka",
      "topic": "your-output-kafka-topic",
      "kafka-mode": {
        "type": "spark-kafka-plugin-producer",
        "brokers" : "your-broker-host:your-broker-port" // eg: localhost:9092
      }
    }
  }
}
```

### 3- Cassandra to Postgres:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "cassandra",
      "keyspace": "your-input-keyspace",
      "table": "your-input-table"
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 4- Cassandra to Cassandra:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "cassandra",
      "keyspace": "your-input-keyspace",
      "table": "your-input-table"
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-output-keyspace",
      "table": "your-output-table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 5- Cassandra to Elasticsearch:

This is a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false // supported values: 'false' and 'true'
    }
  }
}
```

## V- Elasticsearch as an input

### 0- Elasticsearch Search Queries:

The Elasticsearch input section contain a `search-query` property that helps to filter the desired data to be sent to your output 
destination (File -with 5 supported data types-, Kafka, Cassandra or Elasticsearch). **Data Highway** provides 16 search queries :


**"search-query"** : Could be a :

a- "match-all-query" :
```json
...
    "search-query": {
          "type": "match-all-query"
    }
...
```
b- "match-query" :
```json
...
    "search-query": {
        "type": "match-query",
        "field": {
            "name": "field_name",
            "value": "field_value"
        }
    }
...
```
c- "multi-match-query" :
```json
...
    "search-query": {
        "type": "multi-match-query",
        "values": ["value-1", "value-2", "value-n"]
    }
...
```
d- "term-query" :
```json
...
    "search-query": {
        "type": "term-query",
        "field": {
            "name": "field_name",
            "value": "field_value"
        }
    }
...
```
e- "terms-query" :
```json
...
    "search-query": {
        "type": "terms-query",
        "field": {
            "name": "field_name",
            "values": ["value-1", "value-2", "value-n"]
        }
    }
...
```
f- "common-terms-query" :
```json
...
    "search-query": {
        "type": "common-terms-query",
        "field": {
            "name": "field_name",
            "value": "field_value"
        }
    }
...
```
g- "query-string-query" :
```json
...
    "search-query": {
        "type": "query-string-query",
        "query": "string-format elastic query"
    }
...
```
**"query"** should contain an elasticsearch string query such as for example, **"query"**: "(value-1) OR (value-2)"

h- "simple-string-query" :
```json
...
    "search-query": {
        "type": "simple-string-query",
        "query": "string-format elastic query"
    }
...
```
**"query"** should contain an elasticsearch string query such as for example, **"query"**: "(value-1) OR (value-2)"

i- "prefix-query" :
```json
...
    "search-query": {
        "type": "prefix-query",
        "prefix": {
            "field-name": "field_name",
            "value": "prefix_value"
        }
    }
...
```
j- "more-like-this-query" :
```json
...
    "search-query": {
        "type": "more-like-this-query",
        "like-fields": {
            "fields": ["field-1", "field-2", "field-n"],
            "like-texts": ["value-1", "value-2", "value-n"]
        }
    }
...
```
k- "range-query" :
```json
...
    "search-query": {
        "type": "range-query",
        "range-field": {
            "range-type": {
                "type": "range-type"
            },
            "name": "field-name",
            "lte": "lower than or equal value",
            "gte": "greater than or equal value"
        }
    }
...
```
**"range-type"** could have values like : **string-range**, **integer-range**, **long-range** or **float-range**.

**"lte"** and **"gte"** are optional fields.

l- "exists-query" :
```json
...
    "search-query": {
        "type": "exists-query",
        "field-name": "field-name"
    }
...
```
m- "wildcard-query" :
```json
...
    "search-query": {
        "type": "wildcard-query",
        "field": {
            "name": "field_name",
            "value": "wildcard_value"
        }
    }
...
```
n- "regex-query" :
```json
...
    "search-query": {
        "type": "regex-query",
        "field": {
            "name": "field_name",
            "value": "regex_value"
        }
    }
...
```
**"regex-query"** is used for strings indexed as **keyword**.

o- "fuzzy-query" :
```json
...
    "search-query": {
        "type": "fuzzy-query",
        "field": {
            "name": "field_name",
            "value": "field_value"
        }
    }
...
```
p- "ids-query" :
```json
...
    "search-query": {
        "type": "ids-query",
        "ids": ["id-1", "id-2", "id-n"]
    }
...
```
q- "bool-match-phrase-query" :
```json
...
    "search-query": {
        "type": "bool-match-phrase-query",
        "bool-filter": {
            "type": "bool-filter"
        },
        "fields": [
            {
                "name": "field_name-1",
                "value": "field_value-1"
            },
            {
                "name": "field_name-2",
                "value": "field_value-2"
            },
            {
                "name": "field_name-n",
                "value": "field_value-n"
            }
        ]
    }
...
```
**"bool-filter"** can have one of these values : **"must"**, **"must-not"** or **"should"**.


### 1- Elasticsearch to File:

This is a one-time job. See section [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries) to fill in the 
`search-query` property for your Search Query:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false, // supported values: 'false' and 'true'
      "search-query": {
        ....,
        // See section [12- Elasticsearch Search Queries] to fill in the `search-query` property
        ....
      }
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "json" // supported values: 'avro', 'orc'(See **Note about ORC**), 'parquet'(See **Note about Parquet**), 'xml'(See **Note about XML**), 'csv'(See **Note about CSV**), 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/output"
    },
    "storage": {
      "type": "local" // supported values: 'local' and 'hdfs'
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 2- Elasticsearch to Kafka:

#### a- Elasticsearch to Kafka using Kafka Producer:


This is a one-time job. See section [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries) to fill in the
`search-query` property for your Search Query:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false, // supported values: 'false' and 'true'
      "search-query": {
        ....,
        // See section [12- Elasticsearch Search Queries] to fill in the `search-query` property
        ....
      }
    },
    "output": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode": {
        "type": "pure-kafka-producer",
        "brokers" : "your-broker-host:your-broker-port" // eg: localhost:9092
      }
    }
  }
}
```

#### b- Elasticsearch to Kafka using Spark Kafka Connector:

This is a one-time job. See section [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries) to fill in the
`search-query` property for your Search Query:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false, // supported values: 'false' and 'true'
      "search-query": {
        ....,
        // See section [12- Elasticsearch Search Queries] to fill in the `search-query` property
        ....
      }
    },
    "output": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode": {
        "type": "spark-kafka-plugin-producer",
        "brokers" : "your-broker-host:your-broker-port" // eg: localhost:9092
      }
    }
  }
}
```

### 3- Elasticsearch to Postgres:

This is a one-time job. See section [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries) to fill in the
`search-query` property for your Search Query:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false, // supported values: 'false' and 'true'
      "search-query": {
        ....,
        // See section [12- Elasticsearch Search Queries] to fill in the `search-query` property
        ....
      }
    },
    "output": {
      "type": "postgres",
      "database": "your-database",
      "table": "your-table or your-schema.your-table" // "my_table" or "my_schema.my_table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 4- Elasticsearch to Cassandra:

This is a one-time job. See section [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries) to fill in the
`search-query` property for your Search Query:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "elasticsearch",
      "index": "your-elasticsearch-index",
      "bulk-enabled": false, // supported values: 'false' and 'true'
      "search-query": {
        ....,
        // See section [12- Elasticsearch Search Queries] to fill in the `search-query` property
        ....
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

### 5- Elasticsearch to Elasticsearch:

This is a one-time job. See section [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries) to fill in the
`search-query` property for your Search Query:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "elasticsearch",
      "index": "your-input-elasticsearch-index",
      "bulk-enabled": false, // supported values: 'false' and 'true'
      "search-query": {
        ....,
        // See section [12- Elasticsearch Search Queries] to fill in the `search-query` property
        ....
      }
    },
    "output": {
      "type": "elasticsearch",
      "index": "your-output-elasticsearch-index",
      "bulk-enabled": false // supported values: 'false' and 'true'
    }
  }
}
```

### 6- Elasticsearch Admin Operations:

#### a- Index creation:

`POST http://localhost:5555/conversion/query`
```json
{
  "route": {
    "type": "elastic-ops",
    "operation": {
      "type": "index-creation",
      "index-name": "index-name",
      "mapping": "{ \"properties\" : { ... }"
    }
  }
}
```
**mapping** is an optional field. Your Elasticsearch mapping should be inside the **properties** tag.

#### b- Index mapping:

`POST http://localhost:5555/conversion/query`
```json
{
  "route": {
    "type": "elastic-ops",
    "operation": {
      "type": "index-mapping",
      "index-name": "index-name",
      "mapping": "{ \"properties\" : { ... }"
    }
  }
}
```
It adds a mapping for an existing index.

#### c- Index deletion:

`POST http://localhost:5555/conversion/query`
```json
{
  "route": {
    "type": "elastic-ops",
    "operation": {
      "type": "index-deletion",
      "index-name": "index-name"
    }
  }
}
```

# D- Scheduling:

Under the `data-highway/airflow/dags` folder, you will find some Airflow DAG samples, that can help you to automate your data-highway application with Airflow. 
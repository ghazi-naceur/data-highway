Yet another ETL.

Using **data-highway**, you can convert your data to multiple data types or send them to other tools.
The actual supported data types are : **JSON**, **CSV**, **PARQUET**, **AVRO**, **ORC** and **XLSX**.
**Data Highway** interacts as well with **Cassandra**, **Elasticsearch** and **Kafka**, using multiple modes.

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

## Table of contents :
* [A- Getting started](#A--getting-started)
    * [1- Run data-highway jar](#1--run-data-highway-jar)
    * [2- Run data-highway Docker Image](#2--run-data-highway-docker-image)
* [B- Routes](#B--routes)
* [C- Samples](#C--samples)
    * [1- File to File](#1--file-to-file)
    * [2- File to Kafka](#2--file-to-kafka)
        * [a- File to Kafka using Kafka Producer](#a--file-to-kafka-using-kafka-producer)
        * [b- File to Kafka using Spark Kafka Connector](#b--file-to-kafka-using-spark-kafka-connector)
    * [3- File to Cassandra](#3--file-to-cassandra)
    * [4- File to Elasticsearch](#4--file-to-elasticsearch)
    * [5- Kafka to File](#5--kafka-to-file)
        * [a- Consumer with continuous poll to File](#a--consumer-with-continuous-poll-to-file)
        * [b- Consumer with Kafka Streams to File](#b--consumer-with-kafka-streams-to-file)
        * [c- Spark Kafka Connector to File](#c--spark-kafka-connector-to-file)
    * [6- Kafka to Kafka](#6--kafka-to-kafka)
        * [a- Kafka Streams](#a--kafka-streams)
        * [b- Spark Kafka Streaming Connector](#b--spark-kafka-streaming-connector)
    * [7- Kafka to Cassandra](#7--kafka-to-cassandra)
        * [a- Consumer with continuous poll to Cassandra](#a--consumer-with-continuous-poll-to-cassandra)
        * [b- Consumer with Kafka Streams to Cassandra](#b--consumer-with-kafka-streams-to-cassandra)
        * [c- Spark Kafka Connector to Cassandra](#c--spark-kafka-connector-to-cassandra)
    * [8- Cassandra to File](#8--cassandra-to-file)
    * [9- Cassandra to Kafka](#9--cassandra-to-kafka)
        * [a- Cassandra to Kafka using Kafka Producer](#a--cassandra-to-kafka-using-kafka-producer)
        * [b- Cassandra to Kafka using Spark Kafka Connector](#b--cassandra-to-kafka-using-spark-kafka-connector)
    * [10- Cassandra to Cassandra](#10--cassandra-to-cassandra)
    * [11- Cassandra to Elasticsearch](#11--cassandra-to-elasticsearch)
    * [12- Elasticsearch Search Queries](#12--elasticsearch-search-queries)
    * [13- Elasticsearch to File](#13--elasticsearch-to-file)
    * [14- Elasticsearch to Kafka](#14--elasticsearch-to-kafka)
        * [a- Elasticsearch to Kafka using Kafka Producer](#a--elasticsearch-to-kafka-using-kafka-producer)
        * [b- Elasticsearch to Kafka using Spark Kafka Connector](#b--elasticsearch-to-kafka-using-spark-kafka-connector)
    * [15- Elasticsearch to Cassandra](#15--elasticsearch-to-cassandra)
    * [16- Elasticsearch to Elasticsearch](#16--elasticsearch-to-elasticsearch)
    * [17- Elasticsearch Admin Operations](#17--elasticsearch-admin-operations)
        * [a- Index creation](#a--index-creation)
        * [b- Index mapping](#b--index-mapping)
        * [c- Index deletion](#c--index-deletion)
* [D- Scheduling](#D--scheduling)

# A- Getting started:

## 1- Run data-highway jar:

1- Download the latest **zip** release file of **Data Highway** from the [data-highway releases](https://github.com/ghazi-naceur/data-highway/releases)

2- Unzip your **Data Highway** zip release file

3- Enter the unzipped folder 

4- Set the configurations in the **application.conf** and **log4j.properties** files

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

2- Set configurations in the **data-highway/src/main/resources/application.conf** and **data-highway/src/main/resources/log4j.properties** files 
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
      - /the-path-to-your-log4j-conf-file/log4j.properties:/app/config/log4j.properties
      - /the-path-to-your-app-config-file/application.conf:/app/config/application.conf
    network_mode: "host"
    entrypoint: ["java", "-cp", "/app/jar/data-highway-${version}.jar", "gn.oss.data.highway.IOMain", "-Dlog4j.configuration=/app/config/log4j.properties", "-Dconfig.file=/app/config/application.conf"]
```
4- Run the `start.sh` script under `data-highway/docker/rest/generic` to generate your Data Highway Docker image.

5- Run your HTTP request. You can find POSTMAN queries samples here : [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).
You can find as well some data samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources/data).

# B- Routes:

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

# C- Samples:

The following section provides some samples that you can find here [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).
You can find some Postman query samples in the following link, and you can import them into your Postman instance 
[REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).

You can find as well some data samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources/data).

## 1- File to File:

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
        "type": "parquet" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/parquet/input"
    },
    "output": { // 2- The output section
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
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

You can set compression for these output data types as follows :
  - **Parquet**: The supported compression types for parquet are **none**, **uncompressed**, **snappy**, **gzip**, **lzo**, **brotli** and **lz4**
```json
...........
  "output": {
    "type": "file",
    "data-type": {
        "type": "parquet",
        "compression": {
            "type": "gzip" // supported values are: 'none', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli' and 'lz4'
        }
    },
    "path": "/path/to/parquet/output"
  }
..........
```
  - **ORC**: The supported compression types for orc are **none**, **snappy**, **lzo** and **zlib**
```json
...........
  "output": {
    "type": "file",
    "data-type": {
        "type": "orc",
        "compression": {
            "type": "snappy" // supported values are: 'none', 'snappy', 'lzo', and 'zlib'
        }
    },
    "path": "/path/to/parquet/orc"
  }
..........
```

## 2- File to Kafka:

This a one-time job.

You can produce data to a Kafka topic using one of these 2 modes : `Pure Kafka Producer` or `Spark Kafka Plugin`. 

### a- File to Kafka using Kafka Producer:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
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

### b- File to Kafka using Spark Kafka Connector:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "csv" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
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

## 3- File to Cassandra:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "avro" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
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

## 4- File to Elasticsearch:

This a one-time job.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "csv" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
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

## 5- Kafka to File:

You can save data in files by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or
1 Batch mode by `Spark Kafka Connector`.

### a- Consumer with continuous poll to File:

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
        "type": "csv" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
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

### b- Consumer with Kafka Streams to File:

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
        "type": "csv" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
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

### c- Spark Kafka Connector to File:

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
        "type": "csv" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/csv/input"
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

## 6- Kafka to Kafka:

You can mirror topic by triggering one of these 2 Streaming modes using :`Kafka Streams` or `Spark Kafka Streaming Connector`.

### a- Kafka Streams:

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

### b- Spark Kafka Streaming Connector:

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

## 7- Kafka to Cassandra:

You can insert data in Cassandra by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or 
1 Batch mode by `Spark Kafka Connector`.

### a- Consumer with continuous poll to Cassandra:

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

### b- Consumer with Kafka Streams to Cassandra:

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

### c- Spark Kafka Connector to Cassandra:

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

## 8- Kafka to Elasticsearch :

You can index data into Elasticsearch by triggering one of these 3 modes :
2 Streaming modes by :`Pure Consumer with continuous poll` and `Pure Consumer with Kafka Streams` or
1 Batch mode by `Spark Kafka Connector`.

### a- Pure consumer with continuous poll:

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

### b- Pure consumer with Kafka Streams:

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

### c- Spark Kafka Connector:

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

## 8- Cassandra to File:

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
        "type": "json" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
      },
      "path": "/path/to/data/json/output"
    },
    "save-mode": {
      "type": "append" // supported values: 'overwrite', 'append', 'error-if-exists' and 'ignore'
    }
  }
}
```

## 9- Cassandra to Kafka:

You can produce data to a Kafka topic using one of these 2 modes : `Pure Kafka Producer` or `Spark Kafka Plugin`.

### a- Cassandra to Kafka using Kafka Producer:

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

### b- Cassandra to Kafka using Spark Kafka Connector:

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

## 10- Cassandra to Cassandra:

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

## 11- Cassandra to Elasticsearch:

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

## 12- Elasticsearch Search Queries:

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


## 13- Elasticsearch to File:

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
        "type": "json" // supported values: 'avro', orc, 'parquet', 'csv', 'json' and 'xlsx'
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

## 14- Elasticsearch to Kafka:

### a- Elasticsearch to Kafka using Kafka Producer:


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

### b- Elasticsearch to Kafka using Spark Kafka Connector:

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

## 15- Elasticsearch to Cassandra:

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

## 16- Elasticsearch to Elasticsearch:

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

## 17- Elasticsearch Admin Operations:

### a- Index creation:

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

### b- Index mapping:

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

### c- Index deletion:

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
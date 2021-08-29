Yet another ETL.

Using **data-highway**, you can convert your data to multiple data types and send them to databases.
The actual supported data types are : **JSON**, **CSV**, **PARQUET**, **AVRO** and **XLSX**.
**Data Highway** interacts as well with **Cassandra**, **Elasticsearch** and **Kafka**, using multiple modes.

For example, **Data Highway** allows you to :
  - interacts with different technologies through a user-friendly **RESTful API**
  - produce content from **PARQUET**(avro, csv, json or xlsx) files into **Kafka** topics
  - index content from **AVRO**(parquet, csv, json or xlsx) files into **Elasticsearch** index
  - insert content from **XLSX**(avro, csv, json or parquet) files into **Cassandra** Table
  - convert **CSV**(avro, parquet, json or even xlsx) files to **JSON**(avro, csv, parquet or even xlsx)
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
* [A- Getting started](#A--getting-started-)
    * [1- Run data-highway jar ](#1--run-data-highway-jar-)
    * [2- Run data-highway Docker Image ](#2--run-data-highway-docker-image-)
* [B- Routes](#B--routes-)
* [C- Samples](#B--samples-)
    * [1- File to File](#1--file-to-file-)
    * [2- File to Kafka](#2--file-to-kafka-)
    * [3- Kafka to Elasticsearch](#3--kafka-to-elasticsearch-)
    * [4- Elasticsearch to Cassandra](#4--elasticsearch-to-cassandra-)
    * [5- Elasticsearch operations](#5--elasticsearch-operations-)
    * [6- and many more](#6--and-many-more-)
* [C- Scheduling](#C--scheduling-)

# A- Getting started :

## 1- Run data-highway jar :

1- Download the latest **zip** release file of **data-highway** from [data-highway releases](https://github.com/ghazi-naceur/data-highway/releases)

2- Unzip your **data-highway** zip release file

3- Enter the unzipped folder 

4- Set the configurations in the **application.conf** file

5- Run your **data-highway** instance by executing the **start.sh** script :
```shell
chmod +x start.sh
./start.sh
```
![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/data-highway-banner.png?raw=true "Data Highway Launch Banner")

6- Finally, convert or send data using the data-highway RESTful API. You can find some POSTMAN query samples in the following folder 
[REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman). You can import this collection of queries 
in your POSTMAN instance. 

You will find further explanations about **data-highway** RESTful API in the section [B- Routes](#B--routes-)

## 2- Run data-highway Docker Image :

1- Set configurations in the **/the-path-to-your-config-file/application.conf** (to be mounted in the next step)

2- After cloning this repository, specify your mounted volumes in the `docker-compose.yml` located under `data-highway/docker/rest/generic` and 
specify your data-highway version (located in the release file name) :
```yaml
  app:
    build: .
    image: data-highway-app:v${version}
    ports:
      - "5555:5555"
    container_name: bungee-gum-app
    volumes:
      - /the-path-to-spark-input-data-located-in-your-host-machine/:/app/data/spark/input
      - /the-path-to-the-generated-spark-output-in-your-host-machine/:/app/data/spark/output
      - /the-path-to-the-generated-spark-processed-in-your-host-machine/:/app/data/spark/processed
      - /the-path-to-kafka-input-data-located-in-your-host-machine/:/app/data/kafka/input
      - /the-path-to-the-generated-kafka-output-in-your-host-machine/:/app/data/kafka/output
      - /the-path-to-the-generated-kafka-processed-in-your-host-machine/:/app/data/kafka/processed
      - /the-path-to-your-log-file/log4j.properties:/app/config/log4j.properties
      - /the-path-to-your-config-file/application.conf:/app/config/application.conf
    network_mode: "host"
    entrypoint: ["java", "-cp", "/app/jar/data-highway-${version}.jar", "gn.oss.data.highway.IOMain", "-Dlog4j.configuration=/app/config/log4j.properties", "-Dconfig.file=/app/config/application.conf"]
```
3- Run the `start.sh` script under `data-highway/docker/rest/generic` to generate your Data Highway Docker image.

4- Run your HTTP request. You can find POSTMAN queries samples here : [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman).
You can find as well some data samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources/data).

# B- Routes :

A **Data-Highway** route can be triggered by an HTTP POST request. This request contains 3 parts: `input`, `output` and `storage`:
  
  - `input`: represents the input entity. It can be of `type`: `file`, `elasticsearch`, `cassandra` or `kafka`.
    Each type has its own properties.
  - `output`:  represents the output entity. It can be of `type`: `file`, `elasticsearch`, `cassandra` or `kafka`.
    Each type has its own properties.
  - `storage`: It represents the file system storage to be used in this route. It can `local` to represent the Local File System
    or `hdfs` to represent the Hadoop Distributed File System. This property is optional, so it may not be present in some routes,
    depending on the types of `input` and `output`. 

When dealing with the `file` input entity, **Data Highway** will move all the processed files into a `processed` folder, that will be 
created automatically in the base path of the `input` folder:

```shell
path/to/data/
        ├── input/dataset-1/person-1.json
        ├── input/dataset-1/person-2.json
        ├── input/dataset-1/person-3.json
        .............
        ├── input/dataset-2/person-1.json
        ├── input/dataset-2/person-2.json
        ├── input/dataset-2/person-3.json
        .............
        ├── processed/dataset-1/person-1.json
        ├── processed/dataset-1/person-2.json
        ├── processed/dataset-1/person-3.json
        .............
        ├── processed/dataset-2/person-1.json
        ├── processed/dataset-2/person-2.json
        ├── processed/dataset-2/person-3.json
```

# C- Samples :

The following section provides some samples that you can find here [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman)
You can find as well some data samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources/data).

## 1- File to File :

This query will convert `parquet` files to `avro`. all the processed datasets will be moved under the folder `/path/to/data/processed`,
that will be created automatically.

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "parquet"
      },
      "path": "/path/to/data/parquet/input"
    },
    "output": {
      "type": "file",
      "data-type": {
        "type": "avro"
      },
      "path": "/path/to/data/avro/output"
    },
    "storage": {
      "type": "local"
    }
  }
}
```

## 2- File to Kafka :

You can produce data to a Kafka topic by triggering one of these 2 modes : `Pure Kafka Producer` or `Spark Kafka Plugin`. 
Both of these modes provide the same functionality:

- `Pure Kafka Producer`:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "avro"
      },
      "path": "/path/to/data/avro/input"
    },
    "output": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode": {
        "type": "pure-kafka-producer",
        "brokers" : "broker-host:broker-port"
      }
    },
    "storage": {
      "type": "local"
    }
  }
}
```

- `Spark Kafka Plugin`:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "file",
      "data-type": {
        "type": "csv"
      },
      "path": "/path/to/data/csv/input"
    },
    "output": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode": {
        "type": "spark-kafka-plugin-producer",
        "brokers" : "broker-host:broker-port"
      }
    },
    "storage": {
      "type": "local"
    }
  }
}
```

## 3- Kafka to Elasticsearch :

You can index data into Elasticsearch by triggering one of these 3 modes : 
2 Streaming modes by :`Pure Kafka Consumer` and `Pure Kafka Streams Consumer` or 1 Batch mode by `Spark Kafka Plugin`.

- `Pure Kafka Consumer` :

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-consumer",
        "brokers" : "broker-host:broker-port",
        "consumer-group" : "your-consumer-group-name",
        "offset" : {
          "type": "earliest"
        }
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "storage": {
      "type": "local"
    }
  }
}
```

- `Pure Kafka Streams Consumer` :

  `POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "pure-kafka-streams-consumer",
        "brokers" : "broker-host:broker-port",
        "stream-app-id" : "your-stream-app-id",
        "offset" : {
          "type": "earliest"
        }
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "storage": {
      "type": "local"
    }
  }
}
```

- `Spark Kafka Plugin`:

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
      "type": "kafka",
      "topic": "your-kafka-topic",
      "kafka-mode" : {
        "type" : "spark-kafka-plugin-consumer",
        "brokers" : "broker-host:broker-port",
        "offset" : {
          "type": "earliest"
        }
      }
    },
    "output": {
      "type": "cassandra",
      "keyspace": "your-keyspace",
      "table": "your-table"
    },
    "storage": {
      "type": "local"
    }
  }
}
```

## 4- Elasticsearch to Cassandra :

`POST http://localhost:5555/conversion/route`
```json
{
  "route": {
    "input": {
        "type": "elasticsearch",
        "index": "your-index",
        "bulk-enabled": false,
        "search-query": {
            "type": "ids-query",
            "ids": ["wcOPfnsBllGEOdBS7-HB", "zMOPfnsBllGEOdBS8OGr", "zsOPfnsBllGEOdBS8OHM"]
        }
    },
    "output": {
        "type": "cassandra",
        "keyspace": "your-keyspace",
        "table": "your-table"
    },
    "storage": {
        "type": "local"
    }
  }
}
```
The `search-query` input property section can support 16 types of queries, like :

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
You can find all these Elasticsearch `search-queries` here [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman)

## 5- Elasticsearch operations :

a- "index-creation" :

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

b- "index-mapping" :

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

c- "index-deletion" :

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

## 6- and many more :

You can send cassandra-to-cassandra, kafka-to-kafka or elasticsearch-to-elasticsearch... 
Import these POSTMAN [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/postman) to get an idea.

# C- Scheduling :

Under the `data-highway/airflow/dags` folder, you will find some Airflow DAG samples, that can help you to automate your data-highway application with Airflow. 
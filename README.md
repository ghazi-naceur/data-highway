Yet another data converter.

You can convert your data to multiple data types :

* JSON Conversion (**csv-to-json**,  **parquet-to-json** and **avro-to-json**)
* Parquet Conversion (**csv-to-parquet**,  **json-to-parquet**, **avro-to-parquet**)
* CSV Conversion (**json-to-csv**,  **parquet-to-csv**, **avro-to-csv** and **xlsx-to-csv**)
* Avro Conversion (**csv-to-avro**,  **parquet-to-avro** and **json-to-avro**)

You can as well :  
* Send data to Kafka (**file-to-kafka** and **kafka-to-kafka**)
* Consume data from Kafka (**kafka-to-file**)
* Index data in Elasticsearch (**file-to-elasticsearch**)
* Extract data from Elasticsearch (**elasticsearch-to-file**)

**Environment :**

- Docker 20.10.2
- Docker compose 1.25.0
- JDK 1.8
- Scala 2.12.12
- Spark 2.4.6
- Elasticsearch 7.10.2

## Table of contents :
* [A- Getting started](#A--getting-started-)
    * [1- Dataflow](#1--dataflow-)
    * [2- Run data-highway jar ](#2--run-data-highway-jar-)
    * [3- Run data-highway Docker Image ](#3--run-data-highway-docker-image-)
* [B- Routes](#B--routes-)
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
        * [d- From XLSX to CSV](#d--from-xlsx-to-csv-)
    * [4- Avro conversion](#4--avro-conversion-)
        * [a- From Parquet to Avro](#a--from-parquet-to-avro-)
        * [b- From Json to Avro](#b--from-json-to-avro-)
        * [c- From Csv to Avro](#c--from-csv-to-avro-)
    * [5- Send data to Kafka](#5--send-data-to-kafka-)
        * [a- Pure Kafka Producer](#a--pure-kafka-producer-)
        * [b- Spark Kafka Plugin Producer](#b--spark-kafka-plugin-producer-)
    * [6- Consume data from Kafka](#6--consume-data-from-kafka-)
        * [a- Pure Kafka Consumer](#a--pure-kafka-consumer-)
        * [b- Spark Kafka Plugin Consumer](#b--spark-kafka-plugin-consumer-)
    * [7- Index data in Elasticsearch](#7--index-data-in-elasticsearch-)
        * [a- File to Elasticsearch](#a--file-to-elasticsearch-)
        * [b- Elasticsearch to File](#b--elasticsearch-to-file-)
        * [c- Elasticsearch operations](#c--elasticsearch-operations-)
* [C- Scheduling](#C--scheduling-)

# A- Getting started :

## 1- Dataflow :

The data-highway conversion dataflow consists of converting data located under the **input** folder. The converted data will be generated under the **output** folder.
All **input** data will be placed under the **processed** folder :

![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/1-conversions.png?raw=true)

Publishing-data route consists of watching periodically an **input** folder containing json files, and publishing its content 
to a Kafka topic :

![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/2-publish.png?raw=true)

Consuming-data route consists of consuming periodically a Kafka topic and saving its content into json files in an **output** folder :

![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/3-consume.png?raw=true)

**data-highway** can, as well, mirror kafka topics by consuming from an input topic and publishing its content to an output topic :

![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/4-kafka-mirror.png?raw=true)

You have as well a route dedicated to indexing data in Elasticsearch :

![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/5-file_elasticsearch.png?raw=true)

You can extract data from your Elasticsearch index :

![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/6-elasticsearch_file.png?raw=true)

## 2- Run data-highway jar :

1- Download the latest **zip** release file of **data-highway** from [data-highway releases](https://github.com/ghazi-naceur/data-highway/releases)

2- Unzip your **data-highway** zip release file

3- Enter the unzipped folder 

4- Set configurations in the **application.conf** file

5- run your **data-highway** instance by executing the **start.sh** script :
```shell
chmod +x start.sh
./start.sh
```
![image](https://github.com/ghazi-naceur/data-highway/blob/master/src/main/resources/screenshots/data-highway-banner.png?raw=true "Data Highway Launch Banner")

6- Finally, launch a data conversion using a REST query. You can find some query samples in the following folder [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/src/main/resources/rest_queries_samples).

You can find, as well, some data samples to test your **data-highway** instance in the following link [data samples](https://github.com/ghazi-naceur/data-highway/tree/master/src/test/resources).

You will find further explanations about **data-highway** route configuration in the section [B- Routes](#B--routes-)

## 3- Run data-highway Docker Image :

1- Set configurations in the **/the-path-to-your-config-file/application.conf** (to be mounted in the next step)

2- After cloning this repository, specify your mounted volumes in the `docker-compose.yml` located under `data-highway/docker/rest/generic` and 
specify your data-highway version (located in the release file name) :
```yaml
  app:
    build: .
    image: data-highway-app:v1.0
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
    entrypoint: ["java", "-cp", "/app/jar/data-highway-${version}.jar", "io.oss.data.highway.IOMain", "-Dlog4j.configuration=/app/config/log4j.properties", "-Dconfig.file=/app/config/application.conf"]
```
3- Run the `start.sh` script under `data-highway/docker/rest/generic` to generate your Data Highway Docker image.

4- Run your HTTP request. You can find HTTP requests samples here : [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/src/main/resources/rest_queries_samples)

**Note :** For `in` and `out` HTTP request body fields, you need to provide the mounted volumes Docker side (right side).

Example : 

For the following mounted volumes, you need to provide in your HTTP request body : `"in": "/app/data/input"` and `"out": "/app/data/output"` 
```yaml
volumes:
      - /the-path-to-input-data-located-in-your-host-machine/:/app/data/input
      - /the-path-to-the-generated-output-in-your-host-machine/:/app/data/output
```

# B- Routes :

You can find some REST query samples in the following folder [REST queries](https://github.com/ghazi-naceur/data-highway/tree/master/src/main/resources/rest_queries_samples)

## 1- JSON conversion :

There are 3 provided conversions.

Set the conversion `route` in your REST **POST** query, by invoking the url `http://localhost:5555/conversion/route` and 
setting one of these following request bodies : 

### a- From Parquet to JSON :

```json
{
  "route": {
    "type": "parquet-to-json",
    "in": "your-input-folder-containing-parquet-files",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

### b- From CSV to JSON :

```json
{
  "route": {
    "type": "csv-to-json",
    "in": "your-input-folder-containing-csv-files",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### c- From Avro to JSON : 

```json
{
  "route": {
    "type": "avro-to-json",
    "in": "your-input-folder-containing-avro-files",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

## 2- Parquet conversion :

There are 3 provided conversions.

Set the conversion `route` in your REST **POST** query, by invoking the url `http://localhost:5555/conversion/route` and
setting one of these following request bodies :

#### a- From JSON to Parquet :

```json
{
  "route": {
    "type": "json-to-parquet",
    "in": "your-input-folder-containing-json-files",
    "out": "your-output-folder-that-will-contain-your-generated-parquet-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### b- From CSV to Parquet :

```json
{
  "route": {
    "type": "csv-to-parquet",
    "in": "your-input-folder-containing-csv-files",
    "out": "your-output-folder-that-will-contain-your-generated-parquet-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### c- From Avro to Parquet :

```json
{
  "route": {
    "type": "avro-to-parquet",
    "in": "your-input-folder-containing-avro-files",
    "out": "your-output-folder-that-will-contain-your-generated-parquet-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

## 3- CSV conversion :

There are 4 provided conversions.

Set the conversion `route` in your REST **POST** query, by invoking the url `http://localhost:5555/conversion/route` and
setting one of these following request bodies :

#### a- From JSON to CSV :

```json
{
  "route": {
    "type": "json-to-csv",
    "in": "your-input-folder-containing-json-files",
    "out": "your-output-folder-that-will-contain-your-generated-csv-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### b- From Parquet to CSV :

```json
{
  "route": {
    "type": "parquet-to-csv",
    "in": "your-input-folder-containing-parquet-files",
    "out": "your-output-folder-that-will-contain-your-generated-csv-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### c- From Avro to CSV :

```json
{
  "route": {
    "type": "avro-to-csv",
    "in": "your-input-folder-containing-avro-files",
    "out": "your-output-folder-that-will-contain-your-generated-csv-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### d- From XLSX to CSV :

It consists of converting the different sheets of an XLSX file to multiple csv files.

```json
{
  "route": {
    "type": "xlsx-to-csv",
    "in": "your-input-folder-containing-xlsx-files",
    "out": "your-output-folder-that-will-contain-your-generated-csv-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

## 4- Avro conversion :

There are 3 provided conversions.

Set the conversion `route` in your REST **POST** query, by invoking the url `http://localhost:5555/conversion/route` and
setting one of these following request bodies :

#### a- From Parquet to Avro :

```json
{
  "route": {
    "type": "parquet-to-avro",
    "in": "your-input-folder-containing-parquet-files",
    "out": "your-output-folder-that-will-contain-your-generated-avro-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### b- From Json to Avro :

```json
{
  "route": {
    "type": "json-to-avro",
    "in": "your-input-folder-containing-json-files",
    "out": "your-output-folder-that-will-contain-your-generated-avro-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

#### c- From Csv to Avro :

```json
{
  "route": {
    "type": "csv-to-avro",
    "in": "your-input-folder-containing-csv-files",
    "out": "your-output-folder-that-will-contain-your-generated-avro-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    }
  }
}
```

## 5- Send data to Kafka :

This mode consists of publishing :
    
   * json files
   * kafka topic content

Publishing data could be performed by :

   * a- Pure Kafka Producer
   * b- Spark Kafka Plugin Producer

Set the conversion `route` in your REST **POST** query, by invoking the url `http://localhost:5555/conversion/route` and
setting one of these following request bodies :

#### a- Pure Kafka Producer :

##### * File to Kafka :

Publishing data will be performed by **"pure-kafka-producer"** : 
```json
{
  "route": {
    "type": "file-to-kafka",
    "in": "your-input-folder-containing-json-files",
    "out": "your-output-kafka-topic",
    "file-system": {
      "type": "*hdfs* or *local*"
    },
    "kafka-mode": {
      "type": "pure-kafka-producer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas"
    }
  }
}
```
The key **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

##### * Kafka to Kafka :

Publishing data will be performed by **"pure-kafka-streams-producer"** :

```json
{
  "route": {
    "type": "kafka-to-kafka",
    "in": "your-input-kafka-topic",
    "out": "your-output-kafka-topic",
    "kafka-mode": {
      "type": "pure-kafka-streams-producer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas",
      "stream-app-id": "stream-app-name",
      "offset": {
        "type": "offset-to-consume-from"
      }
    }
  }
}
```
The key **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

#### b- Spark Kafka Plugin Producer :

##### * File to Kafka :

Publishing data will be performed by **"spark-kafka-plugin-producer"** :

```json
{
  "route": {
    "type": "file-to-kafka",
    "in": "your-input-folder-containing-json-files",
    "out": "your-output-kafka-topic",
    "file-system": {
      "type": "*hdfs* or *local*"
    },
    "kafka-mode": {
      "type": "spark-kafka-plugin-producer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas"
    }
  }
}
```
The key **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

##### * Kafka to Kafka :

Publishing data will be performed by **"spark-kafka-plugin-streams-producer"** :

```json
{
  "route": {
    "type": "kafka-to-kafka",
    "in": "your-input-kafka-topic",
    "out": "your-output-kafka-topic",
    "kafka-mode": {
      "type": "spark-kafka-plugin-streams-producer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas",
      "offset": {
        "type": "offset-to-consume-from"
      }
    }
  }
}
```
The key **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

## 6- Consume data from Kafka :

This mode consists of consuming an input topic and saving its content into files.

It is available using 4 types of routes :
   * a- Pure Kafka Consumer :
        - without streaming
        - with streaming
   * b- Spark Kafka Consumer Plugin :
        - without streaming
        - with streaming

#### a- Pure Kafka Consumer :

##### * Without streaming :

Consuming data will be performed by **"pure-kafka-consumer"** :

```json
{
  "route": {
    "type": "kafka-to-file",
    "in": "topic-name",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    },
    "kafka-mode": {
      "type": "pure-kafka-consumer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas",
      "consumer-group": "consumer-group-name",
      "offset": {
        "type": "offset-to-consume-from"
      },
      "data-type": {
        "type": "the-desired-datatype-of-the-generated-files"
      }
    }
  }
}
```
- **"data-type"** is an optional field. Its accepted values are json and avro (json is the default value, if not set).
  It will be set as an extension for the generated output files.
  
- **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

- **"offset"** could have one of these values **earliest** and **latest**

##### * With streaming :

Consuming data will be performed by **"pure-kafka-streams-consumer"** :

```json
{
  "route": {
    "type": "kafka-to-file",
    "in": "topic-name",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "kafka-mode": {
      "type": "pure-kafka-streams-consumer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas",
      "stream-app-id": "stream-app-name",
      "offset": {
        "type": "offset-to-consume-from"
      },
      "data-type": {
        "type": "the-desired-datatype-of-the-generated-files"
      }
    }
  }
}
```
- **"data-type"** is an optional field. Its accepted values are json and avro (json is the default value, if not set).
  It will be set as an extension for the generated output files.

- **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

- **"offset"** could have one of these values **earliest** and **latest**

#### b- Spark Kafka Plugin Consumer :

##### * Without streaming :

Consuming data will be performed by **"spark-kafka-plugin-consumer"** :

```json
{
  "route": {
    "type": "kafka-to-file",
    "in": "topic-name",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "file-system": {
      "type": "*hdfs* or *local*"
    },
    "kafka-mode": {
      "type": "spark-kafka-plugin-consumer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas",
      "offset": {
        "type": "offset-to-consume-from"
      },
      "data-type": {
        "type": "the-desired-datatype-of-the-generated-files"
      }
    }
  }
}
```
- **"data-type"** is an optional field. Its accepted values are json and avro (json is the default value, if not set).
  It will be set as an extension for the generated output files.

- **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

- **"offset"** could have one of these values **earliest** and **latest**

##### * With streaming :

Consuming data will be performed by **"spark-kafka-plugin-streams-consumer"** :

```json
{
  "route": {
    "type": "kafka-to-file",
    "in": "topic-name",
    "out": "your-output-folder-that-will-contain-your-generated-json-files",
    "kafka-mode": {
      "type": "spark-kafka-plugin-streams-consumer",
      "brokers": "your-kafka-brokers-with-its-ports-separated-with-commas",
      "offset": {
        "type": "earliest"
      },
      "data-type": {
        "type": "the-desired-datatype-of-the-generated-files"
      }
    }
  }
}
```
- **"data-type"** is an optional field. Its accepted values are json and avro (json is the default value, if not set).
  It will be set as an extension for the generated output files.

- **"brokers"** could have values like :  **"localhost:9092"** or **"10.10.12.13:9091,10.10.12.14:9092"**

- **"offset"** could have one of these values **earliest** and **latest**

## 7- Index data in Elasticsearch :

Set the `route` in your REST **POST** query, by invoking the url `http://localhost:5555/conversion/route` and
setting one of these following request bodies :

##### a- File to Elasticsearch :

Indexing data in Elasticsearch by **"file-to-elasticsearch"** :
```json
{
  "route": {
    "type": "file-to-elasticsearch",
    "in": "your-input-folder-containing-json-files",
    "out": "elasticsearch-index",
    "bulk-enabled": true/false
  }
}
```

##### b- Elasticsearch to File :

Extracting data from an Elasticsearch index by **"elasticsearch-to-file"**. You can find samples [here](https://github.com/ghazi-naceur/data-highway/tree/master/src/main/resources/rest_queries_samples/from_elasticsearch).
```json
{
  "route": {
    "type": "elasticsearch-to-file",
    "in": "elasticsearch-index",
    "out": "your-output-folder-containing-json-files",
    "search-query": {
      "type": "elasticsearch-search-query"
    }
  }
}
```

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

##### c- Elasticsearch operations :

a- "index-creation" :
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


# C- Scheduling :

Under the `data-highway/airflow/dags` folder, you will find some Airflow DAG samples, that can help you to automate your data-highway application with Airflow. 
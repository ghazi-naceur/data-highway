Yet another data converter

You can convert your data to multiple data types.

Provide the following parameters in the `application.conf` and launch the app :

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

**c- Spark Kafka Plugin** : (Experimental feature)
````hocon
route {
  type = json-to-kafka
  in = "your-input-folder-containing-json-files"
  out = "your-output-kafka-topic"
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas", // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "spark-kafka-plugin"
      use-stream = false
      intermediate-topic = "your-intermediate-topic" // Must be set once `use-stream = true`
      checkpoint-folder = "your-checkpoint-folder-related-to-the-intermediate-topic" // Must be set once `use-stream = true`. You must change its value everytime you change the `intermediate-topic`
  }
}
````

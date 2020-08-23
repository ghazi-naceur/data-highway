Yet another data converter

You can convert your data to multiple data types.

Provide the following parameters in the `application.conf` and launch the app :

**1- JSON conversion** :
___

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

**2- Parquet conversion** :
___

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

**3- CSV conversion** :
___

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

**c- From XLSX to CSV** : 

Consist of converting the different sheets of an XLSX or XLS file to multiple csv files.
````hocon
route {
  type = xlsx-to-csv
  in = "your-input-folder-containing-xlsx-files"
  out = "your-output-folder-that-will-contain-your-generated-csv-files"
}
````

**4- Send data to Kafka** :
___

This mode is available using 3 types of channel : 

**a- Simple Kafka Producer** : 
````hocon
route {
  type = json-to-kafka
  in = "your-input-folder-containing-json-files"
  out = "your-output-kafka-topic"
  broker-urls = "your-kafka-brokers-with-its-ports-separated-with-commas", // eg : "localhost:9092" or "10.10.12.13:9091,10.10.12.14:9092"
  kafka-mode = {
      type = "producer-consumer"
      use-consumer = false // This is a quick-debug feature (Experimental). You may want to leave it as `false`. It allows to launch a consumer for your producer.
      offset = "latest" // This parameter takes 3 possible values : "latest", "earliest" and "none". It will be taken into consideration once `use-consumer = true`.
      consumer-group = "your-consumer-group-name" // This is the consumer group name. It will be taken into consideration once `use-consumer = true`.
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
      use-consumer = false // This is a quick-debug feature (Experimental). You may want to leave it as `false`. It allows to launch a consumer for your producer.
      offset = "latest" // This parameter takes 3 possible values : "latest", "earliest" and "none". It will be taken into consideration once `use-consumer = true`.
      consumer-group = "your-consumer-group-name" // This is the consumer group name. It will be taken into consideration once `use-consumer = true`.
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

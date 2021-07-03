hdfs dfs -mkdir -p /data/csv_to_parquet/input
hdfs dfs -mkdir -p /data/csv_to_parquet/output
hdfs dfs -rm -r /data/csv_to_parquet/output/*
hdfs dfs -rm -r /data/csv_to_parquet/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/csv_to_parquet-data/input/* /data/csv_to_parquet/input

hdfs dfs -mkdir -p /data/json_to_parquet/input
hdfs dfs -mkdir -p /data/json_to_parquet/output
hdfs dfs -rm -r /data/json_to_parquet/output/*
hdfs dfs -rm -r /data/json_to_parquet/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/json_to_parquet-data/input/* /data/json_to_parquet/input

hdfs dfs -mkdir -p /data/avro_to_parquet/input
hdfs dfs -mkdir -p /data/avro_to_parquet/output
hdfs dfs -rm -r /data/avro_to_parquet/output/*
hdfs dfs -rm -r /data/avro_to_parquet/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/avro_to_parquet-data/input/* /data/avro_to_parquet/input

hdfs dfs -mkdir -p /data/csv_to_avro/input
hdfs dfs -mkdir -p /data/csv_to_avro/output
hdfs dfs -rm -r /data/csv_to_avro/output/*
hdfs dfs -rm -r /data/csv_to_avro/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/csv_to_avro-data/input/* /data/csv_to_avro/input
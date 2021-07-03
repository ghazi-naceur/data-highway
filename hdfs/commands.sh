hdfs dfs -rm -r /data/csv_to_parquet/output
hdfs dfs -rm -r /data/csv_to_parquet/processed
hdfs dfs -mkdir -p /data/csv_to_parquet/output
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/csv_to_parquet-data/input/* /data/csv_to_parquet/input


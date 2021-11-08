hdfs dfs -rm -r /data
hdfs dfs -mkdir -p /data/
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/data/* /data

hdfs dfs -rm -r /data/avro/input/*
hdfs dfs -rm -r /data/avro/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/data/avro/input/* /data/avro/input

hdfs dfs -rm -r /data/xlsx/input/*
hdfs dfs -rm -r /data/xlsx/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/data/xlsx/input/* /data/xlsx/input
hdfs dfs -rm -r /data
hdfs dfs -mkdir -p /data/
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/data/* /data

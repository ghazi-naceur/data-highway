##################   Parquet   ##################
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

##################   Avro   ##################

hdfs dfs -mkdir -p /data/csv_to_avro/input
hdfs dfs -mkdir -p /data/csv_to_avro/output
hdfs dfs -rm -r /data/csv_to_avro/output/*
hdfs dfs -rm -r /data/csv_to_avro/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/csv_to_avro-data/input/* /data/csv_to_avro/input

hdfs dfs -mkdir -p /data/parquet_to_avro/input
hdfs dfs -mkdir -p /data/parquet_to_avro/output
hdfs dfs -rm -r /data/parquet_to_avro/output/*
hdfs dfs -rm -r /data/parquet_to_avro/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/parquet_to_avro-data/input/* /data/parquet_to_avro/input

hdfs dfs -mkdir -p /data/json_to_avro/input
hdfs dfs -mkdir -p /data/json_to_avro/output
hdfs dfs -rm -r /data/json_to_avro/output/*
hdfs dfs -rm -r /data/json_to_avro/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/json_to_avro-data/input/* /data/json_to_avro/input

##################   Csv   ##################

hdfs dfs -mkdir -p /data/json_to_csv/input
hdfs dfs -mkdir -p /data/json_to_csv/output
hdfs dfs -rm -r /data/json_to_csv/output/*
hdfs dfs -rm -r /data/json_to_csv/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/json_to_csv-data/input/* /data/json_to_csv/input

hdfs dfs -mkdir -p /data/parquet_to_csv/input
hdfs dfs -mkdir -p /data/parquet_to_csv/output
hdfs dfs -rm -r /data/parquet_to_csv/output/*
hdfs dfs -rm -r /data/parquet_to_csv/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/parquet_to_csv-data/input/* /data/parquet_to_csv/input

hdfs dfs -mkdir -p /data/avro_to_csv/input
hdfs dfs -mkdir -p /data/avro_to_csv/output
hdfs dfs -rm -r /data/avro_to_csv/output/*
hdfs dfs -rm -r /data/avro_to_csv/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/avro_to_csv-data/input/* /data/avro_to_csv/input

hdfs dfs -mkdir -p /data/xlsx_to_csv/input
hdfs dfs -mkdir -p /data/xlsx_to_csv/output
hdfs dfs -rm -r /data/xlsx_to_csv/output/*
hdfs dfs -rm -r /data/xlsx_to_csv/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/xlsx_to_csv-data/input/* /data/xlsx_to_csv/input

##################   Json   ##################

hdfs dfs -mkdir -p /data/avro_to_json/input
hdfs dfs -mkdir -p /data/avro_to_json/output
hdfs dfs -rm -r /data/avro_to_json/output/*
hdfs dfs -rm -r /data/avro_to_json/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/avro_to_json-data/input/* /data/avro_to_json/input

hdfs dfs -mkdir -p /data/csv_to_json/input
hdfs dfs -mkdir -p /data/csv_to_json/output
hdfs dfs -rm -r /data/csv_to_json/output/*
hdfs dfs -rm -r /data/csv_to_json/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/csv_to_json-data/input/* /data/csv_to_json/input

hdfs dfs -mkdir -p /data/parquet_to_json/input
hdfs dfs -mkdir -p /data/parquet_to_json/output
hdfs dfs -rm -r /data/parquet_to_json/output/*
hdfs dfs -rm -r /data/parquet_to_json/processed
hadoop fs -copyFromLocal /home/ghazi/workspace/data-highway/src/test/resources/parquet_to_json-data/input/* /data/parquet_to_json/input

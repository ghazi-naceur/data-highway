spark-submit --class "io.oss.data.highway.App" \
  --packages org.apache.spark:spark-avro_2.12:2.4.0 \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf -Dlog4j2.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf -Dlog4j2.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" \
  --files "/home/ghazi/playgroud/data-highway/shell/application.conf,/home/ghazi/playgroud/data-highway/shell/log4j2.properties" \
  /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar
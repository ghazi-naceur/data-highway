spark-submit --class "io.oss.data.highway.App" \
  --packages org.apache.spark:spark-avro_2.12:2.4.0 \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf -Dlogback.configurationFile=/home/ghazi/playgroud/data-highway/shell/logback.xml" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf -Dlogback.configurationFile=/home/ghazi/playgroud/data-highway/shell/logback.xml" \
  --files "/home/ghazi/playgroud/data-highway/shell/application.conf,/home/ghazi/playgroud/data-highway/shell/logback.xml" \
  /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar
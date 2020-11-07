spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/application/data-highway/docker/application.conf" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/application/data-highway/docker/application.conf" \
      --files "/application/data-highway/docker/application.conf" \
      /application/data-highway/target/scala-2.12/data-highway-assembly-0.1.jar
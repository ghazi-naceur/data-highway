# Delete all containers
docker rm -f $(docker ps -aq)
# Delete all images
docker rmi -f $(docker images -a -q)
docker-compose up -d
docker build -t data-highway-spark:v1.0 .
docker run -tid \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/input/:/app/data/input \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/output/:/app/data/output \
  --name bungee-gum-spark data-highway-spark:v1.0
docker ps -a
docker images
docker exec -ti bungee-gum-spark spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties" \
      --files "/app/config/application.conf,/app/config/log4j2.properties" \
      /app/jar/data-highway-assembly-0.1.jar
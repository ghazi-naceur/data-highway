# Delete all containers
docker rm -f $(docker ps -aq)
# Delete all images
docker rmi -f $(docker images -a -q)
docker build -t data-highway:v1.0 .
#docker run -tid --name bungee-gum data-highway:v1.0
docker run -tid \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/input/:/app/data/input \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/output/:/app/data/output \
  --name bungee-gum data-highway:v1.0
docker ps -a
docker images
docker exec -ti bungee-gum spark-submit  \
      --packages org.apache.spark:spark-avro_2.12:2.4.0 \
      --class "io.oss.data.highway.App" --master local[*] \
      --conf "spark.driver.extraJavaOptions=-Dconfig.file=/app/config/application.conf" \
      --conf "spark.executor.extraJavaOptions=-Dconfig.file=/app/config/application.conf" \
      --files "/app/config/application.conf" \
      /app/jar/data-highway-assembly-0.1.jar
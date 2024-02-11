# Delete all containers
docker rm -f $(docker ps -aq)
# Delete all images
docker rmi -f $(docker images -a -q)
docker build -t data-highway-kafka:v1.0 .
docker run -tid \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/input/:/app/data/input \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/output/:/app/data/output \
  --name kafka_app_1 data-highway-kafka:v1.0
docker ps -a
docker images
docker exec -ti kafka_app_1 java -jar -Dconfig.file=/app/config/application.conf \
     -Dlogback.configurationFile=/app/config/logback.xml \
     /app/jar/data-highway-assembly-0.1.jar

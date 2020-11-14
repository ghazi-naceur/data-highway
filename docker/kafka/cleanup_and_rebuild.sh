# Delete all containers
docker rm -f $(docker ps -aq)
# Delete all images
docker rmi -f $(docker images -a -q)
docker build -t data-highway-kafka:v1.0 .
##docker run -tid --name bungee-gum data-highway:v1.0
docker run -tid \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/input/:/app/data/input \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/output/:/app/data/output \
  --name bungee-gum-kafka data-highway-kafka:v1.0
docker ps -a
docker images
docker exec -ti bungee-gum-kafka java -jar -Dconfig.file=/home/ghazi/playgroud/data-highway/shell/kafka-conf/application.conf \
    target/scala-2.12/data-highway-assembly-0.1.jar

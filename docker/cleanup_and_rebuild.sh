docker rm -f $(docker ps -aq)
docker rmi -f $(docker images -a -q)
docker build -t data-highway:v1.0 .
#docker run -tid --name bungee-gum data-highway:v1.0
docker run -tid \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/input/:/input \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/output/:/output \
  --name bungee-gum-2 data-highway:v1.0
docker ps -a
docker images
#docker exec -ti bungee-gum sh
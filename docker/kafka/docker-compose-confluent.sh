cd /tmp
git clone https://github.com/confluentinc/cp-all-in-one.git
git checkout 6.0.0-post
cd cp-all-in-one/cp-all-in-one-community
docker-compose up -d
docker-compose ps
cd /home/ghazi/workspace/data-highway/docker/kafka
docker build -t data-highway-kafka:v1.0 .
docker run -tid \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/input/:/app/data/input \
  -v /home/ghazi/playgroud/data-highway/shell/csv_to_parquet-data/output/:/app/data/output \
  --name bungee-gum-kafka data-highway-kafka:v1.0

docker exec -ti bungee-gum-kafka java -jar -Dconfig.file=/app/config/application.conf  \
  -Dlog4j2.configuration=/app/config/log4j2.properties \
  /app/jar/data-highway-assembly-0.1.jar

# View a list of all Docker container IDs :
docker container ls -a -q

# Run the following command to stop the Docker containers for Confluent :
#docker container stop $(docker container ls -a -q -f "label=io.confluent.docker")

# After stopping the Docker containers, run the following commands to prune the Docker system. Running these commands
# deletes containers, networks, volumes, and images, freeing up disk space :
#docker system prune -a -f --volumes
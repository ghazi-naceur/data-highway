cd /tmp
git clone https://github.com/confluentinc/cp-all-in-one.git
git checkout 6.0.0-post
cd cp-all-in-one/cp-all-in-one-community
docker-compose up -d
docker-compose ps

# View a list of all Docker container IDs :
docker container ls -a -q

# Run the following command to stop the Docker containers for Confluent :
docker container stop $(docker container ls -a -q -f "label=io.confluent.docker")

# After stopping the Docker containers, run the following commands to prune the Docker system. Running these commands
# deletes containers, networks, volumes, and images, freeing up disk space :
docker system prune -a -f --volumes
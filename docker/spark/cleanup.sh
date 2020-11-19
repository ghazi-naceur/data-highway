# Delete all containers
docker rm -f $(docker ps -aq)
# Delete all images
docker rmi -f $(docker images -a -q)
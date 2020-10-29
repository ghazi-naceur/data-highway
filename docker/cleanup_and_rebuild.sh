docker rm -f $(docker ps -aq)
docker rmi -f $(docker images -a -q)
docker build -t data-highway:v1.0 .
docker ps -a
docker images
docker-compose run --service-ports app java -jar -Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties /app/jar/data-highway-assembly-0.1.jar
docker-compose run --service-ports app java -cp /app/jar/data-highway-assembly-0.1.jar io.oss.data.highway.IOMain -Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties
docker-compose run  -p 5555:5555 app java -cp /app/jar/data-highway-assembly-0.1.jar io.oss.data.highway.IOMain -Dconfig.file=/app/config/application.conf -Dlog4j2.configuration=/app/config/log4j2.properties
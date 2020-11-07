start-master.sh
start-slave.sh spark://ghazi-pc:7077
spark-submit --class "io.oss.data.highway.App" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf" --files "/home/ghazi/playgroud/data-highway/shell/application.conf" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar
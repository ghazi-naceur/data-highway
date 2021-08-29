# Importing modules
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data-highway-sample',
    default_args=default_args,
    description='Data Highway Airflow DAG',
    schedule_interval=timedelta(days=1)
)

parquet_to_csv_job = BashOperator(
    task_id='parquet_to_csv_job',
    bash_command='spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.0 --class "gn.oss.data.highway.IOMain" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf1-spark/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf1-spark/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --files "/home/ghazi/playgroud/data-highway/shell/confs/conf1-spark/application.conf,/home/ghazi/playgroud/data-highway/shell/log4j2.properties" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar',
    dag=dag
)

csv_to_json_job = BashOperator(
    task_id='csv_to_json_job',
    bash_command='spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.0 --class "gn.oss.data.highway.IOMain" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf2-spark/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf2-spark/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --files "/home/ghazi/playgroud/data-highway/shell/confs/conf2-spark/application.conf,/home/ghazi/playgroud/data-highway/shell/log4j2.properties" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar',
    dag=dag
)

json_to_kafka_job = BashOperator(
    task_id='json_to_kafka_job',
    bash_command='spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.0 --class "gn.oss.data.highway.IOMain" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf4-kafka/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf4-kafka/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --files "/home/ghazi/playgroud/data-highway/shell/confs/conf4-kafka/application.conf,/home/ghazi/playgroud/data-highway/shell/log4j2.properties" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar',
    dag=dag
)

kafka_to_file_job = BashOperator(
    task_id='kafka_to_file_job',
    bash_command='spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.0 --class "gn.oss.data.highway.IOMain" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf5-kafka/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/confs/conf5-kafka/application.conf -Dlog4j.configuration=/home/ghazi/playgroud/data-highway/shell/log4j2.properties" --files "/home/ghazi/playgroud/data-highway/shell/confs/conf5-kafka/application.conf,/home/ghazi/playgroud/data-highway/shell/log4j2.properties" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar',
    dag=dag
)

parquet_to_csv_job >> csv_to_json_job >> json_to_kafka_job >> kafka_to_file_job
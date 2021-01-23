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
    'data-highway-job',
    default_args=default_args,
    description='Data Highway Airflow DAG',
    schedule_interval=timedelta(days=1)
)

data_highway_task = BashOperator(
    task_id='data-highway-task',
    bash_command='spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.0 --class "io.oss.data.highway.App" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf" --files "/home/ghazi/playgroud/data-highway/shell/application.conf,/home/ghazi/playgroud/data-highway/shell/log4j2.properties" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar',
    dag=dag
)

data_highway_task
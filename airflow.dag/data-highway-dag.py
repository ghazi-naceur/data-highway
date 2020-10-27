# Importing modules
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(2020, 10, 30),
    'depends_on_past': False,
    'email': ['example@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG
dag = DAG(
    'data-highway-job',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='data-highway-task',
    bash_command='spark-submit --class "io.oss.data.highway.App" --master local[*] --conf "spark.driver.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf" --conf "spark.executor.extraJavaOptions=-Dconfig.file=/home/ghazi/playgroud/data-highway/shell/application.conf" --files "/home/ghazi/playgroud/data-highway/shell/application.conf" /home/ghazi/playgroud/data-highway/shell/data-highway-assembly-0.1.jar',
    dag=dag
)

t1 >> t2
import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'end_date': airflow.utils.dates.days_ago(-100),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting # at least 5 minutes #
    'retries': 1, 'retry_delay': timedelta(minutes=5)
}

dag_spark = DAG(
    dag_id="data_aggregate_example_1",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='use case of spark in airflow',
    start_date=airflow.utils.dates.days_ago(1)
)

spark_submit_remote = SparkSubmitOperator(
    application='/opt/map_reduce/sale_average_price.py',
    conn_id='spark_default',
    task_id='spark_submit_remote',
    verbose=False,
    dag=dag_spark
)

sparkSubmit = '/opt/spark-3.5.1/bin/spark-submit'

# task to compute spark
spark_submit_local = BashOperator(
    task_id='spark_submit_local',
    bash_command=sparkSubmit + ' ' + '/opt/map_reduce/sale_average_price.py ',
    dag=dag_spark
)


if __name__ == "__main__":
    dag_spark.cli()

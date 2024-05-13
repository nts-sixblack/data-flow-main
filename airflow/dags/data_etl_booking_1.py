import os
import subprocess

import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Execute the 'hadoop classpath --glob' command
classpath = subprocess.check_output("hadoop classpath --glob", shell=True).decode('utf-8').strip()
# Set the CLASSPATH environment variable
os.environ['CLASSPATH'] = classpath


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
    dag_id="data_etl_booking_1",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='use case of spark in airflow',
    start_date=airflow.utils.dates.days_ago(1)
)

spark_etl_sale = SparkSubmitOperator(
    application='/opt/map_reduce/booking_analyze_sale.py',
    conn_id='spark_default',
    task_id='spark_etl_sale',
    verbose=False,
    dag=dag_spark
)

spark_etl_driver = SparkSubmitOperator(
    application='/opt/map_reduce/booking_analyze_driver.py',
    conn_id='spark_default',
    task_id='spark_etl_driver',
    verbose=False,
    dag=dag_spark
)

spark_etl_sale >> spark_etl_driver


if __name__ == "__main__":
    dag_spark.cli()

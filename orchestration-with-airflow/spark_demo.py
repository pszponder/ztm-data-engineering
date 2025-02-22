from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'spark_demo',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_job',
    application='simple_spark_script.py',
    name='spark_job_demo',
    conn_id='spark_default',
    conf={'master': 'local[*]'},
    dag=dag
)
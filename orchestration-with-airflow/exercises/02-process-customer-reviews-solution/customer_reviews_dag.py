from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import csv

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False,
    description="Review average score",
)
def guest_reviews_minutely_etl():

    @task
    def extract_reviews():
        pg_hook = PostgresHook(postgres_conn_id="postgres_rental_site")

        query = """
            SELECT review_id, listing_id, guest_id, review_score, review_comment, review_date
            FROM guest_reviews
            WHERE review_date >= '{{ prev_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}'
              AND review_date < '{{ ds }}'
        """

        records = pg_hook.get_records(query)
        column_names = ["review_id", "listing_id", "review_score", "review_comment", "review_date"]

        file_date = "{{ execution_date.strftime('%Y%m%d_%H%M') }}"
        file_path = f"/tmp/data/guest_reviews/{file_date}/guest_reviews.csv"

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)
            writer.writerows(records)

        print(f"Extracted guest reviews written to {file_path}")

    extract_task = extract_reviews()

    spark_etl = SparkSubmitOperator(
        task_id="spark_etl_reviews",
        application="spark_etl_reviews.py",
        name="guest_reviews_etl",
        application_args=[
            "--input_file", "/tmp/data/guest_reviews/{{ execution_date.strftime('%Y%m%d_%H%M') }}/guest_reviews.csv",
            "--output_file", "/tmp/data/avg_review_score_by_listing/{{ execution_date.strftime('%Y%m%d_%H%M') }}/avg_review_score_by_listing.csv"
        ],
        conn_id='spark_rental_site',
        conf={'master': 'local[*]'},
    )

    extract_task >> spark_etl

dag_instance = guest_reviews_minutely_etl()
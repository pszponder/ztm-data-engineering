from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import json
import random

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description="",
)
def bookings_spark_pipeline():

    @task
    def generate_bookings():
        context = get_current_context()
        execution_date = context["execution_date"]

        file_date = execution_date.strftime("%Y-%m-%d_%H")
        file_path = f"/tmp/data/bookings/{file_date}/bookings.json"

        num_bookings = random.randint(30, 50)
        bookings = []
        for i in range(num_bookings):
            booking = {
                "booking_id": random.randint(1000, 5000),
                "listing_id": random.choice([13913, 17402, 24328, 33332, 116268, 117203, 127652, 127860]),
                "user_id": random.randint(1000, 5000),
                "status": random.choice(["confirmed", "cancelled", "pending"])
            }
            bookings.append(booking)

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as f:
            json.dump(bookings, f)

        print(f"Generated bookings data written to {file_path}")

        return file_path

    spark_job = SparkSubmitOperator(
        task_id="process_airbnb_and_bookings",
        application="bookings_per_listing_spark.py",
        name="airbnb_listings_bookings_join",
        application_args=[
            "--listings_file", "/tmp/data/inside_airbnb/{{ execution_date.strftime('%Y-%m') }}/listings.csv.gz",
            "--bookings_file", "/tmp/data/bookings/{{ execution_date.strftime('%Y-%m-%d_%H') }}/bookings.json",
            "--output_path", "/tmp/data/bookings_per_listing/{{ execution_date.strftime('%Y-%m-%d_%H') }}"
        ],
        conn_id='spark_default',
        conf={'master': 'local[*]'},
    )

    bookings_file = generate_bookings()
    bookings_file >> spark_job

dag_instance = bookings_spark_pipeline()
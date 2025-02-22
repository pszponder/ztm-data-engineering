from datetime import datetime
import os
import json
import random

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

@dag(
    'average_page_visits_2',
    start_date=datetime(2023, 1, 1),
    schedule_interval='* * * * *',
    catchup=False,
    description=""
)
def average_page_visits_2():

    def get_data_path():
        context = get_current_context()
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/page_visits/{file_date}.json"

    @task
    def produce_airbnb_data():

        if random.random() < 0.2:
            raise Exception("Job has failed")

        page_visits = [
            {"id": 1, "name": "Cozy Apartment", "price": 120, "page_visits": random.randint(0, 50)},
            {"id": 2, "name": "Luxury Condo", "price": 300, "page_visits": random.randint(0, 50)},
            {"id": 3, "name": "Modern Studio", "price": 180, "page_visits": random.randint(0, 50)},
            {"id": 4, "name": "Charming Loft", "price": 150, "page_visits": random.randint(0, 50)},
            {"id": 5, "name": "Spacious Villa", "price": 400, "page_visits": random.randint(0, 50)},
        ]
        file_path = get_data_path()

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as f:
            json.dump(page_visits, f)

        print(f"Written to file: {file_path}")

    @task
    def process_airbnb_data():
        file_path = get_data_path()

        with open(file_path, "r") as f:
            page_visits = json.load(f)

        average_price = sum(page_visit["page_visits"] for page_visit in page_visits) / len(page_visits)
        print(f"Average number of page visits {average_price}")

    produce_airbnb_data() >> process_airbnb_data()

demo_dag = average_page_visits_2()
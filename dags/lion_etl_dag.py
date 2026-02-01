from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import csv


def extract(ti):
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)
    data = response.json()

    ti.xcom_push(key="raw_data", value=data)


def transform(ti):
    raw_data = ti.xcom_pull(key="raw_data", task_ids="extract_task")

    cleaned_data = []

    for item in raw_data:
        cleaned_data.append({
            "post_id": item["id"],
            "title": item["title"].upper(),
            "user_id": item["userId"]
        })

    ti.xcom_push(key="clean_data", value=cleaned_data)


def load(ti):
    clean_data = ti.xcom_pull(key="clean_data", task_ids="transform_task")

    file_path = "/tmp/lion_parcel_output.csv"

    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["post_id", "title", "user_id"])
        writer.writeheader()
        writer.writerows(clean_data)

    print(f"Data saved to {file_path}")


with DAG(
    dag_id="lion_parcel_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    task_transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    task_load = PythonOperator(
        task_id="load_task",
        python_callable=load
    )

    task_extract >> task_transform >> task_load

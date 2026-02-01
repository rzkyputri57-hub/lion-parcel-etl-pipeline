from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract():
    print("Extracting data...")


def transform():
    print("Transforming data...")


def load():
    print("Loading data...")


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

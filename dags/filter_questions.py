from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import csv
import json
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

def filter_math_questions():
    input_path_csv = os.path.join("/opt/airflow/dags/data", "questions.csv")
    output_path = os.path.join("/opt/airflow/dags/data", "questions_math.csv")

    with open(input_path_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)          
        questions = list(reader)
    
    filter_func = lambda q: q['discipline'] in ['matematica']

    filtered = list(filter(filter_func, questions))

    if filtered:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=filtered[0].keys())  # colunas do CSV original
            writer.writeheader()
            writer.writerows(filtered)


# DAG para filtrar as questões de matemática e física
with DAG(
    "dag_filter_questions",
    default_args=default_args,
    description="Pipeline para filtrar as questões de matemática e física da base de dados questions.csv e json",
    schedule=None,
    catchup=False,
) as dag:
    # Task 1: Acessar base de dados
    task_access_data = BashOperator(
        task_id="access_data",
        bash_command=(
            "ls -l /opt/airflow/dags/data/questions.csv && "
            "ls -l /opt/airflow/dags/data/questions.json"
        )
    )

    # Task 2: Filtra as questões de matemática e física
    task_filter_questions = PythonOperator(
        task_id="filter_questions",
        python_callable=filter_math_questions
    )

    task_access_data >> task_filter_questions
    # Task 3: Salva as questões filtradas em um novo arquivo CSV e JSON
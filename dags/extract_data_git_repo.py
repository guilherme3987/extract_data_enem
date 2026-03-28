from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from config import BASE_PATH, REPO_URL
from extract_questions import extract_all_questions

# Argumentos default para a DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

with DAG(
    "dag_enem_extraction",
    default_args=default_args,
    description="Pipeline para clonar o repositório ENEM e extrair todas as questões",
    schedule=None,
    catchup=False,
) as dag:

    #  Task 1: clone do repositório 
    # Remove a pasta inteira antes de clonar para evitar conflitos
    task_clone = BashOperator(
        task_id="clone_repository",
        bash_command=(
            f"rm -rf {BASE_PATH} && "
            f"git clone {REPO_URL} {BASE_PATH}"
        ),
    )

    #  Task 2: extração de todas as questões 
    # Lê todos os details.json de todos os anos e salva questions.json + questions.csv
    task_extract = PythonOperator(
        task_id="extract_all_questions",
        python_callable=extract_all_questions,
    )

    # Ordem das tasks
    task_clone >> task_extract
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import csv
import os
from dotenv import load_dotenv
import google.generativeai as genai
import pandas as pd
import json
import time

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from typing_extensions import TypedDict

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

# 1. Definimos o Schema (Estrutura) exato que queremos que o Gemini retorne
class PhysicsResponse(TypedDict):
    physics_ids: list[str]

# 2. Isolamos a chamada da API com o decorador do Tenacity
# Ele vai tentar até 5 vezes. Se falhar, espera 15s, depois 30s, depois 60s (máximo).
@retry(
    wait=wait_exponential(multiplier=2, min=15, max=60), 
    stop=stop_after_attempt(5),
    reraise=True # Se falhar todas as vezes, levanta o erro para o Airflow falhar a Task
)
def call_gemini_with_retry(model, prompt):
    print("Enviando requisição para a API do Gemini...")
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            response_mime_type="application/json",
            response_schema=PhysicsResponse, # Força o modelo a seguir a estrutura do TypedDict
            temperature=0.0 # Temperatura 0 é ideal para tarefas de classificação exata
        )
    )
    return response

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
            writer = csv.DictWriter(f, fieldnames=filtered[0].keys())
            writer.writeheader()
            writer.writerows(filtered)

def filter_physics_questions():
    if not GEMINI_API_KEY:
        raise ValueError("Chave da API do Gemini não encontrada.")
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash')    
    
    input_path_csv = "/opt/airflow/dags/data/questions.csv"
    output_path = "/opt/airflow/dags/data/questions_physics.csv"

    with open(input_path_csv, 'r', encoding='utf-8') as f:
        questions = list(csv.DictReader(f))

    nature_questions = [q for q in questions if q['discipline'] == 'ciencias-natureza']
    
    physics_ids = []
    batch_size = 15 

    for i in range(0, len(nature_questions), batch_size):
        batch = nature_questions[i:i + batch_size]
        batch_text = ""
        for q in batch:
            txt = str(q.get('context', ''))[:400].replace('\n', ' ')
            batch_text += f"ID:{q['index']}|Questao:{txt}\n"

        # Prompt super enxuto para economizar tokens de entrada
        prompt = f"Analise as questões e identifique APENAS os IDs das questões de FÍSICA.\n\n{batch_text}"

        try:
            # A chamada agora passa pelo Tenacity. Se der erro 429, ele gerencia a pausa automaticamente.
            response = call_gemini_with_retry(model, prompt)
            
            # Como usamos Structured Outputs, a resposta já é uma string de um JSON puro e perfeito
            data = json.loads(response.text)
            
            new_ids = [str(item) for item in data.get("physics_ids", [])]
            physics_ids.extend(new_ids)
            
            print(f"Lote {i} processado com sucesso. IDs: {new_ids}")
            
            # Pausa de segurança padrão entre os lotes para não estourar os limites
            time.sleep(6) 

        except Exception as e:
            # Se cair aqui, é porque o Tenacity tentou 5 vezes, esperou os tempos máximos, e ainda assim falhou (provavelmente a cota diária acabou mesmo).
            print(f"Falha CRÍTICA ao processar o lote {i} após todas as tentativas. Erro: {e}")
            raise # Opcional: levanta o erro para o Airflow marcar a Task como FAILED e não gerar um arquivo pela metade.

    # Filtro final
    final_physics = [
        q for q in nature_questions 
        if str(q.get('index', '')).strip() in physics_ids
    ]

    if final_physics:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=questions[0].keys())
            writer.writeheader()
            writer.writerows(final_physics)
        print(f"Sucesso! {len(final_physics)} questões salvas em {output_path}")
    else:
        print("Atenção: A lista de física está vazia.")

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
    task_filter_questions_math = PythonOperator(
        task_id="task_filter_questions_math",
        python_callable=filter_math_questions
    )

    # Task 3: Filtra as questões de física usando a API do Gemini
    task_filter_questions_physics = PythonOperator(
        task_id="task_filter_questions_physics",
        python_callable=filter_physics_questions
    )

    task_access_data >> [task_filter_questions_math, task_filter_questions_physics]
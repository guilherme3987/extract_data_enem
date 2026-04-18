from airflow import DAG
from airflow.operators.python import PythonOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
import re
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

# ==========================================
# Configurações de Diretórios
# ==========================================
DATA_DIR = "/opt/airflow/data"
INPUT_CSV_PATH = "dags/data/questions_math.csv"
TEMP_CSV_PATH = "dags/data/questions_temp.csv"
FINAL_CSV_PATH = "dags/data/questions_final.csv"
IMAGES_DIR = "dags/data/images" 

# ==========================================
# Funções dos PythonOperators
# ==========================================

def clear_previous_runs():
    """Task 0: Limpa arquivos temporários de execuções anteriores falhas"""
    if os.path.exists(TEMP_CSV_PATH):
        os.remove(TEMP_CSV_PATH)
    print("Ambiente limpo para nova execução.")

def read_database():
    """Task 1: Acessando a base de dados"""
    if not os.path.exists(INPUT_CSV_PATH):
        raise FileNotFoundError(f"Arquivo não encontrado no caminho: {INPUT_CSV_PATH}")

    df = pd.read_csv(INPUT_CSV_PATH)
    print(f"Base carregada com sucesso. Total de registros iniciais: {len(df)}")
    df.to_csv(TEMP_CSV_PATH, index=False)

def check_images_available():
    """Task 2: Extraindo URLs e checando disponibilidade via HTTP HEAD em paralelo"""
    df = pd.read_csv(TEMP_CSV_PATH)

    def extract_url(text):
        if pd.isna(text):
            return None
        urls = re.findall(r'!\[.*?\]\((.*?)\)', str(text))
        return urls[0] if urls else None

    def get_image_status(url):
        if not url:
            return None
        try:
            response = requests.head(url, timeout=10)
            return response.status_code
        except requests.exceptions.RequestException as e:
            return 404 

    df['image_url'] = df['context'].apply(extract_url)
    urls = df['image_url'].tolist()
    status_map = {}

    print(f"Iniciando verificação de status das URLs ({len(urls)} registros)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_image_status, url): i for i, url in enumerate(urls)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                status_map[idx] = future.result()
            except Exception:
                status_map[idx] = 404

    df['image_status'] = [status_map[i] for i in range(len(urls))]
    df.to_csv(TEMP_CSV_PATH, index=False)
    print("Checagem de imagens concluída.")

def check_missing_fields():
    """Task 3: Checando se os campos obrigatórios estão preenchidos"""
    df = pd.read_csv(TEMP_CSV_PATH)

    required_cols = [
        'context', 'alternatives_intro',
        'alternative_A', 'alternative_B', 'alternative_C',
        'alternative_D', 'alternative_E',
        'correct_alternative', 'has_images'
    ]

    df_filtered = df.dropna(subset=required_cols).copy()
    print(f"Registros mantidos após remoção de campos nulos: {len(df_filtered)}")
    df_filtered.to_csv(TEMP_CSV_PATH, index=False)

def download_images():
    """Task 4: Baixando apenas imagens com status 200 (OK) em paralelo"""
    df = pd.read_csv(TEMP_CSV_PATH)
    os.makedirs(IMAGES_DIR, exist_ok=True)

    def download_single(args):
        idx, row = args
        if not (row['has_images'] and row.get('image_status') == 200):
            return idx, None

        img_url = row['image_url']
        img_name = img_url.split("/")[-1]
        img_path = os.path.join(IMAGES_DIR, img_name)

        try:
            if not os.path.exists(img_path):
                response = requests.get(img_url, timeout=15)
                if response.status_code == 200:
                    with open(img_path, 'wb') as f:
                        f.write(response.content)
                    return idx, img_path
                else:
                    return idx, None
            else:
                return idx, img_path
        except Exception:
            return idx, None

    rows = list(df.iterrows())
    path_map = {}

    print(f"Iniciando download de imagens ({len(rows)} registros)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_single, row): row[0] for row in rows}
        for future in as_completed(futures):
            try:
                idx, path = future.result()
                path_map[idx] = path
            except Exception:
                pass

    df['image_path'] = [path_map.get(i) for i in df.index]
    df.to_csv(TEMP_CSV_PATH, index=False)
    print("Download de imagens finalizado.")

def save_final_csv():
    """Task 5: Salvando CSV final, limpando o markdown e reordenando IDs"""
    df = pd.read_csv(TEMP_CSV_PATH)

    # Elimina questões com imagem marcadas como True mas sem caminho de imagem salvo
    df_final = df[~((df['has_images'] == True) & (df['image_path'].isnull()))].copy()

    # Remove colunas auxiliares
    cols_to_drop = [col for col in ['image_url', 'image_status'] if col in df_final.columns]
    df_final.drop(columns=cols_to_drop, inplace=True)

    # ==========================================
    # NOVO: Limpeza da coluna de Contexto
    # ==========================================
    # O regex r'!\[.*?\]\(.*?\)' encontra e apaga qualquer marcação de imagem markdown (ex: ![](https://...))
    if 'context' in df_final.columns:
        df_final['context'] = df_final['context'].astype(str).apply(
            lambda text: re.sub(r'!\[.*?\]\(.*?\)', '', text).strip()
        )

    # Como você deletou várias linhas corrompidas, os IDs ficaram pulando (ex: 1, 4, 5, 9).
    # Isso refaz a contagem numérica da coluna 'index' perfeitamente de 1 em diante:
    if 'index' in df_final.columns:
        df_final['index'] = range(1, len(df_final) + 1)

    df_final.to_csv(FINAL_CSV_PATH, index=False)

    # Limpeza do arquivo temporário
    if os.path.exists(TEMP_CSV_PATH):
        os.remove(TEMP_CSV_PATH)

    print(f"Pipeline concluído! Textos limpos e CSV Final salvo em: {FINAL_CSV_PATH}")

# ==========================================
# Definição da DAG
# ==========================================

with DAG(
    "dag_enem_filter",
    default_args=default_args,
    description="Pipeline para filtrar questões de matemática do ENEM",
    schedule=None,
    catchup=False,
) as dag:

    t0 = PythonOperator(
        task_id="task0_preparacao_ambiente",
        python_callable=clear_previous_runs
    )

    t1 = PythonOperator(
        task_id="task1_acessando_base_de_dados",
        python_callable=read_database
    )

    t2 = PythonOperator(
        task_id="task2_checando_imagens_disponiveis",
        python_callable=check_images_available
    )

    t3 = PythonOperator(
        task_id="task3_checando_campos_preenchidos",
        python_callable=check_missing_fields
    )

    t4 = PythonOperator(
        task_id="task4_baixando_imagens_validas",
        python_callable=download_images
    )

    t5 = PythonOperator(
        task_id="task5_salvando_csv_final",
        python_callable=save_final_csv
    )

    # Ordem de execução corrigida
    t0 >> t1 >> t2 >> t3 >> t4 >> t5
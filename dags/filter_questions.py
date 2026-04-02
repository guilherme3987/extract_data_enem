from airflow import DAG
from airflow.operators.python import PythonOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
import re
import os
import csv
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 1),
}

# ==========================================
# Configurações de Diretórios
# ==========================================
DATA_DIR = "/opt/airflow/data"
INPUT_CSV_PATH = "dags/data/questions_math.csv"
TEMP_CSV_PATH = f"dags/data/questions_temp.csv"
FINAL_CSV_PATH = f"dags/data/questions_final.csv"
IMAGES_DIR = f"dags/data//images"

# ==========================================
# Funções dos PythonOperators
# ==========================================
 
def read_database():
    """Task 1: Acessando a base de dados"""
    if not os.path.exists(INPUT_CSV_PATH):
        raise FileNotFoundError(f"Arquivo não encontrado no caminho: {INPUT_CSV_PATH}")
 
    df = pd.read_csv(INPUT_CSV_PATH)
    print(f"Base carregada com sucesso. Total de registros iniciais: {len(df)}")
 
    # Salva em um arquivo temporário para ser pego pela próxima task
    df.to_csv(TEMP_CSV_PATH, index=False)
 
 
def check_images_available():
    """Task 2: Extraindo URLs e checando disponibilidade via HTTP HEAD em paralelo"""
    df = pd.read_csv(TEMP_CSV_PATH)
 
    def extract_url(text):
        if pd.isna(text):
            return None
        # Usando regex para extrair a URL de dentro da tag markdown ![](url)
        urls = re.findall(r'!\[.*?\]\((.*?)\)', str(text))
        return urls[0] if urls else None
 
    def get_image_status(url):
        if not url:
            return None
        try:
            # Faz um HEAD request (não baixa a imagem, só lê o cabeçalho)
            response = requests.head(url, timeout=10)
            return response.status_code
        except requests.exceptions.RequestException as e:
            print(f"Erro ao acessar {url}: {e}")
            return 404  # Considera como não encontrado em caso de erro de conexão
 
    # 1. Extrai as URLs para facilitar a vida da Task de download
    df['image_url'] = df['context'].apply(extract_url)
 
    # 2. Checa o status HTTP em paralelo — evita travar em requisições lentas
    urls = df['image_url'].tolist()
    status_map = {}
 
    print(f"Iniciando verificação de status das URLs ({len(urls)} registros, paralelo)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_image_status, url): i for i, url in enumerate(urls)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                status_map[idx] = future.result()
            except Exception as e:
                print(f"Erro inesperado no índice {idx}: {e}")
                status_map[idx] = 404
 
    df['image_status'] = [status_map[i] for i in range(len(urls))]
 
    df.to_csv(TEMP_CSV_PATH, index=False)
    print("Checagem de imagens concluída. URLs extraídas e status verificados.")
 
 
def check_missing_fields():
    """Task 3: Checando se os campos obrigatórios estão preenchidos"""
    df = pd.read_csv(TEMP_CSV_PATH)
 
    required_cols = [
        'context', 'alternatives_intro',
        'alternative_A', 'alternative_B', 'alternative_C',
        'alternative_D', 'alternative_E',
        'correct_alternative', 'has_images'
    ]
 
    # Filtra linhas onde TODOS esses campos estão preenchidos
    df_filtered = df.dropna(subset=required_cols).copy()
 
    print(f"Registros mantidos após remoção de campos nulos: {len(df_filtered)}")
    df_filtered.to_csv(TEMP_CSV_PATH, index=False)
 
 
def download_images():
    """Task 4: Baixando apenas imagens com status 200 (OK) em paralelo"""
    df = pd.read_csv(TEMP_CSV_PATH)
    os.makedirs(IMAGES_DIR, exist_ok=True)
 
    def download_single(args):
        """Baixa uma imagem e retorna (índice, caminho_local)"""
        idx, row = args
        if not (row['has_images'] and row.get('image_status') == 200):
            return idx, None
 
        img_url = row['image_url']
        img_name = img_url.split("/")[-1]  # Pega o final do link, como 'dcd1ac1c.jpg'
        img_path = os.path.join(IMAGES_DIR, img_name)
 
        try:
            if not os.path.exists(img_path):
                # Aqui usamos GET para efetivamente baixar o corpo da imagem
                response = requests.get(img_url, timeout=15)
                if response.status_code == 200:
                    with open(img_path, 'wb') as f:
                        f.write(response.content)
                    return idx, img_path
                else:
                    print(f"Erro HTTP {response.status_code} ao baixar {img_url}")
                    return idx, None
            else:
                return idx, img_path
        except Exception as e:
            print(f"Falha na requisição para {img_url}: {e}")
            return idx, None
 
    rows = list(df.iterrows())
    path_map = {}
 
    print(f"Iniciando download de imagens ({len(rows)} registros, paralelo)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_single, row): row[0] for row in rows}
        for future in as_completed(futures):
            try:
                idx, path = future.result()
                path_map[idx] = path
            except Exception as e:
                print(f"Erro inesperado no download: {e}")
 
    df['image_path'] = [path_map.get(i) for i in df.index]
    df.to_csv(TEMP_CSV_PATH, index=False)
    print("Download de imagens finalizado.")
 
 
def save_final_csv():
    """Task 5: Salvando CSV final com questões válidas e caminhos das imagens"""
    df = pd.read_csv(TEMP_CSV_PATH)
 
    # Como o requisito é "Todos devem estar preenchidos",
    # eliminamos questões que marcavam 'has_images' == True, mas cujo download da imagem falhou/ficou nulo
    df_final = df[~((df['has_images'] == True) & (df['image_path'].isnull()))].copy()
 
    # Remove as colunas auxiliares 'image_url' e 'image_status' para limpar a base final
    cols_to_drop = [col for col in ['image_url', 'image_status'] if col in df_final.columns]
    df_final.drop(columns=cols_to_drop, inplace=True)
 
    df_final.to_csv(FINAL_CSV_PATH, index=False)
 
    # Opcional: Remover arquivo temporário
    if os.path.exists(TEMP_CSV_PATH):
        os.remove(TEMP_CSV_PATH)
 
    print(f"Pipeline concluído! CSV Final salvo em: {FINAL_CSV_PATH}")
 
 
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
 
    # Ordem de execução
    t1 >> t2 >> t3 >> t4 >> t5
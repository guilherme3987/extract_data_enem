from pathlib import Path

# Diretórios e URLs
BASE_PATH   = Path("/tmp/projeto_dados_enem")
REPO_URL    = "https://github.com/yunger7/enem-api.git"

# Pasta raiz dos dados no repositório clonado
PUBLIC_PATH = BASE_PATH / "public"

# Destino dos arquivos extraídos
OUTPUT_PATH = Path("/opt/airflow/dags/data")
OUTPUT_JSON = OUTPUT_PATH / "questions.json"
OUTPUT_CSV  = OUTPUT_PATH / "questions.csv"
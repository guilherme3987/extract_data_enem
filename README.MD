# 📊 Projeto ETL ENEM com Apache Airflow + IA

## 📌 Visão Geral
Pipeline ETL para processamento de questões do ENEM com enriquecimento usando IA.

## ⚙️ Tecnologias
- Apache Airflow
- Python
- Pandas
- Groq LLM (LLaMA 3.3-70B)
- Requests
- GitHub API

## 🚀 Pipeline

### 1. Extração
Clona repositório e gera dataset inicial.

### 2. Filtragem
- Valida imagens
- Remove dados incompletos
- Baixa recursos externos

### 3. Enriquecimento
- Gera dicas pedagógicas via IA
- Estrutura saída em JSON

## 📂 Estrutura

dags/
 ├── extract_data_git_repo.py
 ├── extract_questions.py
 ├── filter_questions.py
 ├── dag_enrichment.py
 ├── config.py
 └── data/

## 🧠 IA no projeto
Cada questão recebe análise pedagógica com:
- Abstração
- Decomposição
- Reconhecimento de padrões
- Algoritmo

## ▶️ Execução
```bash
docker-compose up airflow
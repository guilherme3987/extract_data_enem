from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Configurações globais
INPUT_CSV = "/opt/airflow/dags/data/questions_final.csv"
OUTPUT_CSV = "/opt/airflow/dags/data/questions_enrichment.csv"

def processar_enriquecimento():
    import pandas as pd
    import json
    from groq import Groq
    from dotenv import load_dotenv

    # 1. Inicialização
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    client = Groq(api_key=api_key)

    # 2. Carga e Limpeza de "Fantasmas"
    if os.path.exists(OUTPUT_CSV):
        df = pd.read_csv(OUTPUT_CSV)
    else:
        df = pd.read_csv(INPUT_CSV)
        for col in ['abstracao', 'decomposicao', 'reconhecimento_padroes', 'algoritmo']:
            df[col] = None

    # Normalização crítica de IDs e Colunas
    df['index'] = df['index'].astype(str).str.replace('.0', '', regex=False).str.strip()
    
    # Converte strings de erro comuns para NaN real para o filtro não pular a linha
    for col in ['abstracao', 'decomposicao', 'reconhecimento_padroes', 'algoritmo']:
        df[col] = df[col].replace(['nan', 'None', '', ' ', 'NaN'], pd.NA)

    # 3. Seleção da Questão
    pendentes = df[df['abstracao'].isna()]
    
    if pendentes.empty:
        print("✅ Processamento concluído: 100% da base preenchida.")
        return

    questao_atual = pendentes.iloc[0]
    id_solicitado = questao_atual['index']
    
    # 4. Prompt com Grounding (Ancoragem)
    texto_questao = f"CONTEXTO: {questao_atual['context'][:600]}\nCOMANDO: {questao_atual['alternatives_intro']}"
    
    prompt = f"""
    Aja como um Professor de Pensamento Computacional. Gere o JSON com as dicas para a questão abaixo.
    
    REGRAS:
    - Máximo 12 palavras por dica.
    - Comece com Verbo no Imperativo.
    - Não use o ID no JSON, apenas as chaves: abstracao, decomposicao, reconhecimento_padroes, algoritmo.

    QUESTÃO:
    {texto_questao}
    """

    try:
        # 5. Chamada à API
        resposta = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Você é um gerador de JSON estrito. Responda apenas o objeto JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        dados_ia = json.loads(resposta.choices[0].message.content)
        
        # 6. Gravação com Validação de Chaves
        linha_certa = df['index'] == str(id_solicitado)
        
        # Mapeamento dinâmico para evitar erro de chaves faltantes
        for pilar in ['abstracao', 'decomposicao', 'reconhecimento_padroes', 'algoritmo']:
            valor = dados_ia.get(pilar)
            if valor:
                df.loc[linha_certa, pilar] = str(valor).strip()
            else:
                print(f"⚠️ Aviso: Campo '{pilar}' veio vazio da IA para o ID {id_solicitado}")

        # 7. Escrita Atômica
        temp_csv = OUTPUT_CSV + ".tmp"
        df.to_csv(temp_csv, index=False)
        os.replace(temp_csv, OUTPUT_CSV)
        
        print(f"✅ Sucesso: ID {id_solicitado} processado.")

    except Exception as e:
        print(f"❌ Erro no ID {id_solicitado}: {e}")
        raise e

with DAG(
    "data_enrichment",
    default_args={"owner": "airflow", "start_date": datetime(2024, 1, 1)},
    schedule_interval="*/1 * * * *", 
    catchup=False,
) as dag:
    PythonOperator(task_id="gerar_dica_unica", python_callable=processar_enriquecimento)
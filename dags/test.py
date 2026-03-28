import os
import json
from dotenv import load_dotenv
import google.generativeai as genai

def test_gemini_connection():
    # Carrega o arquivo .env
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        print("❌ ERRO: GEMINI_API_KEY não encontrada no arquivo .env")
        return

    print(f"Sinalizando configuração com a chave: {api_key[:5]}...{api_key[-5:]}")
    genai.configure(api_key=api_key)

    # Lista os modelos disponíveis para ver se o 1.5-flash aparece
    print("\n--- Verificando modelos disponíveis ---")
    try:
        available_models = [m.name for m in genai.list_models()]
        for model_name in available_models:
            print(f"Modelo encontrado: {model_name}")
        
        target_model = "models/gemini-1.5-flash"
        if target_model not in available_models:
            print(f"\n⚠️ AVISO: {target_model} não foi listado explicitamente.")
    except Exception as e:
        print(f"❌ Erro ao listar modelos: {e}")

    # Teste de geração de conteúdo
    print("\n--- Testando geração de conteúdo (gemini-1.5-flash) ---")
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content("Responda apenas com a palavra 'Funcionando'.")
        print(f"✅ RESPOSTA DA API: {response.text}")
    except Exception as e:
        print(f"❌ ERRO NA CHAMADA: {e}")
        
        # Teste alternativo com gemini-pro se o flash falhar
        print("\n--- Tentando fallback para gemini-pro ---")
        try:
            model_pro = genai.GenerativeModel('gemini-pro')
            response_pro = model_pro.generate_content("Oi")
            print(f"✅ RESPOSTA PRO: {response_pro.text}")
        except Exception as e_pro:
            print(f"❌ FALHA TOTAL: {e_pro}")

if __name__ == "__main__":
    test_gemini_connection()
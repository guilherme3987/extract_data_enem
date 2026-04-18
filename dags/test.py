import os
import time
from dotenv import load_dotenv

# Importação das bibliotecas das APIs
from google import genai
from groq import Groq
from cerebras.cloud.sdk import Cerebras
from huggingface_hub import InferenceClient

# =============================================================
# FUNÇÃO DE MONITORAMENTO (RATE LIMIT E TOKENS)
# =============================================================
def print_ratelimit_info(api_name, headers=None, usage=None):
    print(f"\n📊 [MÉTRICAS DE ACESSO: {api_name}]")
    
    # Extração de Headers (Padrão Groq/Cerebras/OpenAI)
    if headers:
        rpm = headers.get("x-ratelimit-remaining-requests")
        tpm = headers.get("x-ratelimit-remaining-tokens")
        rpd = headers.get("x-ratelimit-remaining-requests-day") # Comum na Cerebras
        
        if rpm: print(f"🔹 Requisições Restantes (RPM): {rpm}")
        if tpm: print(f"🔹 Tokens Restantes (TPM): {tpm}")
        if rpd: print(f"🔹 Cota Diária Restante (RPD): {rpd}")
        
    # Extração de Uso de Tokens
    if usage:
        # Formato Padrão OpenAI (Groq/Cerebras/HF)
        if hasattr(usage, 'prompt_tokens'):
            print(f"🔸 Consumo: Prompt({usage.prompt_tokens}) | Resposta({usage.completion_tokens}) | Total({usage.total_tokens})")
        # Formato Google GenAI (Gemini)
        elif hasattr(usage, 'prompt_token_count'):
            print(f"🔸 Consumo: Prompt({usage.prompt_token_count}) | Resposta({usage.candidates_token_count}) | Total({usage.total_token_count})")

# =============================================================
# FUNCIONALIDADES DE TESTE
# =============================================================

def check_env_vars():
    print("\n--- Verificando Variáveis no .env ---")
    chaves = {
        "Gemini": "GEMINI_API_KEY",
        "Groq 1": "GROQ_API_KEY",
        "Groq 2": "GROQ_API_2_KEY",
        "Cerebras": "CEREBRAS_API_KEY",
        "Hugging Face": "HUGGINFACE_API_KEY"
    }
    for nome, var in chaves.items():
        val = os.getenv(var)
        status = f"✅ {val[:5]}...{val[-5:]}" if val else "❌ NÃO ENCONTRADA"
        print(f"{nome:15} : {status}")

def test_gemini():
    print("\nIniciando Teste Gemini...")
    key = os.getenv("GEMINI_API_KEY")
    if not key: return
    client = genai.Client(api_key=key)
    try:
        # Gemini 2.0 Flash é o modelo mais estável e rápido para testes
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents="Responda 'OK'."
        )
        print(f"✅ Gemini: {response.text.strip()}")
        print_ratelimit_info("Gemini", usage=response.usage_metadata)
    except Exception as e:
        print(f"❌ Erro Gemini: {e}")

def test_groq(key_var="GROQ_API_KEY", model="llama3-8b-8192"):
    print(f"\nIniciando Teste Groq ({key_var})...")
    key = os.getenv(key_var)
    if not key: return
    client = Groq(api_key=key)
    try:
        # Usando with_raw_response para capturar headers
        raw = client.chat.completions.with_raw_response.create(
            messages=[{"role": "user", "content": "Responda 'OK'."}],
            model=model,
        )
        res = raw.parse()
        print(f"✅ Groq: {res.choices[0].message.content}")
        print_ratelimit_info(f"Groq ({key_var})", headers=raw.headers, usage=res.usage)
    except Exception as e:
        print(f"❌ Erro Groq: {e}")

def test_cerebras():
    print("\nIniciando Teste Cerebras...")
    key = os.getenv("CEREBRAS_API_KEY")
    if not key: return
    client = Cerebras(api_key=key)
    try:
        raw = client.chat.completions.with_raw_response.create(
            messages=[{"role": "user", "content": "Responda 'OK'."}],
            model="llama3.1-8b",
        )
        res = raw.parse()
        print(f"✅ Cerebras: {res.choices[0].message.content}")
        print_ratelimit_info("Cerebras", headers=raw.headers, usage=res.usage)
    except Exception as e:
        print(f"❌ Erro Cerebras: {e}")

def test_hf_text():
    print("\nIniciando Teste Hugging Face Texto...")
    key = os.getenv("HUGGINFACE_API_KEY")
    if not key: return
    client = InferenceClient(api_key=key)
    try:
        res = client.chat_completion(
            model="meta-llama/Llama-3.2-3B-Instruct",
            messages=[{"role": "user", "content": "Responda 'OK'."}],
            max_tokens=10
        )
        print(f"✅ HF Texto: {res.choices[0].message.content}")
        print_ratelimit_info("Hugging Face", usage=res.usage)
    except Exception as e:
        print(f"❌ Erro HF: {e}")

# =============================================================
# MENU PRINCIPAL
# =============================================================

def menu():
    load_dotenv()
    while True:
        print("\n" + "="*30)
        print("   MENU DE TESTES DE API")
        print("="*30)
        print("1. Verificar chaves no .env")
        print("2. Testar Gemini (Google)")
        print("3. Testar Groq 1 (Llama 3 8B)")
        print("4. Testar Groq 2 (Llama 3.3 70B)")
        print("5. Testar Cerebras")
        print("6. Testar Hugging Face (Texto)")
        print("7. Rodar TODOS os testes")
        print("0. Sair")
        
        opcao = input("\nEscolha uma opção: ")

        if opcao == "1": check_env_vars()
        elif opcao == "2": test_gemini()
        elif opcao == "3": test_groq("GROQ_API_KEY", "llama3-8b-8192")
        elif opcao == "4": test_groq("GROQ_API_2_KEY", "llama-3.3-70b-versatile")
        elif opcao == "5": test_cerebras()
        elif opcao == "6": test_hf_text()
        elif opcao == "7":
            check_env_vars()
            test_gemini()
            test_groq("GROQ_API_KEY", "llama3-8b-8192")
            test_cerebras()
            test_hf_text()
        elif opcao == "0":
            print("Encerrando...")
            break
        else:
            print("Opção inválida!")
        
        time.sleep(1)

if __name__ == "__main__":
    menu()
import os
from dotenv import load_dotenv

from google import genai
from groq import Groq
from cerebras.cloud.sdk import Cerebras
from huggingface_hub import InferenceClient

def print_char_api_key():
    print("\n--- Verificando Variáveis no .env ---")
    chaves = {
        "Gemini": "GEMINI_API_KEY",
        "Groq 1": "GROQ_API_KEY",
        "Groq 2": "GROQ_API_2_KEY",
        "Cerebras": "CEREBRAS_API_KEY",
        "Hugging Face": "HUGGINFACE_API_KEY"
    }

    for nome, var_env in chaves.items():
        chave = os.getenv(var_env)
        if chave:
            print(f"✅ {nome} encontrada: {chave[:5]}...{chave[-5:]}")
        else:
            print(f"❌ {nome} ERRO: '{var_env}' não encontrada no arquivo .env")

def test_gemini_connection():
    print("\n=============================================")
    print("           TESTANDO API DO GEMINI            ")
    print("=============================================")
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key: return

    client = genai.Client(api_key=api_key)
    modelos_tentativa = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-1.5-flash']
    
    for nome in modelos_tentativa:
        try:
            response = client.models.generate_content(
                model=nome,
                contents="Responda apenas: 'Funcionando no Gemini!'"
            )
            print(f"✅ Sucesso ({nome}): {response.text.strip()}")
            break
        except Exception as e:
            continue

def test_groq_connection():
    print("\n=============================================")
    print("           TESTANDO API DA GROQ (1)          ")
    print("=============================================")
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key: return

    client = Groq(api_key=api_key)    
    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": "Responda apenas: 'Funcionando na Groq 1!'" }],
            model="llama3-8b-8192", 
        )
        print(f"✅ Sucesso: {chat_completion.choices[0].message.content}")
    except Exception as e:
        print(f"❌ Erro: {e}")

def test_groq_2_connection():
    print("\n=============================================")
    print("           TESTANDO API DA GROQ (2)          ")
    print("=============================================")
    api_key = os.getenv("GROQ_API_2_KEY")
    if not api_key: return

    client = Groq(api_key=api_key)    
    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": "Responda apenas: 'Funcionando na Groq 2!'" }],
            model="llama-3.3-70b-versatile",
        )
        print(f"✅ Sucesso: {chat_completion.choices[0].message.content}")
    except Exception as e:
        print(f"❌ Erro: {e}")

def test_cerebras():
    print("\n=============================================")
    print("          TESTANDO API DA CEREBRAS           ")
    print("=============================================")
    api_key = os.environ.get("CEREBRAS_API_KEY")
    if not api_key: return
    
    client = Cerebras(api_key=api_key)
    try:
        chat_completion = client.chat.completions.create(
            messages=[{"role": "user", "content": "Responda apenas: 'Funcionando na Cerebras!'" }],
            model="llama3.1-8b",
        )
        print(f"✅ Sucesso: {chat_completion.choices[0].message.content}")
    except Exception as e:
        print(f"❌ Erro: {e}")

def test_huggingface_texto():
    print("\n=============================================")
    print("      TESTANDO HUGGING FACE (TEXTO)          ")
    print("=============================================")
    api_key = os.environ.get("HUGGINFACE_API_KEY")
    if not api_key: return
    
    client = InferenceClient(api_key=api_key)
    
    modelos_texto = [
        "meta-llama/Llama-3.2-3B-Instruct",
        "Qwen/Qwen2.5-72B-Instruct"
    ]

    for nome in modelos_texto:
        try:
            print(f"Tentando modelo de texto: {nome}")
            response = client.chat_completion(
                model=nome,
                messages=[{"role": "user", "content": "Responda apenas: 'Funcionando Texto HF!'"}]
            )
            print(f"✅ Sucesso ({nome}): {response.choices[0].message.content}")
            return # Sai da função se der certo
        except Exception as e:
            print(f"❌ Falha com {nome}: {e}")

def test_huggingface_visao():
    print("\n=============================================")
    print("      TESTANDO HUGGING FACE (VISÃO)          ")
    print("=============================================")
    api_key = os.environ.get("HUGGINFACE_API_KEY")
    if not api_key: return
    
    client = InferenceClient(api_key=api_key)
    modelo_visao = "meta-llama/Llama-3.2-11B-Vision-Instruct"
    url_imagem = "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/Cat03.jpg/1200px-Cat03.jpg"

    try:
        print(f"Enviando imagem para análise no modelo: {modelo_visao}")
        response = client.chat_completion(
            model=modelo_visao,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Descreva o animal que aparece nesta imagem em português em uma frase curta."},
                        {"type": "image_url", "image_url": {"url": url_imagem}}
                    ]
                }
            ]
        )
        print(f"✅ A IA viu a imagem: {response.choices[0].message.content}")
    except Exception as e:
        print(f"❌ Erro na visão computacional: {e}")

if __name__ == "__main__":
    load_dotenv()
    print_char_api_key()
    
    test_gemini_connection()
    test_groq_connection()
    test_groq_2_connection()
    test_cerebras()
    test_huggingface_texto()
    test_huggingface_visao()
    
    print("\n✅ Todos os testes foram finalizados.")
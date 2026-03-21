### 📋 Roteiro de Extração: Clone e Localização de Dados

#### **1. Preparação do Ambiente (The Clean Slate)**
```bash
mkdir ~/projeto_dados_enem
cd ~/projeto_dados_enem
```

#### **2. O Clone Definitivo**
Optamos pelo **clone completo** do repositório da API. Diferente do clone parcial, este garante que todos os objetos e a estrutura de arquivos sejam baixados corretamente, totalizando aproximadamente **88.38 MiB**.
```bash
git clone https://github.com/yunger7/enem-api.git .
```
> **Nota:** O uso do ponto `.` ao final do comando instrui o Git a clonar o conteúdo diretamente na pasta atual, sem criar uma subpasta adicional.

#### **3. Mapeamento da Estrutura (Discovery)**
Após o download de **4.702 arquivos**, utilizamos ferramentas de busca do Linux (`find` e `ls`) para localizar os dados. Descobrimos que a estrutura atual do repositório organiza as questões de forma distribuída:
* **Pasta Raiz:** `public/`
* **Subpastas por Ano:** `2009/` até `2023/`
* **Localização dos JSONs:** `public/{ano}/questions/`

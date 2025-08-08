import pandas as pd
import google.generativeai as genai
import os
import json
import logging
import time
from tqdm import tqdm # Para uma barra de progresso visual

# --- CONFIGURAÇÃO ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure sua API Key do Google AI Studio (ou outra)
# É uma boa prática usar variáveis de ambiente
# from dotenv import load_dotenv
# load_dotenv()
# GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key="YOUR_GOOGLE_API_KEY_HERE")  # Substitua pela sua chave de API

INPUT_FILE = 'data/dados_preparados_para_frames.csv'
OUTPUT_FILE = 'data/dados_com_frames.csv'

# --- MODELO DE PROMPT E FUNÇÃO DE CLASSIFICAÇÃO ---

def get_prompt(text_to_classify):
    # O "Prompt Perfeito" que definimos anteriormente
    return f"""Você é um pesquisador assistente especializado em análise de discurso e estudos sociais. Sua tarefa é classificar o texto a seguir, extraído de uma discussão online no Reddit, em UMA das seguintes categorias pré-definidas (frames).

Analise o texto com atenção e escolha a categoria que melhor representa o enquadramento principal da discussão.

**Categorias (Frames):**
- **Ferramenta/Produtividade:** O texto discute o uso prático da IA para resolver problemas, criar ou trabalhar.
- **Riscos/Ética:** O texto foca nos perigos, dilemas morais, vieses ou na necessidade de regulamentação da IA.
- **Mercado de Trabalho:** O texto aborda o impacto da IA em empregos e profissões.
- **Técnico/Curiosidade:** O texto é sobre o funcionamento da tecnologia, modelos específicos ou aspectos técnicos.
- **Cultura/Notícias:** O texto menciona a IA em um contexto de entretenimento, notícias gerais, arte ou memes.
- **Outro/Não relacionado:** O texto não se encaixa em nenhuma das categorias acima.

Responda APENAS com um objeto JSON contendo duas chaves: "frame" com a categoria escolhida e "justificativa" com uma breve explicação (uma frase) da sua escolha.

**Texto para classificar:**
"{text_to_classify}"

**Sua resposta em JSON:**
"""

def classify_text_with_llm(text):
    """Envia o texto para a API do LLM e retorna o resultado da classificação."""
    if not text or not isinstance(text, str) or len(text.strip()) < 10:
        return {"frame": "Outro/Não relacionado", "justificativa": "Texto muito curto ou inválido."}

    prompt = get_prompt(text)
    model = genai.GenerativeModel('gemini-1.5-flash') # Modelo rápido e eficiente para a tarefa
    
    try:
        response = model.generate_content(prompt)
        # Limpa a resposta para garantir que seja um JSON válido
        json_response_text = response.text.strip().replace('```json', '').replace('```', '')
        result = json.loads(json_response_text)
        return result
    except Exception as e:
        logging.error(f"Erro ao classificar texto: {e}")
        # Retorna um erro estruturado para não quebrar o processo
        return {"frame": "ERRO", "justificativa": str(e)}

# --- SCRIPT PRINCIPAL ---

def run_classification():
    # Carrega os dados preparados
    df = pd.read_csv(INPUT_FILE)
    
    # Prepara o arquivo de saída
    if os.path.exists(OUTPUT_FILE):
        logging.info("Arquivo de saída já existe. Carregando e continuando do ponto de parada.")
        df_output = pd.read_csv(OUTPUT_FILE)
        # Converte a coluna de ID para string para garantir a correspondência
        df['doc_id'] = df['doc_id'].astype(str)
        df_output['doc_id'] = df_output['doc_id'].astype(str)
        processed_ids = set(df_output['doc_id'])
    else:
        df_output = pd.DataFrame()
        processed_ids = set()

    # Itera sobre o DataFrame com uma barra de progresso (tqdm)
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Classificando Documentos"):
        doc_id = str(row['doc_id'])
        if doc_id in processed_ids:
            continue # Pula o que já foi processado

        text = row['text_cleaned']
        classification_result = classify_text_with_llm(text)
        
        # Cria um novo DataFrame para a linha atual com os resultados
        new_row_data = row.to_dict()
        new_row_data['frame'] = classification_result.get('frame')
        new_row_data['justificativa_llm'] = classification_result.get('justificativa')
        
        # Anexa a nova linha ao DataFrame de saída
        df_new_row = pd.DataFrame([new_row_data])
        
        # Salva a cada iteração (seguro, mas pode ser lento) ou em lotes
        # Para salvar a cada linha:
        df_new_row.to_csv(OUTPUT_FILE, mode='a', header=not os.path.exists(OUTPUT_FILE), index=False)
        processed_ids.add(doc_id)

        time.sleep(1) # Pausa para não exceder limites da API

    logging.info(f"Classificação concluída! Resultados salvos em '{OUTPUT_FILE}'.")


if __name__ == '__main__':
    # Antes de rodar, uma sugestão:
    # 1. Obtenha sua API Key do Google AI Studio (ou outro provedor).
    # 2. Instale as bibliotecas: pip install google-generativeai tqdm pandas
    # 3. **PLANO DE AÇÃO:** Rode primeiro com uma amostra!
    #    df_sample = pd.read_csv(INPUT_FILE).head(20)
    #    df_sample.to_csv('data/amostra_preparada.csv', index=False)
    #    E mude o INPUT_FILE para a amostra para testar o prompt e o processo.
    
    run_classification()

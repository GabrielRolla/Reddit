import pandas as pd
import re
import os
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_data_preparation(posts_file, comments_file, output_file):
    """
    Carrega os dados brutos, combina posts e comentários, e aplica uma
    limpeza leve para preparar para a análise de frames.
    """
    logging.info("Iniciando a preparação dos dados...")

    # --- 1. CARREGAR DADOS ---
    try:
        # Usa o 'id' dos seus arquivos originais para manter a consistência
        df_posts = pd.read_csv(posts_file, dtype={'id': str})
        df_comments = pd.read_csv(comments_file, dtype={'id': str, 'post_id': str})
        logging.info(f"Carregados {len(df_posts)} posts e {len(df_comments)} comentários.")
    except FileNotFoundError as e:
        logging.error(f"Erro: Arquivo não encontrado. {e}. Verifique o caminho dos arquivos.")
        return
    except Exception as e:
        logging.error(f"Ocorreu um erro ao ler os arquivos CSV: {e}")
        return

    # --- 2. COMBINAR E ESTRUTURAR ---
    
    # Prepara os posts
    df_posts['text'] = df_posts['title'].fillna('') + ' ' + df_posts['body'].fillna('')
    df_posts.rename(columns={'id': 'doc_id'}, inplace=True)
    df_posts['doc_type'] = 'post'
    # Mantém colunas relevantes
    posts_final = df_posts[['doc_id', 'doc_type', 'text', 'created_utc', 'score', 'num_comments', 'subreddit', 'category']]

    # Prepara os comentários
    df_comments.rename(columns={'id': 'doc_id', 'body': 'text'}, inplace=True)
    df_comments['doc_type'] = 'comment'
    # Garante que as colunas essenciais existam
    df_comments['num_comments'] = 0 # Comentários não têm contagem de comentários aninhados aqui
    comments_final = df_comments[['doc_id', 'doc_type', 'text', 'created_utc', 'score', 'num_comments', 'subreddit', 'category', 'post_id']]

    # Concatena em um único DataFrame
    df_combined = pd.concat([posts_final, comments_final], ignore_index=True)
    logging.info(f"Dados combinados. Total de {len(df_combined)} documentos.")

    # --- 3. LIMPEZA LEVE (NÃO-DESTRUTIVA) ---
    logging.info("Aplicando limpeza leve (remoção de URLs e artefatos do Reddit)...")

    def light_clean(text):
        if not isinstance(text, str):
            return ""
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        # Remove marcadores do Reddit como [removido], [deletado]
        text = re.sub(r'\[\s*(removido|deletado)\s*\]', '', text, flags=re.IGNORECASE)
        # Remove markdown de citação ('>') e normaliza espaços
        text = re.sub(r'^\s*>\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    df_combined['text_cleaned'] = df_combined['text'].apply(light_clean)

    # Remove linhas onde o texto ficou completamente vazio após a limpeza
    df_combined.dropna(subset=['text_cleaned'], inplace=True)
    df_combined = df_combined[df_combined['text_cleaned'] != '']
    
    # --- 4. SALVAR DADOS PREPARADOS ---
    os.makedirs('data', exist_ok=True)
    df_combined.to_csv(output_file, index=False, encoding='utf-8-sig')
    logging.info(f"Preparação concluída. {len(df_combined)} documentos salvos em '{output_file}'.")


if __name__ == '__main__':
    # Use os nomes dos arquivos que o seu crawler gerou.
    # Exemplo, se o seu arquivo base foi 'reddit_data_20231027_103000'
    BASE_FILENAME = "reddit_data_20250803_012941" # Adapte este nome para o seu arquivo
    POSTS_FILE = f'data/{BASE_FILENAME}_posts.csv'
    COMMENTS_FILE = f'data/{BASE_FILENAME}_comments.csv'
    OUTPUT_FILE = 'data/dados_preparados_para_frames.csv'
    
    # Verifica se os arquivos de entrada existem
    if os.path.exists(POSTS_FILE) and os.path.exists(COMMENTS_FILE):
        run_data_preparation(POSTS_FILE, COMMENTS_FILE, OUTPUT_FILE)
    else:
        logging.error(f"Arquivos de entrada não encontrados: '{POSTS_FILE}' ou '{COMMENTS_FILE}'.")
        logging.error("Por favor, verifique o nome do arquivo base (BASE_FILENAME) no script.")
import praw
import prawcore
import pandas as pd
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import json
import logging

# Configurar logging corretamente
timestamp_fmt = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=timestamp_fmt,
    handlers=[logging.FileHandler('reddit_crawler.log'), logging.StreamHandler()]
)

class RedditCrawler:
    def __init__(self, config=None):
        load_dotenv()
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'RedditCrawler/1.0'),
            username=os.getenv('REDDIT_USERNAME'),
            password=os.getenv('REDDIT_PASSWORD')
        )
        self.ai_keywords = config.get('ai_keywords') if config and 'ai_keywords' in config else [
            'inteligência artificial', 'IA', 'AI'
            'ChatGPT', 'LLM', 'modelo de linguagem',
            'GPT', 'Gemini', 'artificial intelligence',
        ]
        # Subreddits a serem rastreados por categoria
        self.subreddits = config.get('subreddits') if config and 'subreddits' in config else {
            
            'geral': ['brasil', 'brasil2', 'conversas', 'PergunteReddit', 'r/BrasilOnReddit'],

            'tecnologia': ['brdev', 'datasciencebr', 'chatgpt_brasil', 'computadores',
                            'WindowsBrasil', 'Aplicativo', 'AssistenciaTecnica',
                            'Programadores_Alados', 'hardwarebrasil', 'Linuxbrasil', 'programacao'],
        }
        logging.info("RedditCrawler inicializado")

    def search_posts(self, subreddit_name, keywords=None, limit=100, time_filter='year'):
        keywords = keywords or self.ai_keywords
        posts = []
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            # valida existência
            _ = subreddit.id
        except prawcore.exceptions.Redirect:
            logging.warning(f"r/{subreddit_name} não encontrado. Pulando subreddit.")
            return posts
        except Exception as e:
            logging.warning(f"Erro ao acessar r/{subreddit_name}: {e}")
            return posts

        # Recent posts
        try:
            for post in subreddit.new(limit=limit//2):
                text = f"{post.title} {post.selftext}".lower()
                if any(kw.lower() in text for kw in keywords):
                    kw_used = next(kw for kw in keywords if kw.lower() in text)
                    posts.append({
                        'id': post.id,
                        'title': post.title,
                        'body': post.selftext,
                        'author': str(post.author),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'created_utc': post.created_utc,
                        'subreddit': subreddit_name,
                        'keyword': kw_used,
                        'method': 'recent'
                    })
        except Exception as e:
            logging.warning(f"Erro ao buscar posts recentes em r/{subreddit_name}: {e}")

        # Keyword search
        per_kw = max(1, limit // len(keywords))
        for kw in keywords:
            try:
                for post in subreddit.search(kw, limit=per_kw, time_filter=time_filter, sort='relevance'):
                    if not any(p['id'] == post.id for p in posts):
                        posts.append({
                            'id': post.id,
                            'title': post.title,
                            'body': post.selftext,
                            'author': str(post.author),
                            'score': post.score,
                            'upvote_ratio': post.upvote_ratio,
                            'num_comments': post.num_comments,
                            'created_utc': post.created_utc,
                            'subreddit': subreddit_name,
                            'keyword': kw,
                            'method': 'search'
                        })
                time.sleep(1)
            except prawcore.exceptions.Redirect:
                logging.warning(f"Pesquisa por '{kw}' em r/{subreddit_name} redirecionou. Interrompendo busca de keywords.")
                break
            except Exception as e:
                logging.warning(f"Erro ao buscar '{kw}' em r/{subreddit_name}: {e}")
        return posts

    def get_comments(self, post_id, limit=300):
        comments = []
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comment_sort = 'top'
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list()[:limit]:
                comments.append({
                    'id': comment.id,
                    'body': comment.body,
                    'author': str(comment.author),
                    'score': comment.score,
                    'created_utc': comment.created_utc,
                    'post_id': post_id
                })
        except Exception as e:
            logging.warning(f"Erro ao obter comentários do post {post_id}: {e}")
        return comments

    def crawl_subreddit(self, subreddit_name, posts_limit=500, comments_limit=50):
        logging.info(f"Crawling em r/{subreddit_name}...")
        posts = self.search_posts(subreddit_name, limit=posts_limit)
        all_comments = []
        for post in posts[:10]:
            all_comments.extend(self.get_comments(post['id'], limit=comments_limit))
            time.sleep(1)
        return {'subreddit': subreddit_name, 'posts': posts, 'comments': all_comments}

    def crawl_all(self, posts_limit=100, comments_limit=50):
        data = {}
        for category, subs in self.subreddits.items():
            data[category] = {}
            for sub in subs:
                data[category][sub] = self.crawl_subreddit(sub, posts_limit, comments_limit)
                time.sleep(2)
        return data

    def save_data(self, data, base_filename=None):
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        base = base_filename or f"reddit_data_{ts}"
        os.makedirs('data', exist_ok=True)
        with open(f"data/{base}.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        posts, comments = [], []
        for cat, subs in data.items():
            for sub, d in subs.items():
                for p in d['posts']:
                    p['category'] = cat; posts.append(p)
                for c in d['comments']:
                    c['category'] = cat; c['subreddit'] = sub; comments.append(c)
        if posts: pd.DataFrame(posts).to_csv(f"data/{base}_posts.csv", index=False)
        if comments: pd.DataFrame(comments).to_csv(f"data/{base}_comments.csv", index=False)

if __name__ == '__main__':
    crawler = RedditCrawler()
    data = crawler.crawl_all(posts_limit=50, comments_limit=20)
    crawler.save_data(data)
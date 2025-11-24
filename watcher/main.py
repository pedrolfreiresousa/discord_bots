import os
import asyncio
import logging
from datetime import datetime, timezone
import sqlite3
import httpx
import jwt
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
load_dotenv()


#configurações principais
TWITTER_BEARER = os.getenv("TWITTER_BEARER")
PUBLISHER_API = os.getenv("PUBLISHER_API", "http://127.0.0.1:8000/incoming")
PUBLISHER_SECRET = os.getenv("PUBLISHER_SECRET", "verysecret")
JWT_ALGO = "HS256"
DB_PATH = os.getenv("WATCHER_DB", "watcher.db")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))


#aqui vai ficar cada conta do twiter
X_SOURCES = [
    {"type":"x", "handle":"PL_Web3"},
    {"type":"x", "handle":"SuiNetworkBr"},
    {"type":"x", "handle":"ParaBuilders"}
]

#configura o wather para acompanhar eventos e erros
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("watcher")

#cria uma tabela para armazenar tudo que já foi detectado
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS seen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        item_id TEXT NOT NULL,
        url TEXT,
        seen_at TEXT,
        UNIQUE(source, item_id )
    )
    """)
    conn.commit()
    return conn

db_conn = init_db()

#insere a postagem no banco com timestamp, evita duplicação de postagens
def mark_seen(source, item_id, url = None):
    c = db_conn.cursor()
    try:
        c.execute("INSERT INTO seen (source, item_id, url, seen_at) VALUES (?, ?, ?, ?)",
                (source, item_id, url, datetime.now(timezone.utc).isoformat()))
        db_conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    

#gera um token jwt válido por 60 sec
def make_jwt_for_publisher(payload: dict, expire_seconds = 60):
    p = payload.copy()
    p["iat"] = int(time.time())
    p["exp"] = int(time.time()) + expire_seconds
    return jwt.encode(p, PUBLISHER_SECRET, algorithm = JWT_ALGO)


#envia o post para o publisher
async def send_to_publisher(source_name, url, title = None, published_at = None):
    payload = {"source" : source_name, "url" : url, "title" : title, "published_at" : published_at}
    token = make_jwt_for_publisher({"iss" : "watcher", "source" : source_name})
    headers = {"Authorization" : f"bearer {token}"}
    async with httpx.AsyncClient(timeout = 20) as client:
        try:
            r = await client.post(PUBLISHER_API, json = payload, headers = headers)
            r.raise_for_status()
            logger.info("Sent to publisher: %s -> %s", source_name, url)
            return r.json()
        except Exception as e:
            logger.exception("Failed to send to publisher: %s", e)
            return None
        
#verificação da contra do twiter
async def check_x_account(handle):
    """Consulta oficial da API do X usando plano FREE (últimos tweets do usuário)"""

    if not TWITTER_BEARER:
        logger.error("TWITTER_BEARER não configurado no .env")
        return
    
    headers = {
        "Authorization": f"Bearer {TWITTER_BEARER}"
    }

    # 1 → pegar ID do usuário
    url_user = f"https://api.x.com/2/users/by/username/{handle}"

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get(url_user, headers=headers)
            # Se bater rate limit
            if r.status_code == 429:
                reset = int(r.headers.get("x-rate-limit-reset", time.time() + 60))
                wait = max(30, reset - time.time())
                logger.warning(f"RATE LIMIT ao buscar ID de @{handle}. Aguardando {int(wait)}s...")
                await asyncio.sleep(wait)
                return await check_x_account(handle)
            r.raise_for_status()

            user = r.json().get("data")
            if not user:
                logger.error("Usuário não encontrado: %s", handle)
                return

            user_id = user["id"]

            # 2 → pegar tweets recentes do usuário
            url_tweets = f"https://api.x.com/2/users/{user_id}/tweets?max_results=5&tweet.fields=created_at"

            r = await client.get(url_tweets, headers=headers)

            if r.status_code == 429:
                reset = int(r.headers.get("x-rate-limit-reset", time.time() + 60))
                wait = max(30, reset - time.time())
                logger.warning(f"RATE LIMIT ao buscar tweets de @{handle}. Aguardando {int(wait)}s...")
                await asyncio.sleep(wait)
                return await check_x_account(handle)

            r.raise_for_status()



            tweets = r.json().get("data", [])

            for tw in tweets:
                tweet_id = tw["id"]
                link = f"https://x.com/{handle}/status/{tweet_id}"

                c = db_conn.cursor()
                if not mark_seen(f"x:{handle}", tweet_id, link):
                    continue  # já registrado -> IGNORA

                await send_to_publisher(f"x:{handle}", link, title=None)

                

        except Exception as e:
            logger.exception("Erro ao consultar API do X para %s: %s", handle, e)


#cria uma lista de tarefas assincronas para todas as fontes
async def main_loop():
    while True:
        try:
            tasks = []
            for s in X_SOURCES:
                tasks.append(check_x_account(s["handle"]))
            
            await asyncio.gather(*tasks)
        except Exception:
            logger.exception("Main loop error")
        await asyncio.sleep(CHECK_INTERVAL)

#inicia o loop de monitoramento
if __name__ == "__main__":
    asyncio.run(main_loop())
    
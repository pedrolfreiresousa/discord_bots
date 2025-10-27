import os
import asyncio
import logging
from datetime import datetime, timezone
import sqlite3
import httpx
import jwt
import time
from bs4 import BeautifulSoup

from publisher import start_services

if __name__ == "__main__":
    asyncio.run(start_services())



#configurações principais
PUBLISHER_API = os.getenv("PUBLISHER_API", "https://publisher.example.com:8000/incoming")
PUBLISHER_SECRET = os.getenv("PUBLISHER_SECRET", "verysecret")
JWT_ALGO = "HS256"
DB_PATH = os.getenv("WATCHER_DB", "watcher.db")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "60"))


#aqui vai ficar cada conta do twiter ou site que serão monitorados
X_SOURCES = [
    {"type":"x", "handle":"https://x.com/SuiNetworkBr"},
    {"type":"x", "handle":"https://x.com/WatcherGuru"},
    {"type":"x", "handle":"https://x.com/ParaBuilders"}
]
SITE_SOURCES = [
    {"type":"site", "name":"", "url":"", "article_selector":"article a"},
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
        seen_at TEXT
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
        
#verificação da contra do twiter, talvez precise melhorar ou remover
async def check_x_account(handle):
        """Simple scraping fallback: fetch the user's X page and perse tweet links.
        Note: scraping X might be blocked; prefer official API when available.
        """

        url = f"https://x.com/{handle}"
        async with httpx.AsyncClient(timeout = 20, follow_redirects = True) as client:
            try:
                r = await client.get(url)
                r.raise_for_status()
                html = r.text
                soup = BeautifulSoup(html, "html.parser")


                for a in soup.find_all("a", href = True):
                    href = a["href"]
                    if f"/{handle}/status/" in href:
                        if href.startswith("/"):
                            link = "https://x.com" + href

                        else:
                            link = href

                        try:
                            tweet_id = href.split("/status/")[-1].split("?")[0]

                        except:
                            tweet_id = link

                        c = db_conn.cursor()
                        c.execute("SELECT 1 FROM seen WHERE source = ? AND item_id = ? LIMIT 1", (f"x:{handle}", tweet_id))
                        if c.fetchone():
                            continue 

                        mark_seen(f"x:{handle}", tweet_id, link)
                        await send_to_publisher(f"x:{handle}", link,  title = None)
            except Exception as e:
                logger.exception("Error checking X account %s: %s", handle, e)

#verificação dos sites
async def check_site(source: dict):
    url = source["url"]
    sel = source.get("article_selector")
    async with httpx.AsyncClient(timeout = 20) as client:
        try:
            r = await client.get(url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            if sel:
                elements = soup.select(sel)
            
            else:
                elements = []

            for el in elements[:10]:
                a = el if el.name == "a" and el.get("href") else el.find("a")
                if not a:
                    continue
                href = a.get("href")
                if href.startswith("/"):

                    from urllib.parse import urljoin
                    href = urljoin(url, href)

                item_id = href
                c = db_conn.cursor()
                c.execute("SELECT 1 FROM seen WHERE source = ? AND item_id = ? LIMIT 1", (f"site:{source['name']}", item_id))
                if c.fetchone():
                    continue

                mark_seen(f"site:{source['name']}", item_id, href)
                title = a.get_text(strip = True) or source.get("name")
                await send_to_publisher(f"site:{source['name']}", href, title = title)
        except Exception as e:
            logger.exception("Erro checking site %s: %s", url, e)

#cria uma lista de tarefas assincronas para todas as fontes
async def main_loop():
    while True:
        try:
            tasks = []
            for s in X_SOURCES:
                tasks.append(check_x_account(s["handle"]))
            for s in SITE_SOURCES:
                tasks.append(check_site(s))
            await asyncio.gather(*tasks)
        except Exception:
            logger.exception("Main loop error")
        await asyncio.sleep(CHECK_INTERVAL)

#inicia o loop de monitoramento
if __name__ == "__main__":
    asyncio.run(main_loop())
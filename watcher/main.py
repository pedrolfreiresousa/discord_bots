# main.py — watcher robusto para twitterapi.io (com filtro de autor rigoroso)
import os
import asyncio
import logging
import json
import sqlite3
import time
from datetime import datetime, timezone

import httpx
import jwt
from dotenv import load_dotenv

load_dotenv()

# -----------------------
# CONFIGURAÇÕES
# -----------------------
X_API_KEY = os.getenv("X_API_KEY")
X_API_BASE_URL = os.getenv("X_API_BASE_URL", "https://api.twitterapi.io/twitter")
PUBLISHER_API = os.getenv("PUBLISHER_API", "http://127.0.0.1:8000/incoming")
PUBLISHER_SECRET = os.getenv("PUBLISHER_SECRET", "verysecret")
JWT_ALGO = "HS256"
DB_PATH = os.getenv("WATCHER_DB", "watcher.db")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))  # ajustar para 900 em produção
DELAY_BETWEEN_HANDLES = int(os.getenv("DELAY_BETWEEN_HANDLES", "4"))

# Contas monitoradas — edite apenas aqui
X_SOURCES = [
    {"type": "x", "handle": "ParaDevsAI"},
    {"type": "x", "handle": "ParaBuilders"},
    {"type": "x", "handle": "SuiNetworkBr"}

    
]
MONITORED_SET = {s["handle"].lower() for s in X_SOURCES}

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("watcher")

# -----------------------
# DB (seen)
# -----------------------
def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS seen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        item_id TEXT NOT NULL UNIQUE,
        url TEXT,
        seen_at TEXT
    );
    """)
    conn.commit()
    return conn

db_conn = init_db()

def is_seen(source, item_id):
    if item_id is None:
        return True
    c = db_conn.cursor()
    c.execute("SELECT 1 FROM seen WHERE source = ? AND item_id = ? LIMIT 1", (source, item_id))
    return c.fetchone() is not None

def mark_seen(source, item_id, url=None):
    c = db_conn.cursor()
    try:
        c.execute("INSERT INTO seen (source, item_id, url, seen_at) VALUES (?, ?, ?, ?)",
                (source, item_id, url, datetime.now(timezone.utc).isoformat()))
        db_conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

# -----------------------
# JWT -> publisher
# -----------------------
def make_jwt_for_publisher(payload: dict, expire_seconds=60):
    p = payload.copy()
    p["iat"] = int(time.time())
    p["exp"] = int(time.time()) + expire_seconds
    return jwt.encode(p, PUBLISHER_SECRET, algorithm=JWT_ALGO)

async def send_to_publisher(source_name, url, title=None, published_at=None):
    payload = {"source": source_name, "url": url, "title": title, "published_at": published_at}
    token = make_jwt_for_publisher({"iss": "watcher", "source": source_name})
    headers = {"Authorization": f"bearer {token}"}

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.post(PUBLISHER_API, json=payload, headers=headers)
            r.raise_for_status()
            logger.info("Sent to publisher: %s -> %s (status=%s)", source_name, url, r.status_code)
            return r.json()
        except Exception as e:
            logger.exception("Failed to send to publisher: %s", e)
            return None

# -----------------------
# Helpers de parsing e extração
# -----------------------
def collect_tweet_dicts(obj):
    found = []
    if isinstance(obj, dict):
        if (("id" in obj or "rest_id" in obj or "conversationId" in obj) and
            ("text" in obj or "twitterUrl" in obj or "url" in obj or "createdAt" in obj)):
            found.append(obj)
        for v in obj.values():
            found.extend(collect_tweet_dicts(v))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(collect_tweet_dicts(item))
    return found

def extract_tweets_from_response(data):
    if isinstance(data, dict) and "tweets" in data and isinstance(data["tweets"], list):
        return data["tweets"]
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return data["data"]
    if isinstance(data, dict) and "timeline" in data:
        tl = data["timeline"]
        if isinstance(tl, dict):
            for k in ("items", "instructions", "entries"):
                if k in tl and isinstance(tl[k], list):
                    return tl[k]
    return collect_tweet_dicts(data)

def get_tweet_id(tweet):
    if not isinstance(tweet, dict):
        return None
    for k in ("rest_id", "id", "id_str", "conversationId", "tweetId"):
        v = tweet.get(k)
        if v:
            return str(v)
    if "legacy" in tweet and isinstance(tweet["legacy"], dict):
        v = tweet["legacy"].get("id_str")
        if v:
            return str(v)
    return None

def get_tweet_text(tweet):
    if not isinstance(tweet, dict):
        return ""
    for k in ("text", "full_text"):
        v = tweet.get(k)
        if v:
            return v
    if "legacy" in tweet and isinstance(tweet["legacy"], dict):
        return tweet["legacy"].get("full_text") or tweet["legacy"].get("text") or ""
    return tweet.get("displayText") or tweet.get("display_text") or ""

def get_tweet_url(tweet, handle):
    if not isinstance(tweet, dict):
        return None
    for k in ("url", "twitterUrl"):
        v = tweet.get(k)
        if v:
            return v
    tid = get_tweet_id(tweet)
    if tid:
        return f"https://x.com/{handle}/status/{tid}"
    return None

def get_tweet_author_username(tweet):
    """
    Retorna o nome de usuário (ex: 'ParaBuilders') do autor do tweet se disponível.
    Procura em vários lugares: tweet['author']['userName'], tweet['author']['username'],
    tweet['user']['screen_name'], tweet.get('author_username'), tweet.get('userName'), etc.
    """
    if not isinstance(tweet, dict):
        return None
    # author object common in some providers
    author = tweet.get("author") or tweet.get("user") or tweet.get("user_extended") or {}
    if isinstance(author, dict):
        for k in ("userName", "username", "screen_name", "screenName", "handle"):
            v = author.get(k)
            if v:
                return str(v).lower()
    # top-level possibilities
    for k in ("author_username", "authorUserName", "userName", "username", "screen_name"):
        v = tweet.get(k)
        if v:
            return str(v).lower()
    # sometimes there is 'twitterUrl' containing the handle
    tw_url = tweet.get("twitterUrl") or tweet.get("url")
    if isinstance(tw_url, str) and "/status/" in tw_url:
        try:
            # extract between x.com/ and /status
            part = tw_url.split("://")[-1].split("/", 1)[1]
            handle = part.split("/")[0]
            return handle.lower()
        except Exception:
            pass
    return None

# -----------------------
# CHECAGEM DA CONTA (com filtro de autor)
# -----------------------
async def check_x_account(handle: str):
    if not X_API_KEY:
        logger.error("X_API_KEY não configurada no .env")
        return

    logger.info("Verificando tweets de @%s", handle)
    url_tweets = f"{X_API_BASE_URL}/user/last_tweets"
    params = {
        "userName": handle,
        "pageSize": 10,
        "includeReplies": False,
        "tweet.fields": "id,text,created_at,attachments",
        "media.fields": "url,preview_image_url",
        "expansions": "attachments.media_keys"
    }
    headers = {"X-API-Key": X_API_KEY}

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            r = await client.get(url_tweets, params=params, headers=headers)
        except Exception as e:
            logger.exception("Erro de conexão ao consultar API para @%s: %s", handle, e)
            return

        if r.status_code == 429:
            ra = r.headers.get("Retry-After") or r.headers.get("retry-after")
            try:
                wait = int(float(ra)) if ra else 900
            except Exception:
                wait = 900
            logger.warning("RATE LIMIT (429) para @%s -> esperando %ss", handle, wait)
            await asyncio.sleep(wait)
            return
        if r.status_code == 404:
            logger.error("404 ao buscar @%s — verifique X_API_BASE_URL/permissões", handle)
            return

        try:
            data = r.json()
        except Exception:
            # salva resposta bruta
            fname = f"debug-{handle}.txt"
            with open(fname, "w", encoding="utf-8") as fh:
                fh.write(r.text)
            logger.exception("Falha ao decodificar JSON para @%s — resposta salva em %s", handle, fname)
            return

        # salva debug JSON em arquivo
        try:
            fname = f"debug-{handle}.json"
            with open(fname, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
            logger.debug("Saved debug JSON to %s", fname)
        except Exception:
            pass

        tweets = extract_tweets_from_response(data)
        if not tweets:
            logger.info("Nenhum item retornado pela API para @%s", handle)
            return

        sent_any = False
        for tw in tweets:
            tweet_id = get_tweet_id(tw)
            if tweet_id is None:
                logger.debug("Ignorando item sem id (possível meta/advert) para @%s", handle)
                continue

            # EXTRAI E VALIDA O AUTOR
            author_un = get_tweet_author_username(tw)
            if author_un is None:
                # se não sabemos o autor, ignoramos (defensivo)
                logger.warning("Item %s não tem autor claro — ignorando.", tweet_id)
                continue

            # compara rigorosamente com a lista de monitoradas
            if author_un.lower() != handle.lower():
                # Ignora tweets cujo author não é a conta que estamos verificando
                logger.info("Ignorando tweet %s: autor '%s' ≠ handle esperado '%s'", tweet_id, author_un, handle)
                continue

            source_key = f"x:{handle.lower()}"
            if is_seen(source_key, tweet_id):
                logger.debug("Já visto: %s %s", source_key, tweet_id)
                continue

            text = get_tweet_text(tw)
            link = get_tweet_url(tw, handle)

            # marca e envia
            marked = mark_seen(source_key, tweet_id, link)
            if not marked:
                logger.debug("Não marcou tweet (já marcado?) %s %s", source_key, tweet_id)
                continue

            logger.info("Novo tweet detectado: @%s %s", handle, tweet_id)
            await send_to_publisher(source_key, link or f"https://x.com/{handle}/status/{tweet_id}", title=text)
            sent_any = True
            await asyncio.sleep(0.3)

        if not sent_any:
            logger.info("Nenhum tweet novo (após filtro por autor e seen) para @%s", handle)
        return

# -----------------------
# LOOP PRINCIPAL
# -----------------------
async def main_loop():
    logger.info("Watcher iniciado. Intervalo: %s segundos. Contas: %s", CHECK_INTERVAL, [s["handle"] for s in X_SOURCES])
    while True:
        start = time.time()
        for s in X_SOURCES:
            await check_x_account(s["handle"])
            await asyncio.sleep(DELAY_BETWEEN_HANDLES)
        elapsed = time.time() - start
        wait_for = max(0, CHECK_INTERVAL - elapsed)
        logger.info("Ciclo finalizado. Próxima verificação em %ss", int(wait_for))
        await asyncio.sleep(wait_for)

if __name__ == "__main__":
    asyncio.run(main_loop())

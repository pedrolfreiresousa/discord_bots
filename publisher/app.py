import os
from dotenv import load_dotenv
import asyncio
import logging
from typing import Dict, Any
from datetime import datetime, timezone
import sqlite3

import jwt
from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
import uvicorn

import discord
from discord.ext import commands

load_dotenv()

message_queue = asyncio.Queue()

# Carregamento de variaveis do ambiente
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
PUBLISHER_SECRET = os.getenv("PUBLISHER_SECRET", "verysecret")
JWT_ALGO = "HS256"
DATABASE_PATH = os.getenv("PUBLISHER_DB", "publisher.db")
POST_CHANNEL_ID = int(os.getenv("POST_CHANNEL_ID", "0"))

# Configuração de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("publisher")

# Início do Banco de dados, cria tabela para adicionar os links já postados
def init_db():
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS posted (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            title TEXT,
            received_at TEXT
        )
    """)
    conn.commit()
    return conn

# Chama o bd e mantem conexão ativa em tudo
db_conn = init_db()

# Cria a instância da API
app = FastAPI()

# Modelo para o corpo da requisição de novas publicações
class IncomingItem(BaseModel):
    source: str
    url: str
    title: str | None = None
    published_at: str | None = None

# Verifica se o PUBLISHER_SECRET está correto (via JWT)
async def verify_jwt(auth_header: str | None):
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    try:
        scheme, token = auth_header.split(" ")
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid auth scheme")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid Authorization header format")

    try:
        payload = jwt.decode(token, PUBLISHER_SECRET, algorithms=[JWT_ALGO])
        return payload
    except jwt.PyJWTError as e:
        logger.warning("JWT decode failed: %s", e)
        raise HTTPException(status_code=401, detail="Invalid token")

# Rota para receber novas publicações
@app.post("/incoming")
async def incoming(item: IncomingItem, request: Request, authorization: str | None = Header(None)):
    await verify_jwt(authorization)

    c = db_conn.cursor()
    try:
        c.execute(
            "INSERT INTO posted (source, url, title, received_at) VALUES (?, ?, ?, ?)",
            (item.source, item.url, item.title, datetime.now(timezone.utc).isoformat()),
        )
        db_conn.commit()
    except sqlite3.IntegrityError:
        # Se o link já existe no banco, ignora
        return {"status": "ignored", "reason": "duplicate"}

    # Adiciona a mensagem à fila para ser processada pelo bot
    payload = {"source": item.source, "url": item.url, "title": item.title}
    await message_queue.put(payload)
    return {"status": "posted"}

# Cria o bot com restrições (ler e enviar mensagens)
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Função de loop para enviar mensagens do queue para o Discord
async def message_sender_loop():
    # Espera até o bot estar completamente pronto e logado
    await bot.wait_until_ready()

    if POST_CHANNEL_ID == 0:
        logger.error("POST_CHANNEL_ID não configurado corretamente! O sender não será iniciado.")
        return

    while True:
        # 1. Pega o item da fila (bloqueia até que um item esteja disponível)
        payload = await message_queue.get()
        message = f"🔔 Novo link de **{payload['source']}**\n{payload.get('title') or payload['url']}\n{payload['url']}"
        
        # --- CORREÇÃO APLICADA AQUI ---
        # 2. Busca o canal de forma segura DENTRO do loop
        channel = None
        try:
            # Tenta pegar o canal do cache primeiro (mais rápido)
            channel = bot.get_channel(POST_CHANNEL_ID)
            
            # Se não estiver no cache (bot recém-logado), busca via API
            if channel is None:
                channel = await bot.fetch_channel(POST_CHANNEL_ID)
        except Exception:
            logger.exception("Falha ao encontrar/buscar o canal com ID: %s. Item ignorado.", POST_CHANNEL_ID)
            message_queue.task_done()
            continue # Pula este item e espera o próximo

        # 3. Loop de envio com tratamento de Rate Limit
        while True:
            try:
                await channel.send(message)
                logger.info("Mensagem enviada com sucesso para %s: %s", channel.name, payload["url"])
                break  # Sucesso, sai do loop de re-tentativa
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    # Rate Limit
                    retry = getattr(e, "retry_after", 5) # Obtém o tempo de espera ou 5s como fallback
                    logger.warning(f"Rate limit! Esperando {retry}s para reenviar...")
                    await asyncio.sleep(retry)
                    continue # Tenta novamente
                else:
                    # Outro erro HTTP do Discord (ex: permissões)
                    logger.exception("Erro HTTP ao enviar mensagem ao Discord:")
                    break
            except Exception:
                logger.exception("Erro desconhecido ao enviar mensagem ao Discord:")
                break
        
        message_queue.task_done() # Sinaliza que o item foi processado
# ------------------------------------

# Roda API e o bot simultaneamente.
async def start_services():
    # Configuração e inicialização do servidor FastAPI
    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")), log_level="info")
    server = uvicorn.Server(config)

    api_task = asyncio.create_task(server.serve())
    # Inicia a tarefa do bot (esta é uma tarefa de longo prazo que mantém o bot conectado)
    discord_task = asyncio.create_task(bot.start(DISCORD_TOKEN))
    # Inicia a tarefa do sender (que consome a fila de mensagens)
    sender_task = asyncio.create_task(message_sender_loop())

    # Aguarda todas as tarefas rodarem
    await asyncio.gather(api_task, discord_task, sender_task)


if __name__ == "__main__":
    asyncio.run(start_services())
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


# === CONFIGURAÇÕES ===
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_GUILD_ID = int(os.getenv("DISCORD_GUILD_ID", "0"))
PUBLISHER_SECRET = os.getenv("PUBLISHER_SECRET", "verysecret")
JWT_ALGO = "HS256"
DATABASE_PATH = os.getenv("PUBLISHER_DB", "publisher.db")
POST_CHANNEL_ID = int(os.getenv("POST_CHANNEL_ID", "0"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("publisher")


# === BANCO DE DADOS ===
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


db_conn = init_db()


# === FASTAPI ===
app = FastAPI()


class IncomingItem(BaseModel):
    source: str
    url: str
    title: str | None = None
    published_at: str | None = None


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
        return {"status": "ignored", "reason": "duplicate"}

    payload = {"source": item.source, "url": item.url, "title": item.title}
    await discord_bot_post(payload)
    return {"status": "posted"}


# === DISCORD BOT ===
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


async def discord_bot_post(payload: Dict[str, Any]):
    url = payload["url"]
    title = payload.get("title") or url
    channel_id = POST_CHANNEL_ID

    if channel_id == 0:
        logger.error("POST_CHANNEL_ID não configurado corretamente!")
        return

    try:
        channel = await bot.fetch_channel(channel_id)
    except Exception as e:
        logger.exception("Failed to fetch channel: %s", e)
        return

    try:
        message = f"🔔 Novo link de **{payload['source']}**\n{title}\n{url}"
        await channel.send(message)
        logger.info("Posted to Discord channel %s: %s", channel_id, url)
    except Exception:
        logger.exception("Failed to send message to Discord")


# === INICIALIZAÇÃO ===
async def start_services():
    config = uvicorn.Config(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")), log_level="info")
    server = uvicorn.Server(config)

    api_task = asyncio.create_task(server.serve())
    discord_task = asyncio.create_task(bot.start(DISCORD_TOKEN))

    await asyncio.gather(api_task, discord_task)


if __name__ == "__main__":
    asyncio.run(start_services())

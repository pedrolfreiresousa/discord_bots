import os
import time
import jwt
import httpx
import asyncio
from dotenv import load_dotenv

load_dotenv()

PUBLISHER_SECRET = os.getenv("PUBLISHER_SECRET", "verysecret")
PUBLISHER_API = "http://127.0.0.1:8000/incoming"
JWT_ALGO = "HS256"

def make_jwt_token(source_name: str, expire_seconds: int = 60):
    """Gera um token JWT válido para autenticação"""
    payload = {
        "iss": "test_script",
        "source": source_name,
        "iat": int(time.time()),
        "exp": int(time.time()) + expire_seconds
    }
    return jwt.encode(payload, PUBLISHER_SECRET, algorithm=JWT_ALGO)

async def send_post(source: str, url: str, title: str = None):
    """Envia um post para o Publisher"""
    
    token = make_jwt_token(source)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "source": source,
        "url": url,
        "title": title or url
    }
    
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            response = await client.post(PUBLISHER_API, json=payload, headers=headers)
            response.raise_for_status()
            print(f"✅ Post enviado com sucesso!")
            print(f"   Resposta: {response.json()}")
            return response.json()
        except Exception as e:
            print(f"❌ Erro ao enviar post: {e}")
            return None

if __name__ == "__main__":
    print("🚀 Testando envio de post para o Discord...\n")
    
    asyncio.run(send_post(
        source="Teste Manual",
        url="terceiro teste: https://x.com/SuiNetworkBr?t=Yw77BkjvO7kCBTEXPZKFMg&s=08",
        title="Teste para a palestra"
    ))

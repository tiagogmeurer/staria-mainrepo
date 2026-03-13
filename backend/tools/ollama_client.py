import os
import requests
from typing import Optional, Dict, Any

OLLAMA_BASE = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")

def ollama_chat(
    model: str,
    system: str,
    user: str,
    context: Optional[str] = None,
    temperature: float = 0.3,
    num_ctx: int = 4096,
) -> str:
    url = f"{OLLAMA_BASE}/api/chat"

    if context:
        user_content = (
            "Use o CONTEXTO abaixo como fonte de verdade. "
            "Não invente nada fora dele.\n\n"
            f"### CONTEXTO\n{context}\n\n"
            f"### PERGUNTA\n{user}"
        )
    else:
        user_content = user

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_content},
    ]

    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": temperature,
            "num_ctx": num_ctx,
        },
    }

    r = requests.post(url, json=payload, timeout=300)
    r.raise_for_status()
    data = r.json()
    return data["message"]["content"]
import os
import re
import shutil
import asyncio
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
import requests
from telegram import Update
from telegram.error import NetworkError
from telegram.ext import Application, MessageHandler, ContextTypes, filters

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ========= CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# API do StarIA (FastAPI)
STAR_API_BASE = os.getenv("STAR_API_BASE", "http://127.0.0.1:8000").strip()
STAR_USE_RAG_DEFAULT = os.getenv("STAR_USE_RAG", "true").lower() == "true"

# Raiz oficial do StarIA
DRIVE_ROOT = Path(
    os.getenv("STARIA_DRIVE_ROOT", r"G:\Drives compartilhados\STARMKT\StarIA")
).resolve()

# Pastas operacionais
CURRICULOS_DIR = DRIVE_ROOT / "curriculos"

# Script de indexação manual/rebuild
INDEXER_SCRIPT = Path(os.getenv("INDEXER_SCRIPT", r"C:\AI\backend\index_once.py"))

# Segurança opcional
ALLOWED_CHAT_IDS = os.getenv("ALLOWED_CHAT_IDS", "").strip()
ALLOWED = set()
if ALLOWED_CHAT_IDS:
    for x in ALLOWED_CHAT_IDS.split(","):
        x = x.strip()
        if x:
            ALLOWED.add(int(x))


# ========= HELPERS =========
def route_folder(user_text: str) -> Path | None:
    t = (user_text or "").strip().lower()

    if re.search(r"\bcurr[ií]cul", t) or "curriculos" in t or "currículos" in t:
        return CURRICULOS_DIR

    # futuros:
    # if "contratos" in t: return DRIVE_ROOT / "contratos"
    # if "relatorio" in t: return DRIVE_ROOT / "relatorio"
    # if "banco de talentos" in t: return DRIVE_ROOT / "banco_talentos"

    return None


def safe_filename(name: str) -> str:
    name = (name or "").strip().replace("\u0000", "")
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if len(name) > 180 else name


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def is_probably_question(text: str) -> bool:
    if not text:
        return False

    t = text.strip().lower()

    if t.startswith("/"):
        return False

    if "?" in t:
        return True

    return bool(
        re.match(
            r"^(quais|qual|me diga|liste|listar|mostre|mostrar|resuma|procure|busque|tem|existe|verifique|explique|defina|como|quem|onde|quando|por que|porque|fala|me fale)\b",
            t,
        )
    )


def looks_like_greeting(text: str) -> bool:
    if not text:
        return False

    t = text.strip().lower()

    greetings = [
        "opa",
        "epa",
        "oi",
        "olá",
        "ola",
        "hey",
        "eai",
        "e aí",
        "e ai",
        "e ae",
        "bom dia",
        "boa tarde",
        "boa noite",
        "como vai",
        "tudo bem",
        "fala aí",
        "fala ai",
        "fala ae",
        "salve",
    ]

    if t in greetings:
        return True

    return any(t.startswith(g) for g in greetings)


def call_star_api(question: str, use_rag: bool = True) -> dict:
    payload = {
        "question": question,
        "use_rag": use_rag,
    }

    print("[BOT] DEBUG PAYLOAD:", payload)

    r = requests.post(
        f"{STAR_API_BASE}/ask",
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    print("[BOT] JSON da API:", data)
    return data


def call_files_list(rel_path: str, exts=None, limit: int = 200) -> dict:
    payload = {"rel_path": rel_path, "exts": exts, "limit": limit}
    r = requests.post(f"{STAR_API_BASE}/files/list", json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def looks_like_curriculos_inventory(text: str) -> bool:
    s = (text or "").strip().lower()

    if not re.search(r"\bcurr[ií]cul", s):
        return False

    patterns = [
        r"\bquant(os|as)\b.*\bexist(em|e)?\b",
        r"\bquantidade\b",
        r"\bconta(r)?\b",
        r"\blistar?\b",
        r"\bquais\b.*\bexist(em|e)?\b",
        r"\btem\b.*\bcurr",
        r"\bver\b.*\bcurr",
        r"\bn[uú]mero\b.*\bcurr",
        r"\btotal\b.*\bcurr",
    ]
    return any(re.search(p, s) for p in patterns)


def list_curriculos_files(limit: int = 50) -> list[dict]:
    exts = {".pdf", ".docx", ".doc", ".txt", ".rtf"}
    if not CURRICULOS_DIR.exists():
        return []

    items = []
    for p in CURRICULOS_DIR.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            try:
                st = p.stat()
                items.append(
                    {
                        "name": p.name,
                        "path": str(p),
                        "size": st.st_size,
                        "mtime": int(st.st_mtime),
                    }
                )
            except Exception:
                items.append({"name": p.name, "path": str(p)})

    items.sort(key=lambda x: x.get("mtime", 0), reverse=True)
    return items[:limit]


# ========= HANDLERS =========
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if ALLOWED and chat_id not in ALLOWED:
        await update.message.reply_text("Acesso não autorizado para este bot.")
        return

    msg = update.message
    if not msg or not msg.document:
        return

    caption_or_text = (msg.caption or msg.text or "").strip()

    target_dir = route_folder(caption_or_text)
    if target_dir is None:
        await msg.reply_text(
            "Não entendi a pasta de destino.\n"
            "Ex: envie o arquivo com a legenda: 'Coloque na pasta de currículos'."
        )
        return

    target_dir.mkdir(parents=True, exist_ok=True)

    doc = msg.document
    file_name = safe_filename(doc.file_name or f"arquivo_{now_stamp()}")
    tmp_dir = Path(r"C:\AI\runtime\tmp_telegram")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_path = tmp_dir / f"{now_stamp()}__{file_name}"
    final_path = target_dir / file_name

    tg_file = await context.bot.get_file(doc.file_id)
    await tg_file.download_to_drive(custom_path=str(tmp_path))

    if final_path.exists():
        stem = Path(file_name).stem
        suf = Path(file_name).suffix
        n = 2
        while True:
            cand = target_dir / f"{stem}__{n}{suf}"
            if not cand.exists():
                final_path = cand
                break
            n += 1

    shutil.move(str(tmp_path), str(final_path))

    await msg.reply_text(f"✅ Recebido e salvo em:\n{final_path}")

    try:
        import subprocess
        import sys

        p = subprocess.run(
            [sys.executable, str(INDEXER_SCRIPT)],
            cwd=str(INDEXER_SCRIPT.parent),
            capture_output=True,
            text=True,
            timeout=600,
        )

        if p.returncode == 0:
            await msg.reply_text("📚 Indexação concluída.")
        else:
            err = (p.stderr or p.stdout or "").strip()
            await msg.reply_text(f"⚠️ Indexação falhou:\n{err[-1200:]}")
    except Exception as e:
        await msg.reply_text(f"⚠️ Não consegui rodar a indexação: {e}")


async def handle_text_only(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if ALLOWED and chat_id not in ALLOWED:
        try:
            await update.message.reply_text("Acesso não autorizado para este bot.")
        except Exception as send_err:
            print("[BOT] Falha ao enviar acesso não autorizado:", repr(send_err))
        return

    msg = update.message
    t = (msg.text or "").strip() if msg and msg.text else ""
    if not t:
        return

    print("[BOT] Mensagem recebida:", t)

    # 1) Inventário factual de currículos
    if looks_like_curriculos_inventory(t):
        try:
            files = list_curriculos_files(limit=80)
            total = len(files)

            if total == 0:
                await msg.reply_text(
                    "📂 Pasta de currículos está vazia.\n"
                    f"Destino: {CURRICULOS_DIR}"
                )
                return

            names = [f["name"] for f in files if f.get("name")]
            preview = names[:25]

            out = (
                f"📌 Currículos encontrados: {total}\n"
                f"📂 Pasta: {CURRICULOS_DIR}\n\n"
                "🗂 Últimos arquivos:\n- " + "\n- ".join(preview)
            )
            if total > len(preview):
                out += f"\n... (+{total - len(preview)})"

            await msg.reply_text(out[:3500])
            return

        except Exception as e:
            print("[BOT] Erro ao listar currículos:", repr(e))
            try:
                await msg.reply_text(f"⚠️ Erro ao listar currículos: {e}")
            except Exception as send_err:
                print("[BOT] Falha ao enviar erro ao Telegram:", repr(send_err))
            return

    # 2) Comando de pasta sem arquivo
    if route_folder(t) is not None and not is_probably_question(t):
        try:
            await msg.reply_text(
                "Entendi a pasta — agora me envie o arquivo (PDF/DOCX/XLSX) junto com essa legenda.\n"
                "Ex: envie o PDF com: 'Coloque na pasta de currículos'."
            )
        except Exception as send_err:
            print("[BOT] Falha ao enviar orientação de pasta:", repr(send_err))
        return

    # 3) Perguntas OU saudações vão para o StarIA
    if is_probably_question(t) or looks_like_greeting(t):
        try:
            print("[BOT] Chamando StarIA em:", STAR_API_BASE)

            data = await asyncio.to_thread(call_star_api, t, STAR_USE_RAG_DEFAULT)

            print("[BOT] Resposta recebida da API")

            answer = (data.get("answer") or "").strip() or "(sem resposta)"
            sources = data.get("sources") or []
            files = data.get("files") or []

            out = answer

            if files:
                names = [f.get("name") for f in files if isinstance(f, dict) and f.get("name")]
                if names:
                    out += "\n\n📎 Arquivos:\n- " + "\n- ".join(names[:20])

            if sources:
                out += "\n\n🧾 Fontes:\n- " + "\n- ".join(sources[:6])

            await msg.reply_text(out[:3500])
            return

        except Exception as e:
            print("[BOT] Erro ao consultar StarIA:", repr(e))
            try:
                await msg.reply_text(f"⚠️ Erro ao consultar StarIA: {e}")
            except Exception as send_err:
                print("[BOT] Falha ao enviar mensagem de erro ao Telegram:", repr(send_err))
            return

    # 4) Fallback final
    try:
        await msg.reply_text(
            "Me envie o arquivo (PDF/DOCX/XLSX) com uma legenda indicando a pasta.\n"
            "Ex: 'Coloque na pasta de currículos'.\n\n"
            "Ou faça uma pergunta (ex: 'Quais currículos existem?')."
        )
    except Exception as send_err:
        print("[BOT] Falha ao enviar mensagem padrão:", repr(send_err))


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error

    if isinstance(err, NetworkError):
        print("[BOT] NetworkError transitório (reconectando):", err)
        return

    print("[BOT] Exceção não tratada:", repr(err))


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Defina TELEGRAM_BOT_TOKEN no ambiente (.env)")

    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .pool_timeout(30)
        .build()
    )

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_only))
    app.add_error_handler(on_error)

    print("[ONLINE] StarIA running in polling mode")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
import os
import re
import shutil
from pathlib import Path
from datetime import datetime

import requests
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters

# ========= CONFIG =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# API do StarIA (FastAPI)
STAR_API_BASE = os.getenv("STAR_API_BASE", "http://127.0.0.1:8088").strip()
STAR_USE_RAG_DEFAULT = os.getenv("STAR_USE_RAG", "true").lower() == "true"

# Raiz do StarIA no drive compartilhado (G:)
DRIVE_ROOT = Path(os.getenv("DRIVE_SYNC_ROOT", r"G:\Drives compartilhados\STARMKT\StarIA")).resolve()

# Pastas (MVP: curriculos)
CURRICULOS_DIR = DRIVE_ROOT / "curriculos"

# Indexador (reaproveita seu script atual)
INDEXER_SCRIPT = Path(os.getenv("INDEXER_SCRIPT", r"C:\AI\backend\index_inbox.py"))

# (Opcional) restringe quem pode usar o bot
ALLOWED_CHAT_IDS = os.getenv("ALLOWED_CHAT_IDS", "").strip()  # ex: "123,456"
ALLOWED = set()
if ALLOWED_CHAT_IDS:
    for x in ALLOWED_CHAT_IDS.split(","):
        x = x.strip()
        if x:
            ALLOWED.add(int(x))

# ========= ROUTING (texto → pasta) =========
def route_folder(user_text: str) -> Path | None:
    """
    Decide a pasta com base no texto/caption.
    MVP: só currículos. Depois expandimos com mais regras.
    """
    t = (user_text or "").strip().lower()

    if re.search(r"\bcurr[ií]cul", t) or "curriculos" in t:
        return CURRICULOS_DIR

    # futuros:
    # if "contratos" in t: return DRIVE_ROOT / "contratos"
    # if "apresenta" in t: return DRIVE_ROOT / "apresentacoes"
    # if "rh" in t: return DRIVE_ROOT / "relatorios_rh"
    # if "finance" in t: return DRIVE_ROOT / "relatorios_financeiros"

    return None

def safe_filename(name: str) -> str:
    name = (name or "").strip().replace("\u0000", "")
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)  # windows-safe
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if len(name) > 180 else name

def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def is_probably_question(text: str) -> bool:
    """
    Heurística simples: se tem "?" ou começa com verbos típicos de pedido/pergunta.
    """
    if not text:
        return False
    t = text.strip().lower()
    if t.startswith("/"):
        return False
    if "?" in t:
        return True
    return bool(re.match(r"^(quais|qual|me diga|liste|mostre|resuma|procure|busque|tem|existe|verifique)\b", t))

def call_star_api(question: str, use_rag: bool = True) -> dict:
    r = requests.post(
        f"{STAR_API_BASE}/ask",
        json={"question": question, "use_rag": use_rag},
        timeout=180,
    )
    r.raise_for_status()
    return r.json()


def looks_like_curriculos_inventory(text: str) -> bool:
    t = (text or "").strip().lower()
    return bool(re.search(r"\b(quantos|quais)\b.*\bcurr[ií]cul", t))

def call_files_list(rel_path: str, exts=None, limit: int = 200) -> dict:
    payload = {"rel_path": rel_path, "exts": exts, "limit": limit}
    r = requests.post(f"{STAR_API_BASE}/files/list", json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def format_curriculos_inventory(files: list[dict]) -> str:
    # filtra e ordena por nome
    items = [f for f in (files or []) if isinstance(f, dict) and f.get("name")]
    items.sort(key=lambda x: x["name"].lower())

    total = len(items)
    if total == 0:
        return "📭 Não encontrei currículos na pasta `curriculos`."

    names = [f["name"] for f in items[:30]]
    extra = total - len(names)

    out = f"📌 Currículos encontrados: **{total}**\n\n"
    out += "📎 Arquivos:\n- " + "\n- ".join(names)
    if extra > 0:
        out += f"\n- ... (+{extra} arquivos)"
    return out


# ========= HANDLERS =========
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Segurança (opcional)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if ALLOWED and chat_id not in ALLOWED:
        await update.message.reply_text("Acesso não autorizado para este bot.")
        return

    msg = update.message
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

    # Download
    tg_file = await context.bot.get_file(doc.file_id)
    await tg_file.download_to_drive(custom_path=str(tmp_path))

    # Move atomicamente / versiona se já existir
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

    # Dispara indexação (MVP)
    try:
        import subprocess, sys
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



def looks_like_curriculos_inventory(t: str) -> bool:
    s = (t or "").strip().lower()

    # precisa mencionar curriculo
    if not re.search(r"\bcurr[ií]cul", s):
        return False

    # intenções comuns
    patterns = [
        r"\bquant(os|as)\b.*\bexist(em|e)?\b",
        r"\bquantidade\b",
        r"\bconta(r)?\b",
        r"\blistar?\b",
        r"\bquais\b.*\bexist(em|e)?\b",
        r"\btem\b.*\bcurr",
        r"\bver\b.*\bcurr",
    ]
    return any(re.search(p, s) for p in patterns)

def list_curriculos_files(limit: int = 50) -> list[dict]:
    # lista apenas arquivos “de currículo” comuns (ajusta se quiser)
    exts = {".pdf", ".docx", ".doc", ".txt", ".rtf"}
    if not CURRICULOS_DIR.exists():
        return []

    items = []
    for p in CURRICULOS_DIR.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            try:
                st = p.stat()
                items.append({
                    "name": p.name,
                    "path": str(p),
                    "size": st.st_size,
                    "mtime": int(st.st_mtime),
                })
            except Exception:
                items.append({"name": p.name, "path": str(p)})

    # ordena por mais recente (mtime desc)
    items.sort(key=lambda x: x.get("mtime", 0), reverse=True)
    return items[:limit]





async def handle_text_only(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Segurança (opcional)
    chat_id = update.effective_chat.id if update.effective_chat else None
    if ALLOWED and chat_id not in ALLOWED:
        await update.message.reply_text("Acesso não autorizado para este bot.")
        return

    msg = update.message
    t = (msg.text or "").strip()
    if not t:
        return

    # ✅ 1) INVENTÁRIO DE CURRÍCULOS (não usa LLM)
    if looks_like_curriculos_inventory(t):
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

    # Se o texto parece um "comando de pasta" mas sem arquivo, orienta.
    if route_folder(t) is not None and not is_probably_question(t):
        await msg.reply_text(
            "Entendi a pasta — agora me envie o arquivo (PDF/DOCX/XLSX) junto com essa legenda.\n"
            "Ex: envie o PDF com: 'Coloque na pasta de currículos'."
        )
        return

    # Se é pergunta/pedido, consulta a API (/ask) e responde.
    if is_probably_question(t):
        try:
            data = call_star_api(t, use_rag=STAR_USE_RAG_DEFAULT)
            answer = (data.get("answer") or "").strip() or "(sem resposta)"
            sources = data.get("sources") or []
            files = data.get("files") or []

            out = answer

            # Se o backend devolver lista de arquivos (como no Swagger), mostra
            if files:
                names = [f.get("name") for f in files if isinstance(f, dict) and f.get("name")]
                if names:
                    out += "\n\n📎 Arquivos:\n- " + "\n- ".join(names[:20])

            if sources:
                out += "\n\n🧾 Fontes:\n- " + "\n- ".join(sources[:6])

            await msg.reply_text(out[:3500])
            return
        except Exception as e:
            await msg.reply_text(f"⚠️ Erro ao consultar StarIA: {e}")
            return

    # Caso não seja pergunta: padrão de orientação
    await msg.reply_text(
        "Me envie o arquivo (PDF/DOCX/XLSX) com uma legenda indicando a pasta.\n"
        "Ex: 'Coloque na pasta de currículos'.\n\n"
        "Ou faça uma pergunta (ex: 'Quais currículos existem?')."
    )

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Defina TELEGRAM_BOT_TOKEN no ambiente (.env)")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_only))

    print("StarIA Telegram Bot (polling) rodando...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
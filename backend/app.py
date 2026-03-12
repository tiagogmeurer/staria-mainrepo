import os
import re
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from tools.ollama_client import ollama_chat
from tools.spreadsheets import read_excel_preview, compute_basic_stats
from tools.drive_sync import list_files
from tools.automations import create_folder, write_text_report
from rag.retriever import retrieve

APP_NAME = "StarIA"
MODEL_DEFAULT = os.getenv("STAR_OLLAMA_MODEL", "star-llama")

# Raiz do Drive
DRIVE_ROOT = os.getenv("DRIVE_SYNC_ROOT", r"G:\Drives compartilhados\STARMKT\StarIA")

SYSTEM_PROMPT = """Você é o StarIA, cérebro corporativo da StarMKT.
Regras:
- Seja objetivo, orientado a ação e seguro.
- Se não tiver contexto suficiente, peça o mínimo necessário mas não atue na suposição.
- Quando usar CONTEXT, trate como fonte de verdade e cite o caminho do arquivo quando possível.
- Nunca invente números/valores de documentos: se não estiver no contexto, diga que não está.
- Quando houver CONTEXTO fornecido pelo sistema, você DEVE responder usando esse conteúdo.
- Não recuse pedidos de resumo/extração quando o texto já estiver no CONTEXTO.
"""

app = FastAPI(title=APP_NAME)


class AskRequest(BaseModel):
    question: str
    model: str | None = None
    use_rag: bool = True


@app.get("/healthz")
def healthz():
    return {"ok": True, "app": APP_NAME}


def _safe_base() -> Path:
    return Path(DRIVE_ROOT)


def _curriculos_base() -> Path:
    return _safe_base() / "curriculos"


def _list_curriculos(limit: int = 300) -> list[dict]:
    """
    Lista arquivos reais da pasta curriculos.
    """
    base = _curriculos_base()
    if not base.exists():
        return []

    exts = {".pdf", ".docx", ".doc", ".txt", ".rtf"}
    out: list[dict] = []

    for p in base.rglob("*"):
        if not p.is_file():
            continue

        ext = p.suffix.lower()
        if ext not in exts:
            continue

        try:
            st = p.stat()
            out.append(
                {
                    "name": p.name,
                    "path": str(p),
                    "ext": ext,
                    "size": st.st_size,
                    "mtime": int(st.st_mtime),
                }
            )
        except Exception:
            out.append(
                {
                    "name": p.name,
                    "path": str(p),
                    "ext": ext,
                }
            )

        if len(out) >= limit:
            break

    out.sort(key=lambda x: x.get("name", "").lower())
    return out


def _is_curriculos_scope(q: str) -> bool:
    q = q.strip().lower()
    return bool(
        re.search(r"\bcurr[ií]cul", q)
        or "curriculo" in q
        or "currículos" in q
        or "curriculos" in q
    )


def _is_list_curriculos_intent(q: str) -> bool:
    q = q.strip().lower()
    return _is_curriculos_scope(q) and bool(
        re.search(r"\b(quais|liste|listar|lista|mostre|mostrar|ver|nomes)\b", q)
    )


def _is_count_curriculos_intent(q: str) -> bool:
    q = q.strip().lower()
    return _is_curriculos_scope(q) and bool(
        re.search(r"\b(quantos|quantas|qtd|quantidade|total|n[uú]mero)\b", q)
    )


@app.post("/ask")
def ask(req: AskRequest):
    model = req.model or MODEL_DEFAULT
    question = (req.question or "").strip()

    # 1) INVENTÁRIO: contagem real de currículos
    if _is_count_curriculos_intent(question):
        files = _list_curriculos(limit=1000)
        base_path = str(_curriculos_base())

        return {
            "answer": f"Existem {len(files)} currículo(s) na pasta de currículos.",
            "sources": [base_path],
            "files": files,
        }

    # 2) INVENTÁRIO: listagem real de currículos
    if _is_list_curriculos_intent(question):
        files = _list_curriculos(limit=300)
        base_path = str(_curriculos_base())

        if not files:
            return {
                "answer": "Ainda não há currículos na pasta de currículos.",
                "sources": [base_path],
                "files": [],
            }

        lines = [f"- {f['name']}" for f in files]
        return {
            "answer": f"Currículos encontrados ({len(files)}):\n" + "\n".join(lines),
            "sources": [base_path],
            "files": files,
        }

    # 3) RAG: perguntas semânticas sobre currículos devem buscar só em curriculos
    context = None
    sources: list[str] = []

    if req.use_rag:
        where = {"folder": "curriculos"} if _is_curriculos_scope(question) else None
        hits = retrieve(question, k=6, where=where)

        if hits:
            parts = []
            for h in hits:
                meta = h.get("meta") or {}
                path = meta.get("path", h.get("id"))
                chunk = meta.get("chunk", "?")
                sources.append(path)
                parts.append(f"[SOURCE: {path} | chunk {chunk}]\n{h.get('doc', '')}")

            seen = set()
            sources = [s for s in sources if not (s in seen or seen.add(s))]
            context = "\n\n---\n\n".join(parts)

    answer = ollama_chat(
        model=model,
        system=SYSTEM_PROMPT,
        user=question,
        context=context,
        temperature=0.3,
        num_ctx=4096,
    )

    return {"answer": answer, "sources": sources}


class ListFilesRequest(BaseModel):
    rel_path: str = ""
    exts: list[str] | None = None
    limit: int = 200


@app.post("/files/list")
def files_list(req: ListFilesRequest):
    try:
        files = list_files(req.rel_path, req.exts, req.limit)
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class ExcelPreviewRequest(BaseModel):
    path: str
    sheet_name: str | int | None = None
    n: int = 30


@app.post("/excel/preview")
def excel_preview(req: ExcelPreviewRequest):
    try:
        return read_excel_preview(req.path, req.sheet_name, req.n)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class ExcelStatsRequest(BaseModel):
    path: str
    sheet_name: str | int | None = None


@app.post("/excel/stats")
def excel_stats(req: ExcelStatsRequest):
    try:
        return compute_basic_stats(req.path, req.sheet_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class AutomationCreateFolder(BaseModel):
    path: str


@app.post("/automation/create-folder")
def automation_create_folder(req: AutomationCreateFolder):
    try:
        r = create_folder(req.path)
        return {"ok": r.ok, "message": r.message, "data": r.data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class AutomationWriteReport(BaseModel):
    path: str
    content: str


@app.post("/automation/write-report")
def automation_write_report(req: AutomationWriteReport):
    try:
        r = write_text_report(req.path, req.content)
        return {"ok": r.ok, "message": r.message, "data": r.data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
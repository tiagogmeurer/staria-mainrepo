print("### STARIA APP.PY CARREGADO ###")

import os
import re
import random
from pathlib import Path
from collections import defaultdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from tools.ollama_client import ollama_chat
from tools.spreadsheets import read_excel_preview, compute_basic_stats
from tools.drive_sync import list_files
from tools.automations import create_folder, write_text_report
from rag.retriever import retrieve

APP_NAME = "StarIA"
MODEL_DEFAULT = os.getenv("STAR_OLLAMA_MODEL", "star-llama")

# Raiz do Drive do StarIA (ambiente de teste isolado)
STARIA_DRIVE_ROOT_ENV = os.getenv("STARIA_DRIVE_ROOT")
DRIVE_SYNC_ROOT_ENV = os.getenv("DRIVE_SYNC_ROOT")

DRIVE_ROOT = STARIA_DRIVE_ROOT_ENV or r"G:\Drives compartilhados\STARMKT\StarIA"

SYSTEM_PROMPT = """Você é o StarIA, cérebro corporativo da StarMKT.
Regras:
- Seja objetivo, orientado a ação e seguro.
- Se não tiver contexto suficiente, peça o mínimo necessário mas não atue na suposição.
- Nunca invente números/valores de documentos: se não estiver no contexto, diga que não está.
- Quando houver CONTEXTO fornecido pelo sistema, você DEVE responder usando esse conteúdo.
- Não recuse pedidos de resumo/extração quando o texto já estiver no CONTEXTO.
"""

STRICT_RAG_SYSTEM_PROMPT = """
Você é um assistente factual corporativo baseado EXCLUSIVAMENTE no contexto documental fornecido.

REGRAS OBRIGATÓRIAS:
1. Use somente fatos explicitamente presentes no CONTEXTO.
2. Nunca invente números, nomes, datas, totais ou conclusões.
3. Nunca estime, arredonde, complete lacunas ou use conhecimento externo.
4. Se a resposta não estiver explícita no CONTEXTO, responda EXATAMENTE:
Não encontrei essa informação explicitamente nos documentos disponíveis.
5. Se houver valor aproximado no contexto, preserve a formulação original.
   Exemplo: se o texto diz "beira cem funcionários", NÃO converta para 100 ou 10.
6. Sempre ancore a resposta na evidência textual.
7. Nunca cite arquivos que não estejam no CONTEXTO recebido.
8. Nunca diga que encontrou informação em "algum arquivo não especificado".

FORMATO DE RESPOSTA:
- Se encontrou a resposta no contexto:
Resposta: <resposta curta e fiel ao texto>
Evidência: "<trecho literal mínimo que sustenta a resposta>"
Fonte: <caminho da fonte exatamente como aparece no contexto>

- Se NÃO encontrou:
Não encontrei essa informação explicitamente nos documentos disponíveis.
""".strip()

app = FastAPI(title=APP_NAME)


def _safe_base() -> Path:
    return Path(DRIVE_ROOT)


def _curriculos_base() -> Path:
    return _safe_base() / "curriculos"


@app.on_event("startup")
def startup_check():
    print("[StarIA] STARIA_DRIVE_ROOT env =", STARIA_DRIVE_ROOT_ENV)
    print("[StarIA] DRIVE_SYNC_ROOT env =", DRIVE_SYNC_ROOT_ENV)
    print("[StarIA] DRIVE_ROOT =", DRIVE_ROOT)
    print("[StarIA] CURRICULOS_PATH =", str(_curriculos_base()))
    print("[StarIA] CURRICULOS_EXISTS =", _curriculos_base().exists())


class AskRequest(BaseModel):
    question: str
    model: str | None = None
    use_rag: bool = True


@app.get("/health")
def health():
    return {
        "service": "StarNodeV0",
        "status": "online"
    }


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("\\", "/")


def _user_wants_sources(q: str) -> bool:
    q = (q or "").strip().lower()
    triggers = [
        "mostre as fontes",
        "mostrar fontes",
        "quais fontes",
        "fonte",
        "fontes",
        "source",
        "sources",
        "mostre o caminho dos arquivos",
        "mostrar o caminho dos arquivos",
        "caminho dos arquivos",
        "caminho do arquivo",
        "caminhos",
    ]
    return any(t in q for t in triggers)


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


def _is_greeting(q: str) -> bool:
    q = (q or "").strip().lower()

    greetings = [
        "opa",
        "oi",
        "olá",
        "ola",
        "hey",
        "eai",
        "e aí",
        "e ai",
        "bom dia",
        "boa tarde",
        "boa noite",
        "como vai",
        "tudo bem",
        "fala aí",
        "fala ai",
        "salve",
    ]

    if q in greetings:
        return True

    return any(q.startswith(g) for g in greetings)




    


def _get_greeting_reply() -> str:
    options = [
        "Olá! Sou a StarIA, a inteligência artificial da StarMKT. Como posso ajudar você hoje?",
        "Opa! Fala aí — sou a StarIA. Como posso te ajudar agora?",
        "Hey! StarIA na área. Como posso ser útil?",
        "Oi! Sou a StarIA, assistente da StarMKT. Me diga como posso ajudar.",
        "Olá! Tudo certo? Sou a StarIA. O que você precisa agora?",
    ]
    return random.choice(options)


def _extract_forced_file(q: str) -> str | None:
    ql = (q or "").lower()

    markers = [
        "use apenas o arquivo ",
        "usar apenas o arquivo ",
        "somente o arquivo ",
        "apenas o arquivo ",
    ]

    for marker in markers:
        idx = ql.find(marker)
        if idx >= 0:
            tail = q[idx + len(marker):].strip()

            for sep in [":", ";", ",", "\n", "?", " qual", " quant", " responda", " diga"]:
                pos = tail.lower().find(sep)
                if pos > 0:
                    tail = tail[:pos].strip()

            tail = tail.strip(' "\'“”‘’')
            if tail:
                return tail
    return None


def _hit_path(hit: dict) -> str:
    meta = hit.get("meta") or {}
    return str(meta.get("path", hit.get("id", "")) or "")


def _basename(path: str) -> str:
    p = (path or "").replace("\\", "/").rstrip("/")
    if "/" in p:
        return p.rsplit("/", 1)[-1]
    return p


def _filter_hits_by_forced_file(hits: list[dict], forced_file: str) -> list[dict]:
    if not forced_file:
        return hits

    forced_norm = _norm(forced_file)
    out = []

    for h in hits:
        path = _hit_path(h)
        base = _basename(path)

        path_norm = _norm(path)
        base_norm = _norm(base)

        if forced_norm == path_norm or forced_norm == base_norm or forced_norm in path_norm:
            out.append(h)

    return out


def _filter_hits_for_curriculos_scope(hits: list[dict]) -> list[dict]:
    out = []
    for h in hits:
        meta = h.get("meta") or {}
        folder = str(meta.get("folder", "") or "").strip().lower()
        path = _hit_path(h).lower().replace("\\", "/")

        if folder == "curriculos" or "/curriculos/" in path or path.endswith("/curriculos") or "curriculos" in path:
            out.append(h)

    return out


def _dedupe_sources(paths: list[str]) -> list[str]:
    seen = set()
    out = []
    for p in paths:
        key = _norm(p)
        if key and key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _candidate_name_from_path(path: str) -> str:
    base = _basename(path)
    stem = Path(base).stem
    stem = re.sub(r"[_\-]+", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem or base


def _clean_snippet(text: str, max_len: int = 220) -> str:
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if len(t) <= max_len:
        return t
    return t[:max_len].rstrip() + "..."


def _looks_like_talent_search_intent(q: str) -> bool:
    ql = (q or "").strip().lower()

    triggers = [
        "candidato",
        "candidatos",
        "talento",
        "talentos",
        "perfil",
        "perfis",
        "vaga",
        "shortlist",
        "banco de talentos",
        "quem tem",
        "quem possui",
        "quem conhece",
        "experiência em",
        "experiencia em",
        "com experiência",
        "com experiencia",
        "que trabalhou com",
        "adequado para",
        "adequada para",
        "aderente à vaga",
        "aderente a vaga",
        "alguém com",
        "alguem com",
        "profissional com",
    ]

    if any(t in ql for t in triggers):
        return True

    # Atalho útil para RH mesmo sem citar "currículo"
    if bool(re.search(r"\b(photoshop|excel|crm|varejo|atendimento|social media|tráfego pago|trafego pago|designer|design|marketing|comercial|rh|recrutamento)\b", ql)):
        return True

    return False


def _build_talent_bank_answer(question: str, hits: list[dict], max_candidates: int = 5) -> tuple[str, list[str], list[dict]]:
    """
    Banco de Talentos determinístico:
    agrupa hits por currículo e devolve resposta com evidências literais.
    """
    grouped: dict[str, dict] = {}

    for h in hits:
        path = _hit_path(h)
        if not path:
            continue

        doc = (h.get("doc") or "").strip()
        if not doc:
            continue

        meta = h.get("meta") or {}
        chunk = meta.get("chunk", "?")

        if path not in grouped:
            grouped[path] = {
                "path": path,
                "name": _candidate_name_from_path(path),
                "evidences": [],
                "chunks": [],
                "score": 0,
            }

        snippet = _clean_snippet(doc)
        if snippet and snippet not in grouped[path]["evidences"]:
            grouped[path]["evidences"].append(snippet)

        grouped[path]["chunks"].append(chunk)
        grouped[path]["score"] += 1

    items = sorted(
        grouped.values(),
        key=lambda x: (-x["score"], x["name"].lower())
    )

    items = items[:max_candidates]
    sources = [item["path"] for item in items]

    if not items:
        answer = "Não encontrei candidatos com evidências explícitas para esse critério nos currículos disponíveis."
        return answer, sources, items

    lines = [f"Encontrei {len(items)} candidato(s) com indícios relevantes nos currículos para esse critério:"]
    for idx, item in enumerate(items, 1):
        lines.append(f"{idx}. {item['name']}")
        if item["evidences"]:
            lines.append(f"   Evidência: \"{item['evidences'][0]}\"")
        lines.append(f"   Fonte: {item['path']}")
        if len(item["evidences"]) > 1:
            lines.append(f"   Evidência adicional: \"{item['evidences'][1]}\"")

    answer = "\n".join(lines)
    return answer, sources, items



def _is_identity_question(q: str) -> bool:
    ql = (q or "").strip().lower()
    triggers = [
        "quem é você",
        "quem é vc",
        "o que você é",
        "oq vc é",
        "qual seu nome",
        "qual é seu nome",
        "você é quem",
        "vc é quem",
        "quem é a staria",
        "o que é a staria",
    ]
    return any(t in ql for t in triggers)


def _is_company_question(q: str) -> bool:
    ql = (q or "").strip().lower()
    return (
        "starmkt" in ql
        and any(
            t in ql
            for t in [
                "o que é",
                "quem é",
                "qual é",
                "me fale sobre",
                "fale sobre",
                "explique",
                "do que se trata",
                "qual a missão",
                "qual é a missão",
            ]
        )
    )


@app.post("/ask")
def ask(req: AskRequest):
    model = req.model or MODEL_DEFAULT
    question = (req.question or "").strip()

    # 0-A) Identidade do assistente (não depende de RAG)
    if _is_identity_question(question):
        answer = ollama_chat(
            model=model,
            system=SYSTEM_PROMPT,
            user=(
                "Responda de forma curta, natural e institucional. "
                "Explique quem é a StarIA dentro da StarMKT. "
                "Não diga que não encontrou documentos. "
                "Fale como assistente corporativa da StarMKT."
            ),
            context=None,
            temperature=0.2,
            num_ctx=2048,
        ).strip()

        return {"answer": answer}

    # 0-B) Pergunta institucional sobre a StarMKT
    if _is_company_question(question):
        institutional_context = (
            "A StarMKT é uma empresa focada em campanhas promocionais impactantes "
            "para o setor de atacarejo, com objetivo de impulsionar crescimento de negócios "
            "e engajamento do cliente. O lema da empresa é 'Vender mais e melhor.'"
        )

        answer = ollama_chat(
            model=model,
            system=(
                "Você é o StarIA, cérebro corporativo da StarMKT. "
                "Responda com base no contexto institucional fornecido. "
                "Seja breve, claro e natural."
            ),
            user=question,
            context=institutional_context,
            temperature=0.2,
            num_ctx=2048,
        ).strip()

        return {"answer": answer}

    # 0) Saudação simples
    if _is_greeting(question):
        return {"answer": _get_greeting_reply()}

    # 1) INVENTÁRIO: contagem real de currículos
    if _is_count_curriculos_intent(question):
        files = _list_curriculos(limit=1000)
        base_path = str(_curriculos_base())

        response = {
            "answer": f"Existem {len(files)} currículo(s) na pasta de currículos.",
            "files": files,
        }
        if _user_wants_sources(question):
            response["sources"] = [base_path]
        return response

    # 2) INVENTÁRIO: listagem real de currículos
    if _is_list_curriculos_intent(question):
        files = _list_curriculos(limit=300)
        base_path = str(_curriculos_base())

        if not files:
            response = {
                "answer": "Ainda não há currículos na pasta de currículos.",
                "files": [],
            }
            if _user_wants_sources(question):
                response["sources"] = [base_path]
            return response

        lines = [f"- {f['name']}" for f in files]
        response = {
            "answer": f"Currículos encontrados ({len(files)}):\n" + "\n".join(lines),
            "files": files,
        }
        if _user_wants_sources(question):
            response["sources"] = [base_path]
        return response

    # 3) BANCO DE TALENTOS: busca por perfil/habilidade/cargo com evidência
    if req.use_rag and _looks_like_talent_search_intent(question):
        hits = retrieve(question, k=12, where={"folder": "curriculos"}) or []
        hits = _filter_hits_for_curriculos_scope(hits)

        print("\n[DEBUG TALENT BANK] question =", question)
        print("[DEBUG TALENT BANK] hits count =", len(hits))

        if hits:
            for i, h in enumerate(hits, 1):
                meta = h.get("meta") or {}
                print(f"[DEBUG TALENT BANK] hit {i} path =", meta.get("path"))
                print(f"[DEBUG TALENT BANK] hit {i} chunk =", meta.get("chunk"))
                print(f"[DEBUG TALENT BANK] hit {i} doc preview =", (h.get("doc") or "")[:300])

        answer, sources, ranked_items = _build_talent_bank_answer(question, hits, max_candidates=5)

        response = {
            "answer": answer,
            "matches": [
                {
                    "candidate": item["name"],
                    "path": item["path"],
                    "evidences": item["evidences"][:2],
                    "score": item["score"],
                }
                for item in ranked_items
            ],
        }

        if _user_wants_sources(question):
            response["sources"] = sources

        return response

    # 4) RAG factual controlado
    context = None
    sources: list[str] = []
    forced_file = _extract_forced_file(question)

    if req.use_rag:
        where = {"folder": "curriculos"} if _is_curriculos_scope(question) else None
        hits = retrieve(question, k=6, where=where) or []

        print("\n[DEBUG] question =", question)
        print("[DEBUG] forced_file =", forced_file)
        print("[DEBUG] initial hits count =", len(hits))

        if _is_curriculos_scope(question):
            hits = _filter_hits_for_curriculos_scope(hits)
            print("[DEBUG] hits after curriculos filter =", len(hits))

        if forced_file:
            hits = _filter_hits_by_forced_file(hits, forced_file)
            print("[DEBUG] hits after forced_file filter =", len(hits))

        if hits:
            for i, h in enumerate(hits, 1):
                meta = h.get("meta") or {}
                print(f"[DEBUG] hit {i} path =", meta.get("path"))
                print(f"[DEBUG] hit {i} chunk =", meta.get("chunk"))
                print(f"[DEBUG] hit {i} doc preview =", (h.get("doc") or "")[:300])

        if hits:
            parts = []
            collected_sources = []

            for h in hits:
                meta = h.get("meta") or {}
                path = meta.get("path", h.get("id"))
                chunk = meta.get("chunk", "?")
                doc = (h.get("doc") or "").strip()

                if not doc:
                    continue

                collected_sources.append(path)
                parts.append(f"[FONTE: {path} | chunk {chunk}]\n{doc}")

            sources = _dedupe_sources(collected_sources)
            context = "\n\n---\n\n".join(parts) if parts else None

            print("[DEBUG] context preview =", (context or "")[:1200])

    # 5) Sem contexto válido
    if req.use_rag and not context:
        non_documental = (
            not _is_curriculos_scope(question)
            and not _looks_like_talent_search_intent(question)
            and not _user_wants_sources(question)
        )

        if non_documental:
            answer = ollama_chat(
                model=model,
                system=SYSTEM_PROMPT,
                user=question,
                context=None,
                temperature=0.2,
                num_ctx=2048,
            ).strip()

            return {"answer": answer}

        response = {
            "answer": "Não encontrei essa informação explicitamente nos documentos disponíveis."
        }
        if _user_wants_sources(question):
            response["sources"] = sources
        return response

    # 6) Resposta factual rígida
    answer = ollama_chat(
        model=model,
        system=STRICT_RAG_SYSTEM_PROMPT,
        user=question,
        context=context,
        temperature=0.0,
        num_ctx=4096,
    ).strip()

    # 7) Pós-blindagem
    if not answer:
        answer = "Não encontrei essa informação explicitamente nos documentos disponíveis."

    lower_answer = answer.lower()
    has_not_found = "não encontrei essa informação explicitamente nos documentos disponíveis." in lower_answer
    has_evidence = "evidência:" in lower_answer
    has_source = "fonte:" in lower_answer

    if req.use_rag and not has_not_found and (not has_evidence or not has_source):
        answer = "Não encontrei essa informação explicitamente nos documentos disponíveis."

    response = {"answer": answer}
    if _user_wants_sources(question):
        response["sources"] = sources

    return response

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


@app.get("/debug/curriculos-path")
def debug_curriculos_path():
    base = _curriculos_base()

    print("[StarIA] Listing curriculos from:", str(base))
    print("[StarIA] Exists:", base.exists())

    files = []

    if base.exists():
        try:
            for p in base.rglob("*"):
                if p.is_file():
                    files.append(str(p))
                    if len(files) >= 20:
                        break
        except Exception as e:
            return {
                "drive_root": str(_safe_base()),
                "curriculos_path": str(base),
                "exists": base.exists(),
                "error": str(e),
            }

    print("[StarIA] Sample files found:", len(files))

    return {
        "drive_root": str(_safe_base()),
        "curriculos_path": str(base),
        "exists": base.exists(),
        "sample_files": files,
    }
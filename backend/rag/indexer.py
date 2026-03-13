import os
from pathlib import Path

import chromadb
from chromadb.config import Settings

CHROMA_DIR = os.getenv("CHROMA_DIR", r"C:\AI\vector_db\chroma")
COLLECTION = os.getenv("CHROMA_COLLECTION", "star_docs")


def get_client():
    return chromadb.PersistentClient(
        path=CHROMA_DIR,
        settings=Settings(anonymized_telemetry=False),
    )


def upsert_text(doc_id: str, text: str, metadata: dict):
    client = get_client()
    col = client.get_or_create_collection(COLLECTION)
    col.upsert(ids=[doc_id], documents=[text], metadatas=[metadata])


def _detect_folder_tag(path_obj: Path) -> str:
    parts = [p.lower() for p in path_obj.parts]
    if "curriculos" in parts or "currículos" in parts:
        return "curriculos"
    return "geral"


def index_txt_folder(folder: str, limit: int = 500):
    folderp = Path(folder)
    count = 0

    for p in folderp.rglob("*.txt"):
        text = p.read_text(encoding="utf-8", errors="ignore")

        metadata = {
            "path": str(p),
            "filename": p.name,
            "folder": _detect_folder_tag(p),
            "doc_type": "txt",
            "chunk": 1,
        }

        alias = ""
        if p.name.lower() == "ctx_company.txt":
            alias = "empresa StarMKT contexto institucional companhia organização"
        elif p.name.lower() == "ctx_clients.txt":
            alias = "clientes contas carteira de clientes StarMKT"
        elif p.name.lower() == "ctx_services.txt":
            alias = "serviços soluções entregas escopo StarMKT"
        elif p.name.lower() == "ctx_processes.txt":
            alias = "processos fluxo operação etapas StarMKT"
        elif p.name.lower() == "ctx_glossary.txt":
            alias = "glossário termos definições conceitos StarMKT"
        elif p.name.lower() == "ctx_staria.txt":
            alias = "StarIA inteligência artificial agente assistente corporativo"
        elif p.name.lower() == "ctx_operational_rules.txt":
            alias = "regras operacionais comportamento instruções políticas do agente"

        index_text = f"""Arquivo: {p.name}
Nome sem extensão: {p.stem}
Aliases semânticos: {alias}
Conteúdo:
{text}
"""

        upsert_text(
            doc_id=str(p),
            text=index_text[:20000],
            metadata=metadata,
        )

        count += 1
        if count >= limit:
            break

    return count
from pathlib import Path
from rag.indexer import upsert_text
import os

import pandas as pd
from pathlib import Path
import docx
import pdfplumber

DRIVE_ROOT = Path(os.getenv("STARIA_DRIVE_ROOT", r"G:\Drives compartilhados\STARMKT\StarIA"))
TEXT_EXTS = {
    ".txt",
    ".md",
    ".csv",
    ".xlsx",
    ".xls",
    ".docx",
    ".pdf"
}
MAX_FILES = 20

def safe_read_text(path: Path) -> str:
    try:
        ext = path.suffix.lower()

        if ext in {".txt", ".md", ".csv"}:
            return path.read_text(encoding="utf-8", errors="ignore")

        if ext in {".xlsx", ".xls"}:
            df = pd.read_excel(path)
            return df.to_string()

        if ext in {".docx"}:
            doc = docx.Document(path)
            return "\n".join(p.text for p in doc.paragraphs)

        if ext in {".pdf"}:
            text = ""
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() or ""
            return text

        return ""

    except Exception as e:
        print(f"[ERRO] Falha ao ler {path}: {e}")
        return ""
       
def _semantic_alias_for_file(filename: str) -> str:
    mapping = {
        "ctx_company.txt": "empresa StarMKT contexto institucional companhia organização",
        "ctx_clients.txt": "clientes contas carteira de clientes StarMKT",
        "ctx_services.txt": "serviços soluções entregas escopo StarMKT",
        "ctx_processes.txt": "processos fluxo operação etapas StarMKT",
        "ctx_glossary.txt": "glossário termos definições conceitos StarMKT",
        "ctx_staria.txt": "StarIA inteligência artificial agente assistente corporativo",
        "ctx_operational_rules.txt": "regras operacionais comportamento instruções políticas do agente",
    }
    return mapping.get(filename.lower(), "")    

def main():
    if not DRIVE_ROOT.exists():
        print(f"[ERRO] Pasta não encontrada: {DRIVE_ROOT}")
        return

    count = 0
    for p in DRIVE_ROOT.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in TEXT_EXTS:
            continue

        text = safe_read_text(p).strip()
        if not text:
            continue

        alias = _semantic_alias_for_file(p.name)

        index_text = f"""Arquivo: {p.name}
        Nome sem extensão: {p.stem}
        Aliases semânticos: {alias}
        Conteúdo:
        {text}
         """

        upsert_text(
            doc_id=str(p),
            text=index_text[:20000],
            metadata={
                "path": str(p),
                "type": p.suffix.lower().replace(".", ""),
                "filename": p.name,
            },
        )
        count += 1
        print(f"[OK] Indexado: {p}")

        if count >= MAX_FILES:
            print(f"\n[LIMITE] Parando em {MAX_FILES} arquivos.")
            break

    print(f"\nConcluído. Total indexado: {count}")

if __name__ == "__main__":
    main()
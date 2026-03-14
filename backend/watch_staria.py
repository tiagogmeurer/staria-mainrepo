import os
import time
import threading
from pathlib import Path

import pandas as pd
import pdfplumber
import docx

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from rag.indexer import upsert_text, get_client, COLLECTION

DEFAULT_ROOT = r"G:\Drives compartilhados\STARMKT\StarIA"
STARIA_ROOT = Path(os.getenv("STARIA_DRIVE_ROOT", DEFAULT_ROOT)).resolve()

ALLOWED_EXTS = {
    ".txt",
    ".md",
    ".csv",
    ".xlsx",
    ".xls",
    ".docx",
    ".pdf",
    # ".doc",  # legado binário; melhor converter para .docx
}

IGNORED_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".obsidian",
    ".idea",
    ".vscode",
}

IGNORED_FILE_PREFIXES = ("~$", ".")
IGNORED_FILE_SUFFIXES = (".tmp", ".temp", ".bak", ".crdownload", ".part")
DEBOUNCE_SECONDS = 2.0


def _assert_safe_root(root: Path) -> None:
    root_str = str(root).lower().replace("/", "\\")
    expected = r"g:\drives compartilhados\starmkt\staria"
    if not root_str.startswith(expected):
        raise SystemExit(
            f"[ERRO] STARIA_ROOT inseguro: {root}\n"
            f"O watcher só pode rodar dentro de: {expected}"
        )


def _is_inside_root(path: Path) -> bool:
    try:
        path.resolve().relative_to(STARIA_ROOT)
        return True
    except Exception:
        return False


def _is_ignored(path: Path) -> bool:
    name = path.name.lower()

    if any(part.lower() in IGNORED_DIR_NAMES for part in path.parts):
        return True

    if name.startswith(IGNORED_FILE_PREFIXES):
        return True

    if name.endswith(IGNORED_FILE_SUFFIXES):
        return True

    return False


def _should_index(path: Path) -> bool:
    if not path.is_file():
        return False
    if not _is_inside_root(path):
        return False
    if _is_ignored(path):
        return False
    if path.suffix.lower() not in ALLOWED_EXTS:
        return False
    return True


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


def _folder_tag(path: Path) -> str:
    parts = [p.lower() for p in path.parts]
    if "curriculos" in parts:
        return "curriculos"
    if "datasets" in parts:
        return "datasets"
    return "geral"


def safe_read_text(path: Path) -> str:
    try:
        ext = path.suffix.lower()

        if ext in {".txt", ".md", ".csv"}:
            return path.read_text(encoding="utf-8", errors="ignore")

        if ext in {".xlsx", ".xls"}:
            df = pd.read_excel(path)
            return df.to_string()

        if ext == ".docx":
            d = docx.Document(path)
            return "\n".join(p.text for p in d.paragraphs)

        if ext == ".pdf":
            chunks = []
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    chunks.append(page.extract_text() or "")
            return "\n".join(chunks)

        return ""

    except Exception as e:
        print(f"[WATCH] Falha ao ler {path}: {e}")
        return ""


def delete_from_index(path: Path) -> None:
    client = get_client()
    col = client.get_or_create_collection(COLLECTION)
    col.delete(ids=[str(path)])
    print(f"[WATCH] Removido do índice: {path}")


def index_file(path: Path) -> None:
    if not _should_index(path):
        return

    text = safe_read_text(path).strip()
    if not text:
        print(f"[WATCH] Ignorado (sem texto): {path}")
        return

    alias = _semantic_alias_for_file(path.name)
    index_text = f"""Arquivo: {path.name}
Nome sem extensão: {path.stem}
Aliases semânticos: {alias}
Conteúdo:
{text}
"""

    metadata = {
        "path": str(path),
        "filename": path.name,
        "folder": _folder_tag(path),
        "doc_type": path.suffix.lower().replace(".", ""),
        "chunk": 1,
    }

    upsert_text(
        doc_id=str(path),
        text=index_text[:20000],
        metadata=metadata,
    )
    print(f"[WATCH] Indexado/atualizado: {path}")


class DebouncedIndexer(FileSystemEventHandler):
    def __init__(self) -> None:
        super().__init__()
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def _schedule_index(self, raw_path: str) -> None:
        path = Path(raw_path)

        if not _is_inside_root(path):
            return

        key = str(path)

        with self._lock:
            old = self._timers.get(key)
            if old:
                old.cancel()

            timer = threading.Timer(DEBOUNCE_SECONDS, index_file, args=(path,))
            self._timers[key] = timer
            timer.start()

    def on_created(self, event):
        if not event.is_directory:
            self._schedule_index(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule_index(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            src = Path(event.src_path)
            dst = Path(event.dest_path)

            if _is_inside_root(src):
                try:
                    delete_from_index(src)
                except Exception as e:
                    print(f"[WATCH] Falha ao remover origem movida {src}: {e}")

            if _is_inside_root(dst):
                self._schedule_index(event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
            path = Path(event.src_path)
            if _is_inside_root(path):
                try:
                    delete_from_index(path)
                except Exception as e:
                    print(f"[WATCH] Falha ao remover deletado {path}: {e}")


def bootstrap_initial_index() -> None:
    print(f"[WATCH] Bootstrap inicial em: {STARIA_ROOT}")
    for p in STARIA_ROOT.rglob("*"):
        if _should_index(p):
            index_file(p)


def main() -> None:
    _assert_safe_root(STARIA_ROOT)

    if not STARIA_ROOT.exists():
        raise SystemExit(f"[ERRO] Pasta não encontrada: {STARIA_ROOT}")

    print("[WATCH] STARIA_ROOT =", STARIA_ROOT)

    # opcional: roda uma passada inicial quando o watcher sobe
    bootstrap_initial_index()

    handler = DebouncedIndexer()
    observer = Observer()
    observer.schedule(handler, str(STARIA_ROOT), recursive=True)
    observer.start()

    print("[WATCH] Monitor incremental do StarIA rodando...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[WATCH] Encerrando...")
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()
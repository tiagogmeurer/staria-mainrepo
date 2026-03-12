from pathlib import Path
from rag.indexer import upsert_text

DRIVE_ROOT = Path(r"G:\Drives compartilhados\STARMKT\_StarIA_Test")
TEXT_EXTS = {".txt", ".md", ".csv"}
MAX_FILES = 20

def safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""

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

        upsert_text(
            doc_id=str(p),
            text=text[:20000],
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
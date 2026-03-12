import os
from pathlib import Path
import chromadb
from chromadb.config import Settings

CHROMA_DIR = os.getenv("CHROMA_DIR", r"C:\AI\vector_db\chroma")
COLLECTION = os.getenv("CHROMA_COLLECTION", "star_docs")

def get_client():
    return chromadb.PersistentClient(path=CHROMA_DIR, settings=Settings(anonymized_telemetry=False))

def upsert_text(doc_id: str, text: str, metadata: dict):
    client = get_client()
    col = client.get_or_create_collection(COLLECTION)
    col.upsert(ids=[doc_id], documents=[text], metadatas=[metadata])

def index_txt_folder(folder: str, limit: int = 500):
    folderp = Path(folder)
    count = 0
    for p in folderp.rglob("*.txt"):
        text = p.read_text(encoding="utf-8", errors="ignore")
        upsert_text(
            doc_id=str(p),
            text=text[:20000],  # keep it bounded
            metadata={"path": str(p), "type": "txt"},
        )
        count += 1
        if count >= limit:
            break
    return count

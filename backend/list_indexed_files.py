import os
import chromadb
from chromadb.config import Settings

CHROMA_DIR = os.getenv("CHROMA_DIR", r"C:\AI\vector_db\chroma")
COLLECTION = os.getenv("CHROMA_COLLECTION", "star_docs")

client = chromadb.PersistentClient(
    path=CHROMA_DIR,
    settings=Settings(anonymized_telemetry=False),
)

col = client.get_or_create_collection(COLLECTION)
data = col.get()

print(f"Banco: {CHROMA_DIR}")
print(f"Coleção: {COLLECTION}")
print(f"Total de registros: {len(data.get('ids', []))}")

files = set()

for meta in data.get("metadatas", []):
    if isinstance(meta, dict):
        path = meta.get("path")
        if path:
            files.add(path)

print("\nArquivos indexados:\n")

for f in sorted(files):
    print(f)
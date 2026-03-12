import os
import chromadb
from chromadb.config import Settings

CHROMA_DIR = os.getenv("CHROMA_DIR", r"C:\AI\vector_db\chroma")
COLLECTION = os.getenv("CHROMA_COLLECTION", "star_docs")

_client = chromadb.PersistentClient(
    path=CHROMA_DIR,
    settings=Settings(anonymized_telemetry=False),
)
_col = _client.get_or_create_collection(name=COLLECTION)


def upsert(ids: list[str], docs: list[str], metas: list[dict], embeddings: list[list[float]]):
    _col.upsert(ids=ids, documents=docs, metadatas=metas, embeddings=embeddings)


def query(query_embedding: list[float], n_results: int = 6, where: dict | None = None):
    return _col.query(
        query_embeddings=[query_embedding],
        n_results=n_results,
        where=where,
    )


def delete_by_doc(doc_id: str):
    _col.delete(where={"doc_id": doc_id})
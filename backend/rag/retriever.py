import os
import chromadb
from chromadb.config import Settings

CHROMA_DIR = os.getenv("CHROMA_DIR", r"C:\AI\vector_db\chroma")
COLLECTION = os.getenv("CHROMA_COLLECTION", "star_docs")

_client = chromadb.PersistentClient(
    path=CHROMA_DIR,
    settings=Settings(anonymized_telemetry=False),
)
_col = _client.get_or_create_collection(COLLECTION)


def retrieve(query: str, k: int = 6, where: dict | None = None) -> list[dict]:
    res = _col.query(
        query_texts=[query],
        n_results=k,
        where=where,
        include=["documents", "metadatas", "distances"],
    )

    ids = (res.get("ids") or [[]])[0]
    docs = (res.get("documents") or [[]])[0]
    metas = (res.get("metadatas") or [[]])[0]
    dists = (res.get("distances") or [[]])[0]

    out = []
    for i in range(len(ids)):
        out.append(
            {
                "id": ids[i],
                "doc": docs[i],
                "meta": metas[i] or {},
                "distance": dists[i] if i < len(dists) else None,
            }
        )
    return out
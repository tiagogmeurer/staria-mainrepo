import os
from functools import lru_cache
from sentence_transformers import SentenceTransformer

MODEL_NAME = os.getenv("EMBED_MODEL", "intfloat/multilingual-e5-small")

@lru_cache(maxsize=1)
def _model() -> SentenceTransformer:
    # CPU por padrão (estável). Se quiser forçar GPU depois, dá pra setar device="cuda".
    return SentenceTransformer(MODEL_NAME)

def embed_texts(texts: list[str]) -> list[list[float]]:
    m = _model()
    vecs = m.encode(
        texts,
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=False,
    )
    return [v.tolist() for v in vecs]

def embed_query(text: str) -> list[float]:
    return embed_texts([text])[0]

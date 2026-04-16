from __future__ import annotations

import re
import unicodedata

from datasets.professional_profiles.loader import load_profiles


def _strip_accents(text: str) -> str:
    text = text or ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(c)
    )


def normalize_text(text: str) -> str:
    text = _strip_accents(text or "").lower().strip()
    text = re.sub(r"[^a-z0-9\s/_-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def singularize_pt(text: str) -> str:
    words = []
    for w in normalize_text(text).split():
        if len(w) > 3 and w.endswith("s"):
            words.append(w[:-1])
        else:
            words.append(w)
    return " ".join(words).strip()


def normalize_role_query(query: str) -> str:
    return singularize_pt(query)


def get_profile_alias_map() -> dict[str, str]:
    alias_map: dict[str, str] = {}

    for profile in load_profiles():
        entries = [profile.title, profile.role_id, *profile.aliases, *profile.keywords]

        for entry in entries:
            key = singularize_pt(entry)
            if key:
                alias_map[key] = profile.role_id

    return alias_map


def resolve_role_id(query: str) -> str | None:
    q = singularize_pt(query)
    if not q:
        return None

    alias_map = get_profile_alias_map()

    if q in alias_map:
        return alias_map[q]

    for alias, role_id in alias_map.items():
        if q in alias or alias in q:
            return role_id

    q_tokens = set(q.split())
    for alias, role_id in alias_map.items():
        alias_tokens = set(alias.split())
        if q_tokens and q_tokens.issubset(alias_tokens):
            return role_id

    return None
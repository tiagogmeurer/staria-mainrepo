from __future__ import annotations

import json
from pathlib import Path

from datasets.professional_profiles.schema import (
    ProfessionalProfile,
    ProfessionalProfilesCatalog,
)


BASE_DIR = Path(__file__).resolve().parent
CATALOG_JSON = BASE_DIR / "profiles_catalog.json"


def _read_json_file(path: Path) -> dict:
    if not path.exists():
        return {"profiles": []}

    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return {"profiles": []}

    data = json.loads(raw)
    if isinstance(data, list):
        return {"profiles": data}

    if isinstance(data, dict):
        if "profiles" in data and isinstance(data["profiles"], list):
            return data
        return {"profiles": []}

    return {"profiles": []}


def load_profiles_catalog() -> ProfessionalProfilesCatalog:
    data = _read_json_file(CATALOG_JSON)
    return ProfessionalProfilesCatalog(**data)


def load_profiles() -> list[ProfessionalProfile]:
    return load_profiles_catalog().profiles


def get_profile_by_role_id(role_id: str) -> ProfessionalProfile | None:
    rid = (role_id or "").strip().lower()
    if not rid:
        return None

    for profile in load_profiles():
        if profile.role_id.strip().lower() == rid:
            return profile
    return None


def list_active_profiles() -> list[ProfessionalProfile]:
    return [p for p in load_profiles() if p.status == "active"]


def find_profiles_by_family(family: str) -> list[ProfessionalProfile]:
    family_norm = (family or "").strip().lower()
    if not family_norm:
        return []

    return [
        p
        for p in load_profiles()
        if p.family.strip().lower() == family_norm
    ]


def find_profiles_by_hub(hub: str) -> list[ProfessionalProfile]:
    hub_norm = (hub or "").strip().lower()
    if not hub_norm:
        return []

    return [
        p
        for p in load_profiles()
        if p.hub.strip().lower() == hub_norm
    ]


def search_profiles(query: str, limit: int = 10) -> list[ProfessionalProfile]:
    q = (query or "").strip().lower()
    if not q:
        return list_active_profiles()[:limit]

    scored: list[tuple[int, ProfessionalProfile]] = []

    for profile in list_active_profiles():
        text = profile.search_text().lower()
        score = 0

        for token in q.split():
            if token in text:
                score += 1

        if score > 0:
            scored.append((score, profile))

    scored.sort(key=lambda x: (-x[0], x[1].title.lower()))
    return [item[1] for item in scored[:limit]]
from __future__ import annotations

import json
import os
from pathlib import Path

from datasets.professional_profiles.schema import (
    ProfessionalProfile,
    ProfessionalProfilesCatalog,
)


BASE_DIR = Path(__file__).resolve().parent
LOCAL_CATALOG_JSON = BASE_DIR / "profiles_catalog.json"

DEFAULT_DRIVE_ROOT = os.getenv(
    "STARIA_DRIVE_ROOT",
    r"G:\Drives compartilhados\STARMKT\StarIA",
)
PROFILES_DIR = Path(
    os.getenv(
        "STARIA_PROFILES_DIR",
        str(Path(DEFAULT_DRIVE_ROOT) / "banco_talentos" / "perfis"),
    )
)
SHARED_CATALOG_XLSX = Path(
    os.getenv(
        "STARIA_PROFILES_XLSX",
        str(PROFILES_DIR / "profiles_catalog.xlsx"),
    )
)


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


def _read_xlsx_file(path: Path) -> dict:
    if not path.exists():
        return {"profiles": []}

    from datasets.professional_profiles.sync_profiles import load_catalog_from_xlsx

    catalog = load_catalog_from_xlsx(path)
    return catalog.model_dump(mode="json")


def get_profiles_catalog_paths() -> dict[str, str]:
    return {
        "profiles_dir": str(PROFILES_DIR),
        "shared_catalog_xlsx": str(SHARED_CATALOG_XLSX),
        "local_catalog_json": str(LOCAL_CATALOG_JSON),
    }


def load_profiles_catalog(prefer_shared_xlsx: bool = True) -> ProfessionalProfilesCatalog:
    if prefer_shared_xlsx and SHARED_CATALOG_XLSX.exists():
        data = _read_xlsx_file(SHARED_CATALOG_XLSX)
        return ProfessionalProfilesCatalog(**data)

    data = _read_json_file(LOCAL_CATALOG_JSON)
    return ProfessionalProfilesCatalog(**data)


def load_profiles(prefer_shared_xlsx: bool = True) -> list[ProfessionalProfile]:
    return load_profiles_catalog(prefer_shared_xlsx=prefer_shared_xlsx).profiles


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
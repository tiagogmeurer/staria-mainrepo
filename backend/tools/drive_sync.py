import os
from pathlib import Path
from typing import List

DRIVE_SYNC_ROOT = os.getenv("DRIVE_SYNC_ROOT", r"D:\CompanyData\DriveSync")

def list_files(rel_path: str = "", exts: List[str] | None = None, limit: int = 200) -> list[str]:
    root = Path(DRIVE_SYNC_ROOT).resolve()
    base = (root / rel_path).resolve()

    if root not in base.parents and base != root:
        raise ValueError("Path outside DRIVE_SYNC_ROOT")

    if not base.exists():
        return []

    results = []
    for p in base.rglob("*"):
        if p.is_file():
            if exts and p.suffix.lower() not in [e.lower() for e in exts]:
                continue
            results.append(str(p))
            if len(results) >= limit:
                break
    return results
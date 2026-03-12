from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import time

@dataclass
class AutomationResult:
    ok: bool
    message: str
    data: dict | None = None

SAFE_ROOT = Path(r"D:\CompanyData").resolve()

def ensure_safe_path(path: str) -> Path:
    p = Path(path).resolve()
    if SAFE_ROOT not in p.parents and p != SAFE_ROOT:
        raise ValueError("Path outside SAFE_ROOT")
    return p

def create_folder(path: str) -> AutomationResult:
    p = ensure_safe_path(path)
    p.mkdir(parents=True, exist_ok=True)
    return AutomationResult(True, f"Pasta criada: {p}")

def write_text_report(path: str, content: str) -> AutomationResult:
    p = ensure_safe_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    p.write_text(f"[{stamp}]\n{content}\n", encoding="utf-8")
    return AutomationResult(True, f"Relatório salvo em: {p}")

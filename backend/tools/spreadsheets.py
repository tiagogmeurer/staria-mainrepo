from __future__ import annotations
import pandas as pd
from pathlib import Path

def read_excel_preview(path: str, sheet_name: str | int | None = None, n: int = 30) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    df = pd.read_excel(p, sheet_name=sheet_name)
    if isinstance(df, dict):
        # if sheet_name=None it may return dict of sheets
        first_key = list(df.keys())[0]
        df = df[first_key]

    return {
        "columns": list(df.columns.astype(str)),
        "rows": df.head(n).fillna("").astype(str).to_dict(orient="records"),
        "shape": [int(df.shape[0]), int(df.shape[1])],
    }

def compute_basic_stats(path: str, sheet_name: str | int | None = None) -> dict:
    p = Path(path)
    df = pd.read_excel(p, sheet_name=sheet_name)
    if isinstance(df, dict):
        first_key = list(df.keys())[0]
        df = df[first_key]

    numeric = df.select_dtypes(include="number")
    return {
        "shape": [int(df.shape[0]), int(df.shape[1])],
        "numeric_columns": list(numeric.columns.astype(str)),
        "describe": numeric.describe().fillna("").to_dict(),
    }
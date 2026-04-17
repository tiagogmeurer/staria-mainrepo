from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from datasets.professional_profiles.loader import load_profiles
from datasets.professional_profiles.normalizer import normalize_role_query, resolve_role_id
from datasets.professional_profiles.schema import ProfessionalProfile


DEFAULT_DRIVE_ROOT = os.getenv(
    "STARIA_DRIVE_ROOT",
    r"G:\Drives compartilhados\STARMKT\StarIA",
)
BANCO_TALENTOS_XLSX = Path(
    os.getenv(
        "STARIA_TALENTS_XLSX",
        str(Path(DEFAULT_DRIVE_ROOT) / "banco_talentos" / "banco_talentos.xlsx"),
    )
)


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


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def load_banco_talentos_rows(
    path: Path = BANCO_TALENTOS_XLSX,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    wb = load_workbook(path, data_only=True)
    ws = wb.active

    headers = [str(c.value).strip() if c.value is not None else "" for c in ws[1]]
    rows: list[dict[str, Any]] = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        item: dict[str, Any] = {}
        has_any = False

        for idx, val in enumerate(row):
            key = headers[idx] if idx < len(headers) else f"col_{idx + 1}"
            item[key] = val
            if val not in (None, ""):
                has_any = True

        if has_any:
            rows.append(item)

        if len(rows) >= limit:
            break

    return rows


def candidate_name(row: dict[str, Any]) -> str:
    return (
        _safe_str(row.get("Nome completo"))
        or _safe_str(row.get("Nome"))
        or _safe_str(row.get("Candidato"))
        or "Sem nome"
    )


def candidate_role_text(row: dict[str, Any]) -> str:
    return (
        _safe_str(row.get("Cargo pretendido"))
        or _safe_str(row.get("Cargo"))
        or _safe_str(row.get("Função"))
    )


def candidate_seniority_text(row: dict[str, Any]) -> str:
    return (
        _safe_str(row.get("Nível"))
        or _safe_str(row.get("Senioridade"))
        or _safe_str(row.get("Nivel"))
    )


def candidate_location_text(row: dict[str, Any]) -> str:
    return (
        _safe_str(row.get("Localização"))
        or _safe_str(row.get("Localizacao"))
        or _safe_str(row.get("Cidade"))
        or _safe_str(row.get("Região"))
        or _safe_str(row.get("Regiao"))
    )


def candidate_resume_text(row: dict[str, Any]) -> str:
    preferred_keys = [
        "Resumo",
        "Observações",
        "Observacoes",
        "Skills",
        "Competências",
        "Competencias",
        "Ferramentas",
        "Experiência",
        "Experiencia",
        "Formação",
        "Formacao",
    ]

    parts = [
        candidate_name(row),
        candidate_role_text(row),
        candidate_seniority_text(row),
        candidate_location_text(row),
    ]

    for key in preferred_keys:
        value = _safe_str(row.get(key))
        if value:
            parts.append(value)

    for key, value in row.items():
        if key in preferred_keys:
            continue
        value_str = _safe_str(value)
        if value_str and value_str not in parts:
            parts.append(value_str)

    return " | ".join([p for p in parts if p])


def _token_score(text: str, targets: list[str]) -> tuple[float, list[str]]:
    haystack = singularize_pt(text)
    matched: list[str] = []

    valid_targets = []
    for item in targets:
        needle = singularize_pt(item)
        if not needle:
            continue
        valid_targets.append(item)
        if needle in haystack:
            matched.append(item)

    if not valid_targets:
        return 0.0, []

    score = len(set(matched)) / len(valid_targets)
    return min(score, 1.0), sorted(set(matched))


def _contains_any(text: str, values: list[str]) -> bool:
    normalized = singularize_pt(text)
    for value in values:
        if singularize_pt(value) in normalized:
            return True
    return False


def _parse_query_constraints(query: str) -> dict[str, str]:
    q = normalize_text(query)

    seniority = ""
    if "senior" in q:
        seniority = "senior"
    elif "pleno" in q:
        seniority = "pleno"
    elif "junior" in q:
        seniority = "junior"

    location = ""
    patterns = [
        r"proxim[oa]\s+a\s+([a-z0-9\s-]+)",
        r"perto\s+de\s+([a-z0-9\s-]+)",
        r"na\s+regiao\s+de\s+([a-z0-9\s-]+)",
        r"em\s+([a-z0-9\s-]+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, q)
        if match:
            location = match.group(1).strip(" .,:;!?")
            break

    return {"seniority": seniority, "location": location}


def _resolve_target_profile(query: str) -> ProfessionalProfile | None:
    role_id = resolve_role_id(query)
    profiles = load_profiles()

    if role_id:
        for profile in profiles:
            if profile.role_id == role_id:
                return profile

    normalized_query = normalize_role_query(query)
    query_tokens = normalized_query.split()

    best_profile = None
    best_score = -1.0

    for profile in profiles:
        text = " | ".join([profile.title, *profile.aliases, *profile.keywords, profile.summary])
        score, _ = _token_score(text, query_tokens)
        if score > best_score:
            best_score = score
            best_profile = profile

    return best_profile


def score_candidate_against_profile(
    row: dict[str, Any],
    profile: ProfessionalProfile,
    query: str = "",
) -> dict[str, Any]:
    role_text = candidate_role_text(row)
    seniority_text = candidate_seniority_text(row)
    location_text = candidate_location_text(row)
    resume_text = candidate_resume_text(row)

    required_score, matched_required = _token_score(resume_text, profile.required_skills)
    preferred_score, matched_preferred = _token_score(resume_text, profile.preferred_skills)
    tools_score, matched_tools = _token_score(resume_text, profile.tools)

    role_targets = [profile.title, profile.role_id, *profile.aliases, *profile.keywords]
    role_score, matched_role = _token_score(role_text or resume_text, role_targets)

    constraints = _parse_query_constraints(query)
    seniority_score = 0.0
    location_score = 0.0

    if profile.seniority and singularize_pt(profile.seniority) in singularize_pt(seniority_text + " | " + role_text):
        seniority_score = 1.0

    if constraints["seniority"] and singularize_pt(constraints["seniority"]) in singularize_pt(seniority_text + " | " + role_text):
        seniority_score = max(seniority_score, 1.0)

    if profile.location and _contains_any(location_text, [profile.location]):
        location_score = 1.0

    if constraints["location"] and _contains_any(location_text, [constraints["location"]]):
        location_score = max(location_score, 1.0)

    weights = {
        "role": 0.30,
        "required": 0.30,
        "preferred": 0.10,
        "tools": 0.15,
        "seniority": 0.10,
        "location": 0.05,
    }

    final_score = (
        role_score * weights["role"]
        + required_score * weights["required"]
        + preferred_score * weights["preferred"]
        + tools_score * weights["tools"]
        + seniority_score * weights["seniority"]
        + location_score * weights["location"]
    )

    missing_required = [item for item in profile.required_skills if item not in matched_required]

    return {
        "candidate_name": candidate_name(row),
        "candidate_role": role_text,
        "candidate_seniority": seniority_text,
        "candidate_location": location_text,
        "profile_role_id": profile.role_id,
        "profile_title": profile.title,
        "score": round(final_score, 4),
        "score_pct": round(final_score * 100, 1),
        "matched_role_terms": matched_role,
        "matched_required_skills": matched_required,
        "matched_preferred_skills": matched_preferred,
        "matched_tools": matched_tools,
        "missing_required_skills": missing_required,
        "query_constraints": constraints,
        "raw_row": row,
    }


def search_candidates_by_profile_query(
    query: str,
    limit: int = 10,
    min_score: float = 0.15,
) -> dict[str, Any]:
    profile = _resolve_target_profile(query)
    if not profile:
        return {
            "query": query,
            "profile": None,
            "matches": [],
            "total_candidates": 0,
            "message": "Não consegui resolver um perfil profissional para essa busca.",
        }

    rows = load_banco_talentos_rows()
    scored = [score_candidate_against_profile(row, profile=profile, query=query) for row in rows]
    scored = [item for item in scored if item["score"] >= min_score]
    scored.sort(key=lambda item: (-item["score"], item["candidate_name"].lower()))

    return {
        "query": query,
        "profile": {
            "role_id": profile.role_id,
            "title": profile.title,
            "family": profile.family,
            "hub": profile.hub,
            "seniority": profile.seniority,
            "location": profile.location,
        },
        "matches": scored[:limit],
        "total_candidates": len(rows),
        "message": f"Encontrei {len(scored[:limit])} candidato(s) aderentes para '{profile.title}'.",
    }


def format_match_summary(result: dict[str, Any]) -> str:
    profile = result.get("profile")
    matches = result.get("matches") or []

    if not profile:
        return result.get("message") or "Não consegui resolver um perfil para essa busca."

    if not matches:
        return f"Não encontrei candidatos aderentes para '{profile['title']}' no banco de talentos."

    lines = [f"Perfil alvo: {profile['title']}", "", "Melhores aderências:"]

    for idx, item in enumerate(matches, start=1):
        line = f"{idx}. {item['candidate_name']} | Score: {item['score_pct']}%"
        if item["candidate_role"]:
            line += f" | Cargo: {item['candidate_role']}"
        if item["candidate_seniority"]:
            line += f" | Nível: {item['candidate_seniority']}"
        if item["candidate_location"]:
            line += f" | Local: {item['candidate_location']}"
        lines.append(line)

        evidence_parts = []
        if item["matched_required_skills"]:
            evidence_parts.append("skills: " + ", ".join(item["matched_required_skills"][:4]))
        if item["matched_tools"]:
            evidence_parts.append("tools: " + ", ".join(item["matched_tools"][:4]))
        if item["missing_required_skills"]:
            evidence_parts.append("faltas: " + ", ".join(item["missing_required_skills"][:3]))
        if evidence_parts:
            lines.append("   " + " | ".join(evidence_parts))

    return "\\n".join(lines)
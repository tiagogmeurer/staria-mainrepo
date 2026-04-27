from __future__ import annotations

import os
import re
import shutil
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Any

from openpyxl import Workbook, load_workbook


DEFAULT_DRIVE_ROOT = Path(
    os.getenv("STARIA_DRIVE_ROOT", r"G:\Drives compartilhados\STARMKT\StarIA")
)

DEFAULT_BANCO_TALENTOS_XLSX = Path(
    os.getenv(
        "STARIA_TALENTS_XLSX",
        str(DEFAULT_DRIVE_ROOT / "banco_talentos" / "banco_talentos.xlsx"),
    )
)

DEFAULT_REFINED_XLSX = Path(
    os.getenv(
        "CANDIDATOS_REFINADOS_XLSX",
        str(DEFAULT_DRIVE_ROOT / "banco_talentos" / "candidatos_refinados.xlsx"),
    )
)

VISIBLE_SYSTEM_HEADERS = ["ID", "Nota"]

TECHNICAL_HEADERS = [
    "Caminho do currículo",
    "Nome do arquivo",
    "Data de entrada",
    "Origem",
    "Remetente do email",
    "Status",
    "Observações",
    "Role ID sugerido",
    "Título normalizado",
    "Top 3 roles aderentes",
    "Resumo de aderência",
    "Flags de risco",
]

FALLBACK_REFINED_HEADERS = [
    "Nota",
    "Nome completo",
    "Idade",
    "Localização",
    "Cargo pretendido",
    "Nível",
    "Portfólio",
    "Habilidades",
    "Formações",
    "Email",
    "Telefone",
]

HEADER_ALIASES = {
    "Nome ": "Nome completo",
    "Nome": "Nome completo",
    "Localizacao": "Localização",
    "Nivel": "Nível",
    "Habilidades/Experiência": "Habilidades",
    "Habilidades / Experiência": "Habilidades",
    "Experiência": "Habilidades",
    "Experiencia": "Habilidades",
    "Competências": "Habilidades",
    "Competencias": "Habilidades",
    "Portfolio": "Portfólio",
    "E-mail": "Email",
    "Curriculo": "Caminho do currículo",
    "Currículo": "Caminho do currículo",
}


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def norm(value: Any) -> str:
    text = safe_str(value).lower()
    repl = {
        "á": "a", "à": "a", "â": "a", "ã": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c",
    }
    for a, b in repl.items():
        text = text.replace(a, b)
    return text


def normalize_header(header: Any) -> str:
    h = safe_str(header)
    return HEADER_ALIASES.get(h, h)


def get_headers(ws) -> list[str]:
    return [normalize_header(c.value) for c in ws[1]]


def build_header_map(ws) -> dict[str, int]:
    out = {}
    for idx, cell in enumerate(ws[1], start=1):
        h = normalize_header(cell.value)
        if h:
            out[h] = idx
    return out


def backup_workbook(path: Path) -> Path | None:
    if not path.exists():
        return None

    backup_dir = path.parent / "_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{path.stem}_backup_{stamp}{path.suffix}"
    shutil.copy2(path, backup_path)
    return backup_path


def copy_cell_style(src, dst) -> None:
    if src is None:
        return

    if src.has_style:
        dst.font = copy(src.font)
        dst.fill = copy(src.fill)
        dst.border = copy(src.border)
        dst.alignment = copy(src.alignment)
        dst.number_format = src.number_format
        dst.protection = copy(src.protection)


def first_non_empty_header_cell(ws):
    for cell in ws[1]:
        if safe_str(cell.value):
            return cell
    return ws.cell(row=1, column=1)


def load_or_create_workbook(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        return load_workbook(path, data_only=False)

    wb = Workbook()
    ws = wb.active
    ws.title = "BancoTalentos"
    return wb


def get_refined_workbook(refined_path: Path):
    if refined_path.exists():
        return load_workbook(refined_path, data_only=False)
    return None


def get_refined_sheet_headers(template_ws) -> list[str]:
    if template_ws is None:
        return FALLBACK_REFINED_HEADERS.copy()

    headers = []
    for h in get_headers(template_ws):
        if not h:
            continue
        if h == "ID":
            continue
        headers.append(h)

    if not headers:
        headers = FALLBACK_REFINED_HEADERS.copy()

    if "Nota" not in headers:
        headers = ["Nota"] + headers

    seen = set()
    deduped = []
    for h in headers:
        if h not in seen:
            seen.add(h)
            deduped.append(h)

    return deduped


def build_bank_headers(refined_headers: list[str]) -> list[str]:
    visible_headers = [h for h in refined_headers if h and h != "ID"]

    if "Nota" in visible_headers:
        visible_headers = [h for h in visible_headers if h != "Nota"]

    headers = ["ID", "Nota"] + visible_headers

    for h in TECHNICAL_HEADERS:
        if h not in headers:
            headers.append(h)

    return headers


def extract_rows_from_workbook(wb) -> list[dict[str, Any]]:
    rows = []

    for ws in wb.worksheets:
        header_map = build_header_map(ws)
        if not header_map:
            continue

        headers = get_headers(ws)

        for row in ws.iter_rows(min_row=2, values_only=True):
            item = {"_source_sheet": ws.title}
            has_any = False

            for idx, val in enumerate(row):
                header = headers[idx] if idx < len(headers) else f"col_{idx + 1}"
                header = normalize_header(header)

                if not header:
                    continue

                item[header] = val

                if val not in (None, ""):
                    has_any = True

            if has_any:
                rows.append(item)

    return rows


def merge_duplicate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = {}
    ordered_keys = []

    for row in rows:
        key = (
            safe_str(row.get("ID"))
            or safe_str(row.get("Caminho do currículo"))
            or safe_str(row.get("Email"))
            or safe_str(row.get("Nome completo")) + "|" + safe_str(row.get("Telefone"))
        )

        if not key:
            key = f"row_{len(ordered_keys) + 1}"

        if key not in seen:
            seen[key] = row
            ordered_keys.append(key)
            continue

        current = seen[key]
        for k, v in row.items():
            if k.startswith("_"):
                continue
            if safe_str(v) and not safe_str(current.get(k)):
                current[k] = v

    return [seen[k] for k in ordered_keys]


def copy_template_dimensions(target_ws, target_headers: list[str], template_ws=None) -> None:
    if template_ws is None:
        return

    template_headers = get_headers(template_ws)

    for idx, header in enumerate(target_headers, start=1):
        target_letter = target_ws.cell(row=1, column=idx).column_letter

        if header == "ID":
            src_cell = first_non_empty_header_cell(template_ws)
            src_letter = src_cell.column_letter
        elif header in template_headers:
            src_idx = template_headers.index(header) + 1
            src_letter = template_ws.cell(row=1, column=src_idx).column_letter
        else:
            src_cell = first_non_empty_header_cell(template_ws)
            src_letter = src_cell.column_letter

        width = template_ws.column_dimensions[src_letter].width
        if width:
            target_ws.column_dimensions[target_letter].width = width

    for header in TECHNICAL_HEADERS:
        if header in target_headers:
            idx = target_headers.index(header) + 1
            letter = target_ws.cell(row=1, column=idx).column_letter
            target_ws.column_dimensions[letter].hidden = True
            target_ws.column_dimensions[letter].width = 18


def apply_header_styles(target_ws, target_headers: list[str], template_ws=None) -> None:
    template_headers = get_headers(template_ws) if template_ws is not None else []

    for idx, header in enumerate(target_headers, start=1):
        dst = target_ws.cell(row=1, column=idx)
        dst.value = header

        src = None

        if template_ws is not None:
            if header == "ID":
                src = first_non_empty_header_cell(template_ws)
            elif header in template_headers:
                src = template_ws.cell(row=1, column=template_headers.index(header) + 1)
            else:
                src = first_non_empty_header_cell(template_ws)

        copy_cell_style(src, dst)


def apply_body_row_style(target_ws, row_idx: int, target_headers: list[str], template_ws=None) -> None:
    if template_ws is None:
        return

    template_headers = get_headers(template_ws)
    template_row = 2 if template_ws.max_row >= 2 else 1

    for idx, header in enumerate(target_headers, start=1):
        dst = target_ws.cell(row=row_idx, column=idx)

        src = None

        if header == "ID":
            src = template_ws.cell(row=template_row, column=first_non_empty_header_cell(template_ws).column)
        elif header in template_headers:
            src = template_ws.cell(row=template_row, column=template_headers.index(header) + 1)
        else:
            src = template_ws.cell(row=template_row, column=first_non_empty_header_cell(template_ws).column)

        copy_cell_style(src, dst)


def clear_and_write_sheet(ws, headers: list[str], rows: list[dict[str, Any]], template_ws=None) -> None:
    if ws.max_row:
        ws.delete_rows(1, ws.max_row)

    ws.append(headers)
    apply_header_styles(ws, headers, template_ws)

    for row_data in rows:
        ws.append([row_data.get(h, "") for h in headers])
        apply_body_row_style(ws, ws.max_row, headers, template_ws)

    ws.freeze_panes = "A2"
    copy_template_dimensions(ws, headers, template_ws)

    if ws.max_row >= 1:
        ws.auto_filter.ref = ws.dimensions


def get_next_candidate_id_from_rows(rows: list[dict[str, Any]]) -> str:
    max_num = 0

    for row in rows:
        value = safe_str(row.get("ID"))
        m = re.match(r"BT(\d+)", value, flags=re.IGNORECASE)
        if m:
            max_num = max(max_num, int(m.group(1)))

    return f"BT{max_num + 1:04d}"


def ensure_ids(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    max_num = 0

    for row in rows:
        value = safe_str(row.get("ID"))
        m = re.match(r"BT(\d+)", value, flags=re.IGNORECASE)
        if m:
            max_num = max(max_num, int(m.group(1)))

    for row in rows:
        if safe_str(row.get("ID")):
            continue
        max_num += 1
        row["ID"] = f"BT{max_num:04d}"

    return rows


def canonical_sheet_rules() -> dict[str, list[str]]:
    return {
        "EXECUTIVO DE CONTAS": ["executivo de contas", "account executive", "executivo comercial"],
        "ATENDIMENTO": ["atendimento", "account manager", "relacionamento", "cliente interno"],
        "COORDENADOR DE CONTEUDO": ["coordenador de conteudo", "coordenador de conteúdo", "conteudo", "conteúdo", "social media", "editorial"],
        "DIRETOR DE ARTE BRANDING": ["diretor de arte branding", "branding", "identidade visual", "brand", "marca"],
        "DIRETOR DE ARTE DIGITAL": ["diretor de arte digital", "da digital", "performance digital", "marketplace", "ecommerce", "e-commerce"],
        "DIRETOR DE ARTE INSTITUCIONAL": ["diretor de arte institucional", "campanhas institucionais", "institucional", "diretor de arte", "designer grafico senior", "designer gráfico sênior"],
        "DIAGRAMADOR": ["diagramador", "diagramadora", "diagramação", "diagramacao", "tabloide", "tablóide", "ofertas", "encarte"],
        "PLANEJAMENTO PERFORMANCE & GROW": ["performance", "growth", "google ads", "meta ads", "analytics", "tiktok ads"],
        "PLANEJAMENTO ESTRATÉGICO": ["estrategista", "planejamento estrategico", "planejamento estratégico", "brand strategy"],
        "MOTION DESIGNER": ["motion", "motion designer", "after effects", "premiere", "animação", "animacao"],
        "REDATOR": ["redator", "redatora", "copywriter", "copy", "redação", "redacao"],
    }


def choose_sheet_name(sheet_names: list[str], row: dict[str, Any]) -> str:
    direct_role_to_sheet = {
        "executivo_de_contas": "EXECUTIVO DE CONTAS",
        "atendimento_senior": "ATENDIMENTO",
        "coordenador_conteudo": "COORDENADOR DE CONTEUDO",
        "diretor_arte_senior_branding_produto": "DIRETOR DE ARTE BRANDING",
        "diretor_arte_senior_digital": "DIRETOR DE ARTE DIGITAL",
        "diretor_arte_senior_campanhas": "DIRETOR DE ARTE INSTITUCIONAL",
        "diagramador_ofertas": "DIAGRAMADOR ",
        "performance_growth_planejamento": "PLANEJAMENTO PERFORMANCE & GROW",
        "estrategista_senior_planejamento": "PLANEJAMENTO ESTRATÉGICO",
        "motion_designer": "MOTION DESIGNER",
        "redator_digital": "REDATOR",
    }

    role_id = safe_str(row.get("Role ID sugerido"))
    if role_id in direct_role_to_sheet:
        target = direct_role_to_sheet[role_id]
        if target in sheet_names:
            return target

    haystack = norm(
        " | ".join(
            [
                safe_str(row.get("Role ID sugerido")),
                safe_str(row.get("Título normalizado")),
                safe_str(row.get("Cargo pretendido")),
                safe_str(row.get("Habilidades")),
                safe_str(row.get("Habilidades/Experiência")),
                safe_str(row.get("Observações")),
            ]
        )
    )

    rules = canonical_sheet_rules()

    best_sheet = ""
    best_score = 0

    for sheet_name in sheet_names:
        aliases = rules.get(sheet_name, [])
        score = sum(1 for alias in aliases if norm(alias) in haystack)

        if score > best_score:
            best_score = score
            best_sheet = sheet_name

    if best_sheet:
        return best_sheet

    source_sheet = safe_str(row.get("_source_sheet"))
    if source_sheet in sheet_names:
        return source_sheet

    return sheet_names[0]


def redistribute_rows_by_sheet(
    rows: list[dict[str, Any]],
    sheet_names: list[str],
) -> dict[str, list[dict[str, Any]]]:
    buckets = {name: [] for name in sheet_names}

    for row in rows:
        sheet_name = choose_sheet_name(sheet_names, row)
        buckets.setdefault(sheet_name, []).append(row)

    return buckets


def ensure_bank_workbook_structure(
    banco_path: Path = DEFAULT_BANCO_TALENTOS_XLSX,
    refined_path: Path = DEFAULT_REFINED_XLSX,
    create_backup: bool = True,
    redistribute_existing: bool = True,
) -> dict[str, Any]:
    banco_path = Path(banco_path)
    refined_path = Path(refined_path)

    banco_path.parent.mkdir(parents=True, exist_ok=True)

    backup_path = None
    if create_backup and banco_path.exists():
        backup_path = backup_workbook(banco_path)

    wb_bank = load_or_create_workbook(banco_path)
    existing_rows = merge_duplicate_rows(extract_rows_from_workbook(wb_bank))
    existing_rows = ensure_ids(existing_rows)

    wb_ref = get_refined_workbook(refined_path)

    if wb_ref is not None:
        target_sheet_names = wb_ref.sheetnames
    else:
        target_sheet_names = ["BancoTalentos"]

    if not target_sheet_names:
        target_sheet_names = ["BancoTalentos"]

    for sheet_name in list(wb_bank.sheetnames):
        if sheet_name not in target_sheet_names:
            del wb_bank[sheet_name]

    for sheet_name in target_sheet_names:
        if sheet_name not in wb_bank.sheetnames:
            wb_bank.create_sheet(sheet_name)

    if "Sheet" in wb_bank.sheetnames and len(wb_bank.sheetnames) > 1:
        del wb_bank["Sheet"]

    buckets = redistribute_rows_by_sheet(existing_rows, target_sheet_names) if redistribute_existing else {
        target_sheet_names[0]: existing_rows
    }

    for sheet_name in target_sheet_names:
        ws = wb_bank[sheet_name]
        template_ws = wb_ref[sheet_name] if wb_ref is not None and sheet_name in wb_ref.sheetnames else None

        refined_headers = get_refined_sheet_headers(template_ws)
        headers = build_bank_headers(refined_headers)

        clear_and_write_sheet(ws, headers, buckets.get(sheet_name, []), template_ws)

    wb_bank.save(banco_path)

    return {
        "ok": True,
        "banco_path": str(banco_path),
        "refined_path": str(refined_path),
        "backup_path": str(backup_path) if backup_path else "",
        "sheets": target_sheet_names,
        "rows_preserved": len(existing_rows),
    }



def normalize_duplicate_key(value: Any) -> str:
    text = norm(safe_str(value))
    text = re.sub(r"[^a-z0-9@./+-]", "", text)
    return text


def normalize_phone(value: Any) -> str:
    return re.sub(r"\D+", "", safe_str(value))


def is_duplicate_candidate(existing_rows: list[dict[str, Any]], values: dict[str, Any]) -> bool:
    new_email = normalize_duplicate_key(values.get("Email"))
    new_phone = normalize_phone(values.get("Telefone"))
    new_resume_name = normalize_duplicate_key(values.get("Nome do arquivo"))
    new_name = normalize_duplicate_key(values.get("Nome completo"))
    new_role = normalize_duplicate_key(values.get("Cargo pretendido"))

    for row in existing_rows:
        old_email = normalize_duplicate_key(row.get("Email"))
        old_phone = normalize_phone(row.get("Telefone"))
        old_resume_name = normalize_duplicate_key(row.get("Nome do arquivo"))
        old_name = normalize_duplicate_key(row.get("Nome completo"))
        old_role = normalize_duplicate_key(row.get("Cargo pretendido"))

        if new_email and old_email and new_email == old_email and new_role == old_role:
            return True

        if new_phone and old_phone and len(new_phone) >= 8 and new_phone == old_phone and new_role == old_role:
            return True

        if new_resume_name and old_resume_name and new_resume_name == old_resume_name and new_role == old_role:
            return True

        if new_name and old_name and new_name == old_name and new_role == old_role:
            return True

    return False


def append_candidate_record(
    values: dict[str, Any],
    banco_path: Path = DEFAULT_BANCO_TALENTOS_XLSX,
    refined_path: Path = DEFAULT_REFINED_XLSX,
) -> str:
    ensure_bank_workbook_structure(
        banco_path=banco_path,
        refined_path=refined_path,
        create_backup=False,
        redistribute_existing=False,
    )

    wb = load_workbook(banco_path)
    all_existing_rows = extract_rows_from_workbook(wb)

    if is_duplicate_candidate(all_existing_rows, values):
        print(
            "[TALENT BANK] Candidato duplicado detectado. Registro ignorado:",
            values.get("Nome completo") or values.get("Email") or values.get("Nome do arquivo"),
        )
        return "DUPLICADO"
    
    candidate_id = safe_str(values.get("ID")) or get_next_candidate_id_from_rows(all_existing_rows)

    values = dict(values)
    values["ID"] = candidate_id

    sheet_name = choose_sheet_name(wb.sheetnames, values)
    ws = wb[sheet_name]
    header_map = build_header_map(ws)

    ws.insert_rows(2, amount=1)

    if ws.max_row >= 3:
        for col in range(1, ws.max_column + 1):
            copy_cell_style(ws.cell(row=3, column=col), ws.cell(row=2, column=col))

    for header, col in header_map.items():
        ws.cell(row=2, column=col, value=values.get(header, ""))

    wb.save(banco_path)
    return candidate_id


def find_rows_missing_core_fields(
    banco_path: Path = DEFAULT_BANCO_TALENTOS_XLSX,
) -> list[dict[str, Any]]:
    wb = load_workbook(banco_path, data_only=True)
    missing = []

    for ws in wb.worksheets:
        header_map = build_header_map(ws)
        headers = get_headers(ws)

        for row_idx in range(2, ws.max_row + 1):
            row_data = {"_sheet": ws.title, "_row": row_idx}
            has_any = False

            for col_idx, header in enumerate(headers, start=1):
                if not header:
                    continue
                value = ws.cell(row=row_idx, column=col_idx).value
                row_data[header] = value
                if value not in (None, ""):
                    has_any = True

            if not has_any:
                continue

            missing_name = not safe_str(row_data.get("Nome completo"))
            missing_age = not safe_str(row_data.get("Idade"))
            missing_location = not safe_str(row_data.get("Localização"))

            has_curriculum = bool(safe_str(row_data.get("Caminho do currículo")))

            if has_curriculum and (missing_name or missing_age or missing_location):
                missing.append(row_data)

    return missing


def update_candidate_row(
    sheet_name: str,
    row_idx: int,
    updates: dict[str, Any],
    banco_path: Path = DEFAULT_BANCO_TALENTOS_XLSX,
    only_if_blank: bool = True,
) -> None:
    wb = load_workbook(banco_path)

    if sheet_name not in wb.sheetnames:
        raise RuntimeError(f"Aba não encontrada: {sheet_name}")

    ws = wb[sheet_name]
    header_map = build_header_map(ws)

    for header, value in updates.items():
        header = normalize_header(header)
        if header not in header_map:
            continue

        col = header_map[header]
        current = ws.cell(row=row_idx, column=col).value

        if only_if_blank and safe_str(current):
            continue

        if safe_str(value):
            ws.cell(row=row_idx, column=col, value=value)

    wb.save(banco_path)
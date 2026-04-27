import imaplib
import email
from email.header import decode_header
from email.utils import parseaddr

import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

import datetime
import re
import json
import unicodedata


from typing import Any

import requests
from dotenv import load_dotenv
from openpyxl import load_workbook
from pypdf import PdfReader
from docx import Document

from datasets.professional_profiles.matching_engine import score_candidate_against_profiles
from rh.talent_bank_workbook import append_candidate_record, ensure_bank_workbook_structure

# =========================
# LOAD .ENV DO BACKEND
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env", override=True)


# =========================
# CONFIG
# =========================

IMAP_SERVER = "imap.gmail.com"

EMAIL_ACCOUNT = os.getenv("STARIA_EMAIL_ACCOUNT", "staria@starmkt.com.br").strip()
EMAIL_PASSWORD = (
    os.getenv("STARIA_EMAIL_PASSWORD", "")
    .strip()
    .replace(" ", "")
    .replace('"', "")
    .replace("'", "")
)

CURRICULOS_DIR = Path(r"G:\Drives compartilhados\STARMKT\StarIA\curriculos")
BANCO_TALENTOS_XLSX = Path(
    r"G:\Drives compartilhados\STARMKT\StarIA\banco_talentos\banco_talentos.xlsx"
)

ALLOWED_EXTS = {".pdf", ".doc", ".docx", ".txt", ".rtf"}

STARIA_API_BASE = os.getenv("STARIA_API_BASE", "http://127.0.0.1:8088").strip()
STARIA_OLLAMA_MODEL = os.getenv("STAR_OLLAMA_MODEL", "star-llama").strip()

DEFAULT_STATUS = "Banco de talentos"
DEFAULT_ORIGEM = "email"


CANDIDATOS_REFINADOS_XLSX = Path(
    os.getenv(
        "CANDIDATOS_REFINADOS_XLSX",
        r"G:\Drives compartilhados\STARMKT\StarIA\banco_talentos\candidatos_refinados.xlsx",
    )
)



# =========================
# UTILS
# =========================


def decode_mime_words(s: str) -> str:
    if not s:
        return ""

    decoded = decode_header(s)
    result = ""

    for part, encoding in decoded:
        if isinstance(part, bytes):
            result += part.decode(encoding or "utf-8", errors="ignore")
        else:
            result += part

    return result


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return normalize_spaces(str(value))


def sanitize_sheet_value(value: Any) -> str:
    s = safe_str(value)
    if not s:
        return ""
    if s.startswith(("=", "+", "-", "@")):
        return "'" + s
    return s


def ensure_paths():
    CURRICULOS_DIR.mkdir(parents=True, exist_ok=True)

    if not BANCO_TALENTOS_XLSX.exists():
        raise RuntimeError(f"Planilha não encontrada: {BANCO_TALENTOS_XLSX}")

    ensure_bank_workbook_structure(
        banco_path=BANCO_TALENTOS_XLSX,
        refined_path=CANDIDATOS_REFINADOS_XLSX,
        create_backup=False,
        redistribute_existing=False,
    )


def decode_sender(sender_raw: str) -> str:
    name, addr = parseaddr(sender_raw or "")
    name = decode_mime_words(name)
    if name and addr:
        return f"{name} <{addr}>"
    return addr or name or ""


# =========================
# EMAIL BODY
# =========================


def extract_email_body(msg) -> str:
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")

            if "attachment" in disposition.lower():
                continue

            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"

                if payload:
                    body += payload.decode(charset, errors="ignore")
    else:
        payload = msg.get_payload(decode=True)
        charset = msg.get_content_charset() or "utf-8"

        if payload:
            body += payload.decode(charset, errors="ignore")

    return body


# =========================
# EXTRACT NÍVEL
# =========================


def extract_level(email_body: str) -> str:
    if not email_body:
        return ""

    text = email_body.lower()

    patterns = [
        r"n[íi]vel\s*:\s*(j[uú]nior|pleno|s[êe]nior)",
        r"senioridade\s*:\s*(j[uú]nior|pleno|s[êe]nior)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = match.group(1)

            if value in ["junior", "júnior"]:
                return "Júnior"
            if value == "pleno":
                return "Pleno"
            if value in ["senior", "sênior"]:
                return "Sênior"

    return ""


# =========================
# EXTRACT PORTFOLIO
# =========================


def extract_portfolio(email_body: str) -> str:
    if not email_body:
        return ""

    pattern = r"(portf[oó]lio|portfolio)\s*:\s*(https?://\S+|\S+\.\S+)"
    match = re.search(pattern, email_body, flags=re.IGNORECASE)

    if match:
        return match.group(2).strip()

    return ""


# =========================
# CONFIG CHECK
# =========================


def validate_config():
    if not EMAIL_ACCOUNT:
        raise RuntimeError("STARIA_EMAIL_ACCOUNT não definido no .env")

    if not EMAIL_PASSWORD:
        raise RuntimeError(
            "STARIA_EMAIL_PASSWORD não definido no .env " f"({BASE_DIR / '.env'})"
        )


# =========================
# CONNECT MAILBOX
# =========================


def connect_mailbox():
    validate_config()
    ensure_paths()

    print("[EMAIL] BASE_DIR =", BASE_DIR)
    print("[EMAIL] EMAIL_ACCOUNT =", EMAIL_ACCOUNT)
    print("[EMAIL] PASSWORD_LOADED =", bool(EMAIL_PASSWORD))
    print("[EMAIL] CURRICULOS_DIR =", CURRICULOS_DIR)
    print("[EMAIL] BANCO_TALENTOS_XLSX =", BANCO_TALENTOS_XLSX)
    print("[EMAIL] STARIA_API_BASE =", STARIA_API_BASE)
    print("[EMAIL] PASSWORD_LENGTH =", len(EMAIL_PASSWORD))

    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)

    print("[EMAIL] Login IMAP realizado com sucesso.")
    return mail


# =========================
# SAVE ATTACHMENTS
# =========================


def save_attachment(msg):
    saved_files = []
    CURRICULOS_DIR.mkdir(parents=True, exist_ok=True)

    for part in msg.walk():
        content_disposition = str(part.get("Content-Disposition") or "")

        if "attachment" not in content_disposition.lower():
            continue

        filename = part.get_filename()
        if not filename:
            continue

        filename = decode_mime_words(filename)
        ext = Path(filename).suffix.lower()

        if ext not in ALLOWED_EXTS:
            print(f"[EMAIL] Anexo ignorado por extensão: {filename}")
            continue

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = f"{timestamp}_{filename}"
        filepath = CURRICULOS_DIR / safe_name

        with open(filepath, "wb") as f:
            f.write(part.get_payload(decode=True))

        print("[EMAIL] Currículo salvo:", filepath)
        saved_files.append(filepath)

    return saved_files


# =========================
# TEXT EXTRACTION
# =========================


def extract_text_from_pdf(file_path: Path) -> str:
    try:
        reader = PdfReader(str(file_path))
        parts = []
        for page in reader.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                parts.append(txt)
        return "\n".join(parts).strip()
    except Exception as e:
        print(f"[EMAIL] Falha ao ler PDF {file_path.name}: {e}")
        return ""


def extract_text_from_docx(file_path: Path) -> str:
    try:
        doc = Document(str(file_path))
        parts = []
        for p in doc.paragraphs:
            txt = p.text or ""
            if txt.strip():
                parts.append(txt)
        return "\n".join(parts).strip()
    except Exception as e:
        print(f"[EMAIL] Falha ao ler DOCX {file_path.name}: {e}")
        return ""


def extract_text_from_file(file_path: Path) -> str:
    ext = file_path.suffix.lower()

    if ext in {".txt", ".rtf"}:
        try:
            return file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            try:
                return file_path.read_text(errors="ignore")
            except Exception as e:
                print(f"[EMAIL] Falha ao ler TXT/RTF {file_path.name}: {e}")
                return ""

    if ext == ".pdf":
        return extract_text_from_pdf(file_path)

    if ext == ".docx":
        return extract_text_from_docx(file_path)

    return ""


# =========================
# CARGO FALLBACK
# =========================


def guess_job_title_from_text(text: str) -> str:
    """
    Heurística simples para cargo quando a IA não preencher.
    """
    if not text:
        return ""

    lines = [normalize_spaces(x) for x in text.splitlines() if normalize_spaces(x)]
    top = lines[:20]

    job_patterns = [
        r"\b(analista [\w\s]+)\b",
        r"\b(assistente [\w\s]+)\b",
        r"\b(coordenador[a]? [\w\s]+)\b",
        r"\b(gerente [\w\s]+)\b",
        r"\b(especialista [\w\s]+)\b",
        r"\b(supervisor[a]? [\w\s]+)\b",
        r"\b(designer(?: gráfico)?(?: [\w\s]+)?)\b",
        r"\b(social media)\b",
        r"\b(marketing(?: [\w\s]+)?)\b",
        r"\b(produtor[a]? de eventos)\b",
        r"\b(eventos corporativos)\b",
        r"\b(planejamento de eventos)\b",
    ]

    for line in top:
        lower = line.lower()
        for pattern in job_patterns:
            m = re.search(pattern, lower, flags=re.IGNORECASE)
            if m:
                return normalize_spaces(m.group(1)).title()

    return ""


# =========================
# AI EXTRACTION
# =========================


def extract_json_block(text: str) -> str:
    if not text:
        return ""

    text = text.strip().replace("```json", "").replace("```", "").strip()

    start = text.find("{")
    end = text.rfind("}")

    if start >= 0 and end > start:
        return text[start : end + 1].strip()

    return text


def extract_candidate_data_with_ai(file_path: Path) -> tuple[dict, str]:
    text = extract_text_from_file(file_path)
    print(f"[EMAIL] Texto extraído de {file_path.name}: {len(text)} caracteres")

    empty = {
        "nome_completo": "",
        "idade": "",
        "localizacao": "",
        "cargo_pretendido": "",
        "habilidades": "",
        "formacoes": "",
        "email": "",
        "telefone": "",
    }

    if not text.strip():
        print(f"[EMAIL] Currículo sem texto extraível: {file_path.name}")
        return empty, text

    prompt = f"""
Extraia do currículo as informações abaixo e devolva APENAS JSON válido.

Campos obrigatórios do JSON:
{{
  "nome_completo": "",
  "idade": "",
  "localizacao": "",
  "cargo_pretendido": "",
  "habilidades": "",
  "formacoes": "",
  "email": "",
  "telefone": ""
}}

Regras:
- Não invente informações.
- Se não encontrar um campo, deixe "".
- Use para "cargo_pretendido" o cargo desejado, headline profissional, título principal do currículo ou área/cargo mais provável explicitamente indicada no topo do documento.
- "habilidades" deve ser uma string curta, com itens separados por "; ".
- "formacoes" deve ser uma string curta, com itens separados por "; ".
- Responda somente com JSON puro, sem markdown, sem explicação.

Nome do arquivo: {file_path.name}

Texto do currículo:
{text[:15000]}
""".strip()

    try:
        payload = {
            "question": prompt,
            "use_rag": False,
            "model": STARIA_OLLAMA_MODEL,
        }

        resp = requests.post(
            f"{STARIA_API_BASE}/ask",
            json=payload,
            timeout=180,
        )
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("answer", "").strip()

        json_text = extract_json_block(answer)
        parsed = json.loads(json_text)

        extracted = {
            "nome_completo": safe_str(parsed.get("nome_completo")),
            "idade": safe_str(parsed.get("idade")),
            "localizacao": safe_str(parsed.get("localizacao")),
            "cargo_pretendido": safe_str(parsed.get("cargo_pretendido")),
            "habilidades": safe_str(parsed.get("habilidades")),
            "formacoes": safe_str(parsed.get("formacoes")),
            "email": safe_str(parsed.get("email")),
            "telefone": safe_str(parsed.get("telefone")),
        }

        if not extracted["cargo_pretendido"]:
            extracted["cargo_pretendido"] = guess_job_title_from_text(text)

        return extracted, text

    except Exception as e:
        print(f"[EMAIL] Falha na extração IA de {file_path.name}: {e}")

        fallback = empty.copy()
        fallback["cargo_pretendido"] = guess_job_title_from_text(text)

        return fallback, text


# =========================
# EXCEL HELPERS
# =========================


def build_header_map(ws) -> dict:
    return {
        safe_str(cell.value): idx + 1
        for idx, cell in enumerate(ws[1])
        if safe_str(cell.value)
    }


def get_next_candidate_id(ws, header_map: dict) -> str:
    id_col = header_map.get("ID")
    if not id_col:
        return "BT0001"

    max_num = 0
    for row in range(2, ws.max_row + 1):
        value = ws.cell(row=row, column=id_col).value
        s = safe_str(value)
        m = re.match(r"BT(\d+)", s, flags=re.IGNORECASE)
        if m:
            max_num = max(max_num, int(m.group(1)))

    return f"BT{max_num + 1:04d}"


def append_candidate_to_sheet(
    file_path: Path,
    sender: str,
    level: str,
    portfolio: str,
    extracted: dict,
    vacancy_title: str = "",
):
    curriculum_text = extract_text_from_file(file_path)

    requested_role = vacancy_title or extracted.get("cargo_pretendido", "")

    match_result = score_candidate_against_profiles(
        candidate=extracted,
        curriculum_text=curriculum_text,
        requested_role=requested_role,
        extra_query=vacancy_title,
    )

    best = match_result.get("best") or {}
    top_matches = match_result.get("top_matches") or []

    nota = int(match_result.get("nota", 0) or 0)

    if nota < 50:
        print(
            f"[EMAIL] Candidato descartado por nota abaixo da corte: "
            f"{extracted.get('nome_completo') or file_path.name} | Nota: {nota}"
        )
        return

    top_matches_text = "; ".join(
        [
            f"{m.get('title', '')} ({m.get('nota', m.get('score_pct', 0))})"
            for m in top_matches
            if m.get("title")
        ]
    )

    final_portfolio = portfolio or ""
    if not final_portfolio:
        final_portfolio = str(file_path)

    values = {
        "Nota": nota,
        "Nome completo": extracted.get("nome_completo", ""),
        "Idade": extracted.get("idade", ""),
        "Localização": extracted.get("localizacao", ""),
        "Cargo pretendido": requested_role,
        "Nível": level or "",
        "Portfólio": final_portfolio,
        "Habilidades": extracted.get("habilidades", ""),
        "Formações": extracted.get("formacoes", ""),
        "Email": extracted.get("email", ""),
        "Telefone": extracted.get("telefone", ""),
        "Caminho do currículo": str(file_path),
        "Nome do arquivo": file_path.name,
        "Data de entrada": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Origem": DEFAULT_ORIGEM,
        "Remetente do email": sender,
        "Status": DEFAULT_STATUS,
        "Observações": "",
        "Role ID sugerido": best.get("role_id", ""),
        "Título normalizado": best.get("title", ""),
        "Top 3 roles aderentes": top_matches_text,
        "Resumo de aderência": match_result.get("summary", ""),
        "Flags de risco": "; ".join(best.get("gaps", [])[:6]),
    }

    candidate_id = append_candidate_record(
        values=values,
        banco_path=BANCO_TALENTOS_XLSX,
        refined_path=CANDIDATOS_REFINADOS_XLSX,
    )

    if candidate_id == "DUPLICADO":
        print(
            f"[EMAIL] Candidato duplicado ignorado: "
            f"{values.get('Nome completo') or values.get('Email') or file_path.name}"
        )
        return

    print(
        f"[EMAIL] Planilha atualizada com candidato {candidate_id}: "
        f"{file_path.name} | Vaga: {requested_role} | "
        f"Nota: {match_result.get('nota', 0)} | Perfil: {best.get('title', '')}"
    )


# =========================
# EMAIL FILTERS
# =========================


def extract_job_title_from_subject(subject: str) -> str:
    """
    Exemplo:
    New application: Diretor(a) de Arte Sênior — Criação (...) from Nome
    """
    if not subject:
        return ""

    text = decode_mime_words(subject)

    patterns = [
        r"new application:\s*(.+?)\s+from\s+.+$",
        r"application received:\s*(.+?)\s+from\s+.+$",
        r"candidate applied:\s*(.+?)\s+from\s+.+$",
        r"candidatura:\s*(.+?)\s+de\s+.+$",
        r"nova candidatura:\s*(.+?)\s+de\s+.+$",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return normalize_spaces(m.group(1))

    return ""


APPLICATION_KEYWORDS = [
    "new application",
    "application received",
    "candidate applied",
    "candidatura",
    "nova candidatura",
    "inscricao",
    "inscrição",
]

RESUME_FILENAME_HINTS = ["cv", "curriculo", "currículo", "resumo", "resume"]


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.lower().strip()


def is_bank_talent_subject(subject: str) -> bool:
    return "banco de talentos" in normalize_text(subject)


def is_linkedin_sender(sender: str) -> bool:
    return "jobs-listings@linkedin.com" in normalize_text(sender)


def has_application_signal(subject: str, body: str) -> bool:
    text = normalize_text(subject + " " + body)
    return any(k in text for k in APPLICATION_KEYWORDS)


def filename_looks_like_cv(filename: str) -> bool:
    name = normalize_text(filename)

    if not any(name.endswith(ext) for ext in ALLOWED_EXTS):
        return False

    return any(hint in name for hint in RESUME_FILENAME_HINTS)


def email_has_cv_attachment(msg) -> bool:
    for part in msg.walk():

        content_disposition = str(part.get("Content-Disposition") or "")
        if "attachment" not in content_disposition.lower():
            continue

        filename = part.get_filename()

        if not filename:
            continue

        filename = decode_mime_words(filename)

        if filename_looks_like_cv(filename):
            return True

    return False


DIRECT_APPLICATION_HINTS = [
    "vaga",
    "curriculo",
    "currículo",
    "candidatura",
    "candidate",
    "application",
    "resume",
]


def has_direct_application_signal(subject: str, body: str) -> bool:
    text = normalize_text(subject + " " + body)
    return any(k in text for k in DIRECT_APPLICATION_HINTS)


DIRECT_APPLICATION_HINTS = [
    "vaga",
    "curriculo",
    "currículo",
    "candidatura",
    "candidate",
    "application",
    "resume",
]


def has_direct_application_signal(subject: str, body: str) -> bool:
    text = normalize_text(subject + " " + body)
    return any(k in text for k in DIRECT_APPLICATION_HINTS)



def should_process_email(msg, subject: str, sender: str, body: str) -> bool:

    legacy_flow = is_bank_talent_subject(subject)

    linkedin_flow = (
        is_linkedin_sender(sender)
        and has_application_signal(subject, body)
        and email_has_cv_attachment(msg)
    )

    direct_cv_flow = email_has_cv_attachment(msg) and has_direct_application_signal(
        subject, body
    )

    return legacy_flow or linkedin_flow or direct_cv_flow


# =========================
# PROCESS INBOX
# =========================


def process_inbox():
    mail = connect_mailbox()
    mail.select("inbox")

    status, messages = mail.search(None, "UNSEEN")

    if status != "OK":
        print("[EMAIL] Erro ao buscar emails.")
        return

    mail_ids = messages[0].split()
    print("[EMAIL] Emails novos encontrados:", len(mail_ids))

    for mail_id in mail_ids:
        status, msg_data = mail.fetch(mail_id, "(RFC822)")

        if status != "OK":
            print("[EMAIL] Falha ao buscar email ID:", mail_id)
            continue

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        subject = decode_mime_words(msg.get("Subject", ""))
        sender_raw = msg.get("From", "")
        sender = decode_sender(sender_raw)

        print("\n[EMAIL] Processando email")
        print("[EMAIL] Assunto:", subject)
        print("[EMAIL] De:", sender)

        body = extract_email_body(msg)

        if not should_process_email(msg, subject, sender, body):

            print("[EMAIL] Ignorado por não atender critérios de currículo")
            continue

        body = extract_email_body(msg)
        level = extract_level(body)
        portfolio = extract_portfolio(body)

        print("[EMAIL] Nível detectado:", level if level else "não informado")
        print(
            "[EMAIL] Portfólio detectado:", portfolio if portfolio else "não informado"
        )


        vacancy_title = extract_job_title_from_subject(subject)

        print(
            "[EMAIL] Vaga detectada:",
            vacancy_title if vacancy_title else "não informada",
        )


        saved = save_attachment(msg)

        if not saved:
            print("[EMAIL] Nenhum currículo válido encontrado")
            continue

        print("[EMAIL] Total de currículos salvos:", len(saved))

        for file_path in saved:
            extracted, raw_text = extract_candidate_data_with_ai(file_path)

            print("[EMAIL] Dados extraídos:")
            print("         Nome:", extracted.get("nome_completo") or "(vazio)")
            print("         Idade:", extracted.get("idade") or "(vazio)")
            print("         Localização:", extracted.get("localizacao") or "(vazio)")
            print(
                "         Cargo pretendido:",
                extracted.get("cargo_pretendido") or "(vazio)",
            )
            print("         Habilidades:", extracted.get("habilidades") or "(vazio)")
            print("         Formações:", extracted.get("formacoes") or "(vazio)")
            print("         Email:", extracted.get("email") or "(vazio)")
            print("         Telefone:", extracted.get("telefone") or "(vazio)")

            has_any_data = any(
                [
                    extracted.get("nome_completo"),
                    extracted.get("idade"),
                    extracted.get("localizacao"),
                    extracted.get("cargo_pretendido"),
                    extracted.get("habilidades"),
                    extracted.get("formacoes"),
                    extracted.get("email"),
                    extracted.get("telefone"),
                ]
            )

            if not has_any_data:
                print(
                    f"[EMAIL] Extração vazia para {file_path.name}. Linha NÃO será gravada na planilha."
                )
                continue

            append_candidate_to_sheet(
                file_path=file_path,
                sender=sender,
                level=level,
                portfolio=portfolio,
                extracted=extracted,
                vacancy_title=vacancy_title,
            )


if __name__ == "__main__":
    process_inbox()

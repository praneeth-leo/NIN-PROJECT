from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, jsonify, flash
)
import csv
import os
import re
import pandas as pd
import tempfile
from barcode import Code128
from barcode.writer import ImageWriter
from contextlib import contextmanager
from datetime import datetime
from flask_talisman import Talisman
from dotenv import load_dotenv
from werkzeug.security import check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
import uuid

if os.name == "nt":
    import msvcrt
else:
    import fcntl



load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
import logging
logging.warning("ADMIN USER: %s", os.getenv("ADMIN_USERNAME"))
logging.warning("ADMIN HASH: %s", os.getenv("ADMIN_PASSWORD_HASH"))
# --------------------------------------------------
# App setup
# --------------------------------------------------
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
# ----------------------------
# ADD CSP CONFIG HERE
# ----------------------------

csp = {
    "default-src": ["'self'"],

    "script-src": [
        "'self'",
        "https://cdn.jsdelivr.net",
        "https://cdnjs.cloudflare.com"
    ],

    "style-src": [
        "'self'",
        "https://cdn.jsdelivr.net",
        "https://cdnjs.cloudflare.com",
        "https://fonts.googleapis.com"
    ],

    "font-src": [
        "'self'",
        "https://fonts.gstatic.com",
        "https://cdnjs.cloudflare.com",
        "https://cdn.jsdelivr.net"
    ],

    "img-src": [
        "'self'",
        "data:"
    ],

    "object-src": ["'none'"],
    "base-uri": ["'self'"],
    "frame-ancestors": ["'none'"],
    "form-action": ["'self'"]
}

Talisman(
    app,
    content_security_policy=csp,
    content_security_policy_nonce_in=['script-src', 'style-src'],
    force_https=False
)

_env_secret = (os.getenv("FLASK_SECRET_KEY") or "").strip()
app.secret_key = _env_secret or "change-this-in-production"
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_SECURE=False,
    SESSION_COOKIE_PATH="/"
)

@app.after_request
def set_security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
    # Let Flask-Talisman own the CSP header so per-request nonces are preserved.
    if request.is_secure:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    # Best-effort removal; production should run behind a server/proxy that suppresses this.
    response.headers.pop("Server", None)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return response

# ---------------- INVESTIGATOR SETTINGS ----------------
INVESTIGATOR_USERNAME_ALIASES = {
    "kriti": "krithi",
}


def normalize_login_username(username):
    normalized = (username or "").strip().casefold()
    return INVESTIGATOR_USERNAME_ALIASES.get(normalized, normalized)


def load_investigator_credentials():
    creds = []
    for idx in range(1, 51):
        username = (os.getenv(f"INVESTIGATOR_USERNAME_{idx}") or "").strip()
        password_hash = (os.getenv(f"INVESTIGATOR_PASSWORD_HASH_{idx}") or "").strip()
        if username and password_hash:
            creds.append((username, password_hash))
    return creds


INVESTIGATOR_CREDENTIALS = load_investigator_credentials()

# ---------------- ADMIN SETTINGS ----------------
ADMIN_USERNAME = (os.getenv("ADMIN_USERNAME") or "").strip()
ADMIN_PASSWORD_HASH = (os.getenv("ADMIN_PASSWORD_HASH") or "").strip()


def load_admin_credentials():
    creds = []
    if ADMIN_USERNAME and ADMIN_PASSWORD_HASH:
        creds.append((ADMIN_USERNAME, ADMIN_PASSWORD_HASH))
    for idx in range(1, 51):
        username = (os.getenv(f"ADMIN_USERNAME_{idx}") or "").strip()
        password_hash = (os.getenv(f"ADMIN_PASSWORD_HASH_{idx}") or "").strip()
        if username and password_hash:
            creds.append((username, password_hash))
    return creds


ADMIN_CREDENTIALS = load_admin_credentials()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FORM_TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "form.html")

PROFILE_CSV = os.path.join(BASE_DIR, "profiles.csv")
RESPONSE_CSV = os.path.join(BASE_DIR, "responses.csv")
RESPONSE_HISTORY_CSV = os.path.join(BASE_DIR, "responses_history.csv")
PROFILE_XLSX = os.path.join(BASE_DIR, "profiles.xlsx")
RESPONSE_XLSX = os.path.join(BASE_DIR, "responses.xlsx")
LINKED_CSV = os.path.join(BASE_DIR, "linked_data.csv")
LINKED_XLSX = os.path.join(BASE_DIR, "linked_data.xlsx")
AUDIT_LOG_CSV = os.path.join(BASE_DIR, "investigator_audit_log.csv")
RESPONSE_SAVE_AUDIT_CSV = os.path.join(BASE_DIR, "response_save_audit.csv")

BARCODE_FOLDER = os.path.join(BASE_DIR, "static", "barcodes")
EXPORT_FOLDER = os.path.join(BASE_DIR, "exports")
BARCODE_LABEL_WIDTH_MM = 40
BARCODE_LABEL_HEIGHT_MM = 20
BARCODE_DPI = 300
LEGACY_PROFILE_ID_ALIASES = {
    "SK300322MATN": "AK140920MATN",
    "AB160621MATN": "SG071121FATN",
    "AA060921FATN": "AK141221MATN",
    "VG290621MATN": "VJ031220MATN",
    "PP040520MATN": "HG110822MATN",
}

# ✅ UPDATED: dob + age_full added
PROFILE_FIELDS = [
    "profile_id",
    "created_at",
    "name",
    "surname",
    "dob",
    "age",
    "age_full",
    "gender",
    "school",
    "location",
    "class",
    "section"
]

RESPONSE_METADATA_FIELDS = ["response_id", "profile_id", "submitted_at"]
FORM_LOOP_EXPANSIONS = {
    "freq_{{ item }}": [
        "freq_green_leafy",
        "freq_jaggery",
        "freq_dates",
        "freq_eggs",
        "freq_meat",
        "freq_fruits",
    ],
    "parent_q_{{ no }}": [f"parent_q_{no}" for no in range(39, 47)],
}


def extract_response_question_fields():
    if not os.path.exists(FORM_TEMPLATE_PATH):
        return []

    with open(FORM_TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    raw_names = re.findall(
        r'<(?:input|select|textarea)\b[^>]*\bname="([^"]+)"',
        template,
        flags=re.IGNORECASE,
    )

    fields = []
    seen = set()
    for name in raw_names:
        expanded_names = FORM_LOOP_EXPANSIONS.get(name, [name])
        for expanded_name in expanded_names:
            if expanded_name == "submit_action" or expanded_name in seen:
                continue
            seen.add(expanded_name)
            fields.append(expanded_name)
    return fields


RESPONSE_FORM_FIELDS = extract_response_question_fields()
RESPONSE_FIELDS = RESPONSE_METADATA_FIELDS + RESPONSE_FORM_FIELDS
HORIBA_RESULT_FIELDS = [
    "MPV",
    "PDW",
    "PLT",
    "THT",
    "HCT",
    "HGB",
    "MCH",
    "MCHC",
    "MCV",
    "RBC",
    "RDW",
    "RDW_SD",
    "GRA#",
    "GRA%",
    "LYM#",
    "LYM%",
    "MON#",
    "MON%",
    "WBC",
]
HORIBA_UPLOAD_FIELD_MAP = {
    "mpv": "MPV",
    "pdw": "PDW",
    "plt": "PLT",
    "tht": "THT",
    "hct": "HCT",
    "hgb": "HGB",
    "mch": "MCH",
    "mchc": "MCHC",
    "mcv": "MCV",
    "rbc": "RBC",
    "rdw": "RDW",
    "rdw_sd": "RDW_SD",
    "gra_num": "GRA#",
    "gra_pct": "GRA%",
    "lym_num": "LYM#",
    "lym_pct": "LYM%",
    "mon_num": "MON#",
    "mon_pct": "MON%",
    "wbc": "WBC",
}

LOCK_TIMEOUT_SECONDS = 20
RESPONSE_SAVE_AUDIT_FIELDS = ["saved_at"] + RESPONSE_FIELDS


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def admin_required():
    return session.get("admin_logged_in") is True


def investigator_required():
    return session.get("investigator_logged_in") is True


def machine_access_required():
    return admin_required() or investigator_required()


def machine_redirect_if_unauthorized():
    if not machine_access_required():
        return redirect(url_for("investigator_login"))
    return None


def append_investigator_audit(event, details):
    actor_type = "investigator" if investigator_required() else ("admin" if admin_required() else "system")
    actor = (
        session.get("investigator_username")
        or session.get("admin_username")
        or "unknown"
    )
    row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "actor_type": actor_type,
        "actor": actor,
        "event": event,
        "details": details,
    }
    file_exists = os.path.exists(AUDIT_LOG_CSV)
    with open(AUDIT_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "actor_type", "actor", "event", "details"],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def update_excel_files():
    try:
        if os.path.exists(PROFILE_CSV):
            normalized_profiles = sort_profile_rows_by_created_at(
                normalize_profile_storage(write_back=True),
                newest_first=True,
            )
            profile_df = pd.DataFrame(normalized_profiles, columns=PROFILE_FIELDS).fillna("")
            with pd.ExcelWriter(PROFILE_XLSX, engine="openpyxl") as writer:
                profile_df.to_excel(writer, sheet_name="Profiles", index=False)
        if os.path.exists(RESPONSE_CSV):
            normalized_rows = normalize_response_storage(write_back=True)
            response_df = pd.DataFrame(normalized_rows, columns=RESPONSE_FIELDS).fillna("")
            with pd.ExcelWriter(RESPONSE_XLSX, engine="openpyxl") as writer:
                response_df.to_excel(writer, sheet_name="Responses", index=False)
        if os.path.exists(RESPONSE_SAVE_AUDIT_CSV):
            audit_xlsx = os.path.join(BASE_DIR, "response_save_audit.xlsx")
            audit_rows = sort_rows_by_timestamp(
                read_csv_as_dict_list(RESPONSE_SAVE_AUDIT_CSV),
                timestamp_key="saved_at",
                newest_first=True,
            )
            audit_df = pd.DataFrame(audit_rows, columns=RESPONSE_SAVE_AUDIT_FIELDS).fillna("")
            with pd.ExcelWriter(audit_xlsx, engine="openpyxl") as writer:
                audit_df.to_excel(writer, sheet_name="SaveProgressAudit", index=False)
    except Exception as e:
        print("Excel error:", e)


def update_linked_excel_file():
    try:
        if os.path.exists(LINKED_CSV):
            pd.read_csv(LINKED_CSV).to_excel(LINKED_XLSX, index=False)
    except Exception as e:
        print("Linked excel error:", e)


def _lock_path_for(target_path):
    base_name = os.path.basename(target_path)
    return os.path.join(BASE_DIR, f".{base_name}.lock")


@contextmanager
def locked_file_access(target_path, mode="r", timeout_seconds=LOCK_TIMEOUT_SECONDS):
    lock_path = _lock_path_for(target_path)
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "a+b") as lock_file:
        start_time = datetime.now()
        while True:
            try:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if (datetime.now() - start_time).total_seconds() >= timeout_seconds:
                    raise TimeoutError(f"Timed out waiting for file lock on {target_path}")
        try:
            # Only the sidecar lock file stays open here. On Windows, keeping the
            # target CSV open blocks os.replace() when we atomically rewrite it.
            yield target_path
        finally:
            try:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass


def read_csv_as_dict_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            clean = {}
            for k, v in row.items():
                if k is None:
                    continue
                clean[str(k).strip()] = v
            rows.append(clean)
        return rows


def write_dict_list_to_csv(path, rows, fieldnames):
    target_dir = os.path.dirname(path) or "."
    fd, temp_path = tempfile.mkstemp(prefix="tmp_", suffix=".csv", dir=target_dir)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        try:
            os.replace(temp_path, path)
        except PermissionError:
            # OneDrive/Windows can briefly deny atomic replacement even when we
            # hold our own sidecar lock. Fall back to rewriting in place.
            with open(temp_path, "r", newline="", encoding="utf-8") as src:
                contents = src.read()
            with open(path, "w", newline="", encoding="utf-8") as dst:
                dst.write(contents)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def sort_rows_by_timestamp(rows, timestamp_key, newest_first=False):
    dated_rows = []
    undated_rows = []

    for row in rows:
        timestamp_value = (row.get(timestamp_key, "") or "").strip()
        try:
            parsed = datetime.strptime(timestamp_value, "%Y-%m-%d %H:%M:%S")
            dated_rows.append((parsed, row))
        except ValueError:
            undated_rows.append(row)

    dated_rows.sort(key=lambda item: item[0], reverse=newest_first)
    undated_rows.sort(
        key=lambda row: (
            (row.get(timestamp_key, "") or "").strip().casefold(),
            (row.get("profile_id", "") or "").strip().casefold(),
        )
    )
    return [row for _, row in dated_rows] + undated_rows


def upsert_response_save_audit(row):
    profile_id = normalize_profile_id_value(row.get("profile_id", ""))
    if not profile_id:
        return

    audit_row = {field: row.get(field, "") for field in RESPONSE_FIELDS}
    audit_row["profile_id"] = profile_id
    audit_row["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with locked_file_access(RESPONSE_SAVE_AUDIT_CSV, mode="a+"):
        existing_rows = read_csv_as_dict_list(RESPONSE_SAVE_AUDIT_CSV)
        updated_rows = []
        replaced = False
        for existing in existing_rows:
            existing_profile_id = normalize_profile_id_value(existing.get("profile_id", ""))
            if existing_profile_id == profile_id:
                updated_rows.append(audit_row)
                replaced = True
            else:
                updated_rows.append({field: existing.get(field, "") for field in RESPONSE_SAVE_AUDIT_FIELDS})
        if not replaced:
            updated_rows.append(audit_row)

        ordered_rows = sort_rows_by_timestamp(updated_rows, timestamp_key="saved_at", newest_first=True)
        write_dict_list_to_csv(RESPONSE_SAVE_AUDIT_CSV, ordered_rows, RESPONSE_SAVE_AUDIT_FIELDS)


def _normalized_profile_value(value):
    return " ".join(str(value or "").strip().casefold().split())


def build_profile_identity_key(profile):
    return (
        _normalized_profile_value(profile.get("name", "")),
        _normalized_profile_value(profile.get("surname", "")),
        (profile.get("dob", "") or "").strip(),
        _normalized_profile_value(profile.get("gender", "")),
        _normalized_profile_value(profile.get("school", "")),
        _normalized_profile_value(profile.get("location", "")),
    )


def normalize_profile_id_value(profile_id):
    return re.sub(r"[^A-Za-z0-9]", "", (profile_id or "")).upper()


def validate_profile_row(profile_row):
    required_labels = {
        "name": "Name",
        "dob": "DOB",
        "gender": "Gender",
        "school": "School",
        "location": "Location",
    }
    missing = [label for key, label in required_labels.items() if not str(profile_row.get(key, "")).strip()]
    if missing:
        return f"Please fill all required profile fields: {', '.join(missing)}."
    return None


def sanitize_profile_row(row, fallback_created_at=""):
    clean_row = {key: row.get(key, "") for key in PROFILE_FIELDS}
    clean_row["profile_id"] = normalize_profile_id_value(clean_row.get("profile_id", ""))
    created_at = (clean_row.get("created_at", "") or "").strip()
    if not created_at:
        clean_row["created_at"] = fallback_created_at
    return clean_row


def resolve_profile_id_alias(profile_id):
    pid = normalize_profile_id_value(profile_id)
    return LEGACY_PROFILE_ID_ALIASES.get(pid, pid)


def normalize_profile_storage(rows=None, write_back=False):
    profile_rows = rows if rows is not None else read_csv_as_dict_list(PROFILE_CSV)

    unique_profiles = {}

    for row in profile_rows:
        pid = normalize_profile_id_value(row.get("profile_id", ""))

        if not pid:
            continue

        clean_row = sanitize_profile_row(row)

        # ✅ Keep latest occurrence (last wins)
        unique_profiles[pid] = clean_row

    normalized_rows = list(unique_profiles.values())

    if write_back:
        write_dict_list_to_csv(PROFILE_CSV, normalized_rows, PROFILE_FIELDS)

    return normalized_rows


def find_profile_by_id(profile_id, rows=None):
    pid = resolve_profile_id_alias(profile_id)
    if not pid:
        return None
    profiles = rows if rows is not None else read_csv_as_dict_list(PROFILE_CSV)
    for row in profiles:
        row_pid = normalize_profile_id_value(row.get("profile_id", ""))
        if row_pid == pid:
            return row
    return None


def profile_id_exists(profile_id, rows=None):
    return find_profile_by_id(profile_id, rows=rows) is not None


def profile_display_name(profile):
    return f"{(profile.get('name', '') or '').strip()} {(profile.get('surname', '') or '').strip()}".strip()


def bind_response_identity_from_profile(row, profile):
    if not profile:
        return row
    row["profile_id"] = (profile.get("profile_id", "") or row.get("profile_id", "") or "").strip().upper()
    row["study_id"] = row["profile_id"]
    row["child_id_code"] = row["profile_id"]
    row["participant_name"] = profile_display_name(profile)
    row["dob"] = (profile.get("dob", "") or "").strip()
    row["sex"] = (profile.get("gender", "") or "").strip()
    row["school_anganwadi_name"] = (profile.get("school", "") or "").strip()
    row["location_type"] = (profile.get("location", "") or "").strip()
    return row


def sync_response_identifiers(row):
    profile_id = (row.get("profile_id", "") or "").strip().upper()
    if not profile_id:
        return row
    row["profile_id"] = profile_id
    row["study_id"] = profile_id
    row["child_id_code"] = profile_id
    return row


def sanitize_response_row(row, profile_lookup=None):
    clean_row = {key: row.get(key, "") for key in RESPONSE_FIELDS}
    clean_row = sync_response_identifiers(clean_row)
    profile_id = (clean_row.get("profile_id", "") or "").strip().upper()
    if profile_id and profile_lookup:
        profile = profile_lookup.get(profile_id)
        if profile:
            clean_row = bind_response_identity_from_profile(clean_row, profile)
    if not (clean_row.get("response_id", "") or "").strip():
        clean_row["response_id"] = str(uuid.uuid4())
    return clean_row


def normalize_response_storage(rows=None, write_back=False):
    response_rows = rows if rows is not None else read_csv_as_dict_list(RESPONSE_CSV)
    profile_lookup = {}
    for profile in read_csv_as_dict_list(PROFILE_CSV):
        profile_id = re.sub(r"[^A-Za-z0-9]", "", (profile.get("profile_id", "") or "")).upper()
        if profile_id:
            profile_lookup[profile_id] = profile
    normalized_rows = [sanitize_response_row(row, profile_lookup=profile_lookup) for row in response_rows]
    if write_back:
        write_dict_list_to_csv(RESPONSE_CSV, normalized_rows, RESPONSE_FIELDS)
    return normalized_rows


def rewrite_responses_in_submitted_order(newest_first=False):
    sorted_rows = sort_response_rows_by_submitted_at(
        normalize_response_storage(),
        newest_first=newest_first,
    )
    write_dict_list_to_csv(RESPONSE_CSV, sorted_rows, RESPONSE_FIELDS)
    return sorted_rows


def sort_response_rows_by_submitted_at(rows, newest_first=False):
    dated_rows = []
    undated_rows = []

    for row in rows:
        submitted_at = (row.get("submitted_at", "") or "").strip()
        try:
            parsed = datetime.strptime(submitted_at, "%Y-%m-%d %H:%M:%S")
            dated_rows.append((parsed, row))
        except ValueError:
            undated_rows.append(row)

    dated_rows.sort(key=lambda item: item[0], reverse=newest_first)
    undated_rows.sort(
        key=lambda row: (
            (row.get("submitted_at", "") or "").strip().casefold(),
            (row.get("profile_id", "") or "").strip().casefold(),
        )
    )
    return [row for _, row in dated_rows] + undated_rows


def sort_profile_rows_by_created_at(rows, newest_first=False):
    dated_rows = []
    undated_rows = []

    for row in rows:
        created_at = (row.get("created_at", "") or "").strip()
        try:
            parsed = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
            dated_rows.append((parsed, row))
        except ValueError:
            undated_rows.append(row)

    dated_rows.sort(key=lambda item: item[0], reverse=newest_first)
    undated_rows.sort(
        key=lambda row: (
            (row.get("created_at", "") or "").strip().casefold(),
            (row.get("profile_id", "") or "").strip().casefold(),
        )
    )
    return [row for _, row in dated_rows] + undated_rows


def deduplicate_response_rows(rows):
    latest_by_profile = {}
    blank_profile_rows = []

    for row in sort_response_rows_by_submitted_at(normalize_response_storage(rows=rows)):
        profile_id = (row.get("profile_id", "") or "").strip().upper()
        if not profile_id:
            blank_profile_rows.append(row)
            continue
        latest_by_profile[profile_id] = row

    deduped_rows = list(latest_by_profile.values()) + blank_profile_rows
    return sort_response_rows_by_submitted_at(deduped_rows)



def upsert_response_row(rows, row):
    row_profile_id = normalize_profile_id_value(row.get("profile_id", ""))

    if not row_profile_id:
        rows.append(row)
        return rows

    updated = False

    for i, existing in enumerate(rows):
        existing_id = normalize_profile_id_value(existing.get("profile_id", ""))

        if existing_id == row_profile_id:
            row["response_id"] = existing.get("response_id") or row.get("response_id")
            row["profile_id"] = row_profile_id
            rows[i] = row
            updated = True
            break

    if not updated:
        row["profile_id"] = row_profile_id
        rows.append(row)

    return rows


def read_uploaded_response_rows(path):
    if path.lower().endswith(".xlsx"):
        df = pd.read_excel(path, dtype=str).fillna("")
        return df.to_dict(orient="records")

    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return df.to_dict(orient="records")


def build_ordered_fieldnames(rows, preferred=None):
    preferred = preferred or []
    seen = set()
    fields = []

    for key in preferred:
        if key not in seen:
            seen.add(key)
            fields.append(key)

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fields.append(key)
    return fields


def build_linked_view_data():
    profiles = normalize_profile_storage(write_back=True)
    linked_rows = read_csv_as_dict_list(LINKED_CSV)
    responses = normalize_response_storage()

    profile_map = {}
    for p in profiles:
        pid = (p.get("profile_id", "") or "").strip().upper()
        if pid:
            profile_map[pid] = p

    # Keep latest response row per profile (last row in CSV wins)
    response_map = {}
    for r in responses:
        pid = (r.get("profile_id", "") or "").strip().upper()
        if pid:
            response_map[pid] = r

    # Start with linked rows (if available), fallback to all profiles
    base_rows = []
    seen = set()
    if linked_rows:
        for lr in linked_rows:
            pid = (lr.get("profile_id", "") or "").strip().upper()
            if pid and pid not in seen:
                seen.add(pid)
                base_rows.append(lr)
    else:
        for p in profiles:
            pid = (p.get("profile_id", "") or "").strip().upper()
            if pid and pid not in seen:
                seen.add(pid)
                base_rows.append({"profile_id": pid})

    # Include profiles that exist but were not in linked_data.csv
    for p in profiles:
        pid = (p.get("profile_id", "") or "").strip().upper()
        if pid and pid not in seen:
            seen.add(pid)
            base_rows.append({"profile_id": pid, "profile_found": "yes"})

    # Include responses even when profiles/linked rows are missing.
    for r in responses:
        pid = (r.get("profile_id", "") or "").strip().upper()
        if pid and pid not in seen:
            seen.add(pid)
            base_rows.append({"profile_id": pid, "profile_found": "no"})

    merged = []
    for base in base_rows:
        row = dict(base)
        pid = (row.get("profile_id", "") or "").strip().upper()
        if not pid:
            continue
        row["profile_id"] = pid
        row["study_id"] = pid
        row["child_id_code"] = pid

        p = profile_map.get(pid, {})
        resp = response_map.get(pid, {})
        row.setdefault("profile_found", "yes" if p else "no")
        row.setdefault("name", p.get("name") or resp.get("participant_name", ""))
        row.setdefault("school", p.get("school") or resp.get("school_anganwadi_name", ""))
        row.setdefault("class", p.get("class", ""))
        row.setdefault("section", p.get("section", ""))
        row.setdefault("horiba", "")
        for field in HORIBA_RESULT_FIELDS:
            row.setdefault(field, "")

        # Bring latest response fields into linked view
        for k, v in resp.items():
            if k is None:
                continue
            key = str(k).strip()
            if not key:
                continue
            if key not in row:
                row[key] = v
            elif (row.get(key, "") in ["", None]) and v not in ["", None]:
                row[key] = v

        merged.append(row)

    # Ensure key machine columns are always visible
    preferred_prefix = [
        "profile_id", "profile_found", "name", "school", "class", "section",
        "submitted_at", "response_id",
    ]
    all_keys = set()
    for r in merged:
        all_keys.update(r.keys())
    all_keys.add("horiba")
    for field in HORIBA_RESULT_FIELDS:
        all_keys.add(field)
    trailing_fields = ["horiba"] + HORIBA_RESULT_FIELDS
    extra_keys = [k for k in sorted(all_keys) if k not in preferred_prefix and k not in trailing_fields]
    headers = [k for k in preferred_prefix if k in all_keys] + extra_keys + [k for k in trailing_fields if k in all_keys]

    normalized = []
    for r in merged:
        normalized.append({h: r.get(h, "") for h in headers})

    return normalized, headers


def save_linked_rows(rows):
    preferred_prefix = [
        "profile_id", "profile_found", "name", "school", "class", "section",
        "submitted_at", "response_id",
    ]
    trailing_fields = ["horiba"] + HORIBA_RESULT_FIELDS
    all_fields = build_ordered_fieldnames(rows)
    extra_fields = [field for field in all_fields if field not in preferred_prefix and field not in trailing_fields]
    fields = [field for field in preferred_prefix if field in all_fields] + extra_fields + [
        field for field in trailing_fields if field in all_fields
    ]
    normalized_rows = []
    for row in rows:
        normalized_row = {k: row.get(k, "") for k in fields}
        normalized_row = sync_response_identifiers(normalized_row)
        normalized_rows.append(normalized_row)
    write_dict_list_to_csv(LINKED_CSV, normalized_rows, fields)
    update_linked_excel_file()


def delete_profile_related_data(profile_id):
    profile_id = (profile_id or "").strip().upper()
    if not profile_id:
        return False

    deleted_any = False

    profiles = normalize_profile_storage(write_back=True)
    if profiles:
        profiles_new = [p for p in profiles if p.get("profile_id", "").strip().upper() != profile_id]
        if len(profiles_new) != len(profiles):
            write_dict_list_to_csv(PROFILE_CSV, profiles_new, PROFILE_FIELDS)
            deleted_any = True

    responses = normalize_response_storage()
    if responses:
        responses_new = [r for r in responses if r.get("profile_id", "").strip().upper() != profile_id]
        if len(responses_new) != len(responses):
            write_dict_list_to_csv(RESPONSE_CSV, responses_new, RESPONSE_FIELDS)
            deleted_any = True

    linked_rows = read_csv_as_dict_list(LINKED_CSV)
    if linked_rows:
        linked_new = [r for r in linked_rows if r.get("profile_id", "").strip().upper() != profile_id]
        if len(linked_new) != len(linked_rows):
            linked_fields = build_ordered_fieldnames(
                linked_new,
                preferred=[
                    "profile_id", "profile_found", "name", "school", "class", "section",
                    "submitted_at", "response_id", "horiba",
                ],
            ) if linked_new else [
                "profile_id", "profile_found", "name", "school", "class", "section",
                "submitted_at", "response_id", "horiba",
            ]
            write_dict_list_to_csv(LINKED_CSV, linked_new, linked_fields)
            deleted_any = True

    barcode_path = os.path.join(BARCODE_FOLDER, f"{profile_id}.png")
    if os.path.exists(barcode_path):
        os.remove(barcode_path)
        deleted_any = True

    if deleted_any:
        update_excel_files()
        update_linked_excel_file()

    return deleted_any


def _normalized_df_columns(df):
    normalized = []
    for col in df.columns:
        raw_col = str(col).strip()
        raw_compact = re.sub(r"\s+", "", raw_col).upper()
        special_map = {
            "GRA#": "gra_num",
            "GRA%": "gra_pct",
            "LYM#": "lym_num",
            "LYM%": "lym_pct",
            "MON#": "mon_num",
            "MON%": "mon_pct",
        }
        if raw_compact in special_map:
            normalized.append(special_map[raw_compact])
            continue

        key = raw_col.lower()
        key = re.sub(r"\s+", "_", key)
        key = re.sub(r"[^a-z0-9_]", "", key)
        normalized.append(key)
    df.columns = normalized
    return df


def _find_profile_id_column(df):
    for candidate in ["profile_id", "barcode", "barcode_id", "participant_id", "sampleid", "sample_id", "id"]:
        if candidate in df.columns:
            return candidate
    return None


def _find_machine_value_column(df, machine_key, profile_col):
    if machine_key in df.columns:
        return machine_key
    machine_like = [c for c in df.columns if machine_key in c]
    if machine_like:
        return machine_like[0]
    candidates = [c for c in df.columns if c != profile_col]
    if len(candidates) == 1:
        return candidates[0]
    return None


def profile_exists(profile):
    if not os.path.exists(PROFILE_CSV):
        return False

    target_key = build_profile_identity_key(profile)
    with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if build_profile_identity_key(row) == target_key:
                return True
    return False


def find_profile_by_identity(profile, rows=None):
    target_key = build_profile_identity_key(profile)
    profiles = rows if rows is not None else read_csv_as_dict_list(PROFILE_CSV)
    for row in profiles:
        if build_profile_identity_key(row) == target_key:
            return row
    return None


def _first_alpha(text, default="X"):
    for ch in (text or "").strip().upper():
        if ch.isalpha():
            return ch
    return default


def _school_two_letter_code(text):
    words = [w for w in re.findall(r"[A-Za-z]+", (text or "").upper()) if w]
    if len(words) >= 2:
        return f"{words[0][0]}{words[1][0]}"
    joined = "".join(words)
    if len(joined) >= 2:
        return joined[:2]
    return joined.ljust(2, "X")


def _dob_code(dob):
    dob = (dob or "").strip()
    try:
        dt = datetime.strptime(dob, "%Y-%m-%d")
        return dt.strftime("%d%m%y")
    except Exception:
        digits = "".join(ch for ch in dob if ch.isdigit())
        if len(digits) >= 6:
            return digits[-6:]
        return digits.rjust(6, "0")


def _calculate_age_years(dob):
    dob = (dob or "").strip()
    if not dob:
        return None
    try:
        birth = datetime.strptime(dob, "%Y-%m-%d").date()
    except Exception:
        return None
    today = datetime.now().date()
    years = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
    return years


def generate_profile_id(name, surname, dob, gender, school, location):
    # Requested format:
    # Name first letter + Surname first letter + DOB(DDMMYY) + Gender first letter + School first 2 letters + Location first letter
    return (
        f"{_first_alpha(name)}"
        f"{_first_alpha(surname)}"
        f"{_dob_code(dob)}"
        f"{_first_alpha(gender)}"
        f"{_school_two_letter_code(school)}"
        f"{_first_alpha(location)}"
    )


def generate_unique_profile_id(name, surname, dob, gender, school, location, existing_rows=None):
    base_id = generate_profile_id(name, surname, dob, gender, school, location)
    rows = existing_rows if existing_rows is not None else normalize_profile_storage(write_back=True)
    existing_ids = {
        normalize_profile_id_value(row.get("profile_id", ""))
        for row in rows
        if (row.get("profile_id", "") or "").strip()
    }

    if base_id not in existing_ids:
        return base_id

    for suffix in range(1, 1000):
        candidate = f"{base_id}{suffix:02d}"
        if candidate not in existing_ids:
            return candidate

    raise ValueError("Unable to generate a unique profile ID.")


def generate_barcode(profile_id):
    barcode = Code128(profile_id, writer=ImageWriter())
    path = os.path.join(BARCODE_FOLDER, profile_id)
    barcode.save(path, options={
        "module_width": 0.16,
        "module_height": 8.0,
        "quiet_zone": 0.8,
        "font_size": 0,
        "text_distance": 1,
        "write_text": False,
        "dpi": BARCODE_DPI,
    })
    return f"barcodes/{profile_id}.png"


def ensure_barcode_image(profile_id):
    pid = re.sub(r"[^A-Za-z0-9]", "", (profile_id or "")).upper()
    if not pid:
        return ""
    barcode_file = f"{pid}.png"
    barcode_fs_path = os.path.join(BARCODE_FOLDER, barcode_file)
    if not os.path.exists(barcode_fs_path):
        try:
            os.makedirs(BARCODE_FOLDER, exist_ok=True)
            generate_barcode(pid)
        except Exception:
            return ""
    return f"barcodes/{barcode_file}" if os.path.exists(barcode_fs_path) else ""


# --------------------------------------------------
# USER SECTION
# --------------------------------------------------
@app.route("/")
def home():
    return redirect(url_for("login"))


@app.route("/investigator-login", methods=["GET", "POST"])
def investigator_login():
    next_url = (request.args.get("next", "") or request.form.get("next", "")).strip()
    if next_url and not next_url.startswith("/"):
        next_url = ""
    default_next = url_for("form") if session.get("profile_id") else url_for("dashboard")

    if investigator_required():
        return redirect(next_url or default_next)

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        username_norm = normalize_login_username(username)
        password = request.form.get("password", "").strip()

        valid = any(
            username_norm == normalize_login_username(inv_user) and check_password_hash(inv_pass_hash, password)
            for inv_user, inv_pass_hash in INVESTIGATOR_CREDENTIALS
            if inv_user and inv_pass_hash
        )
        if valid:
            session["investigator_logged_in"] = True
            session["investigator_username"] = username
            append_investigator_audit("login", "Investigator logged in")
            return redirect(next_url or default_next)
        return render_template("investigator_login.html", error="Invalid investigator credentials")

    return render_template("investigator_login.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        raw_id = request.form.get("profile_id", "")
        entered_id = resolve_profile_id_alias(raw_id)

        if entered_id == "":
            return render_template("login.html", error="Please enter Barcode ID")

        if not os.path.exists(PROFILE_CSV):
            return render_template("login.html", error="No profiles found. Please create a profile first.")

        matched_profile = find_profile_by_id(entered_id)
        if not matched_profile:
            return render_template("login.html", error=f"Invalid Barcode ID: {entered_id}")

        session["profile_id"] = (matched_profile.get("profile_id", "") or entered_id).strip().upper()
        session["scanned_profile"] = matched_profile or {}
        return redirect(url_for("profile_details"))

    return render_template("login.html")


@app.route("/profile-details")
def profile_details():
    profile_id = session.get("profile_id", "").strip().upper()
    if not profile_id:
        return redirect(url_for("login"))

    profile = session.get("scanned_profile") or {}
    cached_profile_id = re.sub(r"[^A-Za-z0-9]", "", (profile.get("profile_id", "") or "")).upper()
    if cached_profile_id != profile_id:
        profile = {}

    if not profile:
        profile = find_profile_by_id(profile_id)
        if profile:
            session["scanned_profile"] = profile

    if not profile:
        return render_template("login.html", error="Profile not found for scanned barcode.")

    barcode_path = ensure_barcode_image(profile_id)

    return render_template(
        "profile_details.html",
        profile=profile,
        profile_id=profile_id,
        barcode_path=barcode_path,
    )


@app.route("/profile-details/<profile_id>")
def profile_details_by_id(profile_id):
    pid = resolve_profile_id_alias(profile_id)
    if not pid:
        return render_template("login.html", error="Invalid Barcode ID.")

    profile = find_profile_by_id(pid)

    if not profile:
        return render_template("login.html", error=f"Profile not found for Barcode ID: {pid}")

    barcode_path = ensure_barcode_image(pid)

    return render_template(
        "profile_details.html",
        profile=profile,
        profile_id=pid,
        barcode_path=barcode_path,
    )


@app.route("/dashboard")
def dashboard():
    if not session.get("profile_id"):
        return redirect(url_for("login"))
    return render_template("dashboard.html")


@app.route("/profile", methods=["GET", "POST"])
def profile():
    form_data = {}
    if request.method == "POST":
        form = request.form.to_dict()
        form_data = dict(form)

        profile_row = {
            "profile_id": "",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "name": form.get("name", "").strip(),
            "surname": form.get("surname", "").strip(),
            "dob": form.get("dob", "").strip(),
            "age": form.get("age", "").strip(),
            "age_full": form.get("age_full", "").strip(),
            "gender": form.get("gender", "").strip(),
            "school": form.get("school", "").strip(),
            "location": form.get("location", "").strip(),
            "class": form.get("class", "").strip(),
            "section": form.get("section", "").strip(),
        }

        validation_error = validate_profile_row(profile_row)
        if validation_error:
            return render_template("profile.html", error_message=validation_error, form_data=form_data)

        age_years = _calculate_age_years(profile_row["dob"])
        if age_years is None:
            return render_template("profile.html", error_message="Invalid DOB. Please enter a valid date of birth.", form_data=form_data)
        if age_years not in [3, 4, 5]:
            return render_template("profile.html", error_message="Only ages 3, 4, and 5 are allowed.", form_data=form_data)

        try:
            with locked_file_access(PROFILE_CSV, mode="a+"):
                existing_rows = normalize_profile_storage(write_back=True)

                existing_profile = find_profile_by_identity(profile_row, rows=existing_rows)
                if existing_profile:
                    return render_template(
                        "profile.html",
                        error_message="Profile already exists. Use the exact existing profile ID shown below instead of creating a new one.",
                        existing_profile=existing_profile,
                        existing_barcode_path=ensure_barcode_image(existing_profile.get("profile_id", "")),
                        form_data=form_data,
                    )

                profile_id = generate_unique_profile_id(
                    profile_row["name"],
                    profile_row["surname"],
                    profile_row["dob"],
                    profile_row["gender"],
                    profile_row["school"],
                    profile_row["location"],
                    existing_rows=existing_rows,
                )
                if profile_id_exists(profile_id, rows=existing_rows):
                    return render_template(
                        "profile.html",
                        error_message="Profile ID already exists. Please use the existing participant barcode/profile.",
                        existing_profile=find_profile_by_id(profile_id, rows=existing_rows),
                        existing_barcode_path=ensure_barcode_image(profile_id),
                        form_data=form_data,
                    )
                profile_row["profile_id"] = profile_id

                existing_rows.append({k: profile_row.get(k, "") for k in PROFILE_FIELDS})
                write_dict_list_to_csv(PROFILE_CSV, existing_rows, PROFILE_FIELDS)
        except TimeoutError:
            return render_template(
                "profile.html",
                error_message="Profile data is busy right now. Please try again in a few seconds.",
                form_data=form_data,
                existing_profile=None,
                existing_barcode_path="",
            )
        except Exception:
            app.logger.exception("Profile creation failed while saving profile data")
            return render_template(
                "profile.html",
                error_message="Unable to create profile right now. Please try again.",
                form_data=form_data,
                existing_profile=None,
                existing_barcode_path="",
            )

        try:
            update_excel_files()
        except Exception:
            app.logger.exception("Profile created but Excel export refresh failed")

        barcode_path = ""
        try:
            barcode_path = generate_barcode(profile_id)
        except Exception:
            app.logger.exception("Profile created but barcode generation failed for %s", profile_id)

        session["profile_id"] = profile_id
        session["scanned_profile"] = dict(profile_row)

        return render_template(
            "profile_view.html",
            profile_id=profile_id,
            barcode_path=barcode_path,
            profile_name=f"{profile_row.get('name', '').strip()} {profile_row.get('surname', '').strip()}".strip(),
            profile_notice="Use this exact Profile ID every time. Similar children may have IDs ending in 01, 02, etc.",
        )

    return render_template("profile.html", error_message="", form_data=form_data, existing_profile=None, existing_barcode_path="")


@app.route("/form", methods=["GET", "POST"])
def form():
    profile_id = session.get("profile_id")
    investigator_username = (session.get("investigator_username") or "").strip()
    if not profile_id:
        return redirect(url_for("login"))
    profile = find_profile_by_id(profile_id)
    if not profile:
        session.pop("profile_id", None)
        session.pop("scanned_profile", None)
        return redirect(url_for("login"))
    if not investigator_required():
        return redirect(url_for("investigator_login", next=url_for("form")))

    if request.method == "POST":
        raw_answers = request.form.to_dict(flat=False)
        answers = {}
        for key, values in raw_answers.items():
            if not values:
                answers[key] = ""
            elif len(values) == 1:
                answers[key] = values[0]
            else:
                answers[key] = "; ".join([str(v).strip() for v in values if str(v).strip()])

        submit_action = (answers.pop("submit_action", "submit_questionnaire") or "").strip()

        def normalize_single_text(value):
            parts = [p.strip() for p in str(value or "").split(";") if p.strip()]
            if not parts:
                return ""
            seen = set()
            unique = []
            for part in parts:
                key = part.casefold()
                if key in seen:
                    continue
                seen.add(key)
                unique.append(part)
            return unique[0]

        if not (answers.get("investigator_username", "") or "").strip():
            answers["investigator_username"] = investigator_username
        if not (answers.get("investigator_name", "") or "").strip():
            answers["investigator_name"] = investigator_username
        answers["investigator_name"] = normalize_single_text(answers.get("investigator_name", ""))
        answers["investigator_signature"] = normalize_single_text(answers.get("investigator_signature", ""))

        # Keep IFA dose consistent with questionnaire formula: 3 * body weight / 20 (ml/day)
        weight_value = (answers.get("weight_kgs", "") or answers.get("weight_kg", "") or "").strip()
        try:
            weight = float(weight_value)
            if weight > 0:
                dose = round((3 * weight) / 20, 2)
                answers["ifa_dose"] = f"{dose:g} ml/day"
            else:
                answers["ifa_dose"] = ""
        except (TypeError, ValueError):
            answers["ifa_dose"] = ""

        answers["profile_id"] = profile_id
        answers["submitted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        answers = bind_response_identity_from_profile(answers, profile)
        response_row = sanitize_response_row(answers)

# 🟡 STEP 1: SAVE FULL HISTORY
        if not os.path.exists(RESPONSE_HISTORY_CSV):
            write_dict_list_to_csv(RESPONSE_HISTORY_CSV, [], RESPONSE_FIELDS)

        with locked_file_access(RESPONSE_HISTORY_CSV, mode="a+"):
            history_rows = read_csv_as_dict_list(RESPONSE_HISTORY_CSV)
            history_rows.append(response_row)
            write_dict_list_to_csv(RESPONSE_HISTORY_CSV, history_rows, RESPONSE_FIELDS)

# 🟢 STEP 2: SAVE ONLY LATEST
        with locked_file_access(RESPONSE_CSV, mode="a+"):
            existing_rows = read_csv_as_dict_list(RESPONSE_CSV)
            existing_rows = upsert_response_row(existing_rows, response_row)
            write_dict_list_to_csv(RESPONSE_CSV, existing_rows, RESPONSE_FIELDS)
        if submit_action == "save_progress":
            upsert_response_save_audit(response_row)

        update_excel_files()
        if submit_action == "save_progress":
            saved_time = datetime.now().strftime("%I:%M:%S %p")
            flash(f"Progress saved successfully at {saved_time}.", "success")
            return redirect(url_for("form"))
        return redirect(url_for("dashboard"))

    responses = normalize_response_storage(write_back=True)
    saved_answers = {}
    for row in responses:
        row_profile_id = (row.get("profile_id", "") or "").strip().upper()
        if row_profile_id == profile_id.strip().upper():
            saved_answers = dict(row)

    for key in ["response_id", "profile_id", "submitted_at", "submit_action"]:
        saved_answers.pop(key, None)

    now = datetime.now()
    return render_template(
        "form.html",
        profile=profile,
        investigator_username=investigator_username,
        profile_id=profile_id,
        today_date=now.strftime("%Y-%m-%d"),
        current_datetime=now.strftime("%Y-%m-%d %H:%M:%S"),
        saved_answers=saved_answers,
    )


def _section_status(response, keys):
    if not response or not keys:
        return "red"
    filled = 0
    for key in keys:
        value = str(response.get(key, "")).strip()
        if value:
            filled += 1
    if filled == 0:
        return "red"
    if filled == len(keys):
        return "green"
    return "orange"


@app.route("/resume-profile/<profile_id>")
def resume_profile(profile_id):
    pid = resolve_profile_id_alias(profile_id)
    if not pid:
        return redirect(url_for("section_status"))

    selected_profile = find_profile_by_id(pid)

    if not selected_profile:
        return redirect(url_for("section_status"))

    session["profile_id"] = (selected_profile.get("profile_id", "") or pid).strip().upper()
    session["scanned_profile"] = selected_profile

    if not investigator_required():
        return redirect(url_for("investigator_login", next=url_for("form")))
    return redirect(url_for("form"))


@app.route("/section-status")
def section_status():
    current_profile_id = (session.get("profile_id", "") or "").strip().upper()
    if not current_profile_id:
        return redirect(url_for("login"))

    section_keys = {
        "A": ["child_id_code", "dob", "age_completed", "sex", "birth_order", "siblings_count"],
        "B": ["family_type", "family_members_total", "religion", "social_category", "edu_head_family", "occupation_head_family"],
        "C": ["monthly_income"],
        "D": ["kuppuswamy_total_score", "ses_class"],
        "E": ["low_birth_weight", "chronic_illness", "worm_infestation", "deworming_tablet", "iron_supplementation"],
        "F": ["diet_type", "freq_green_leafy", "freq_jaggery", "freq_dates", "freq_eggs", "freq_meat", "freq_fruits"],
        "G": ["hb_previously_tested"],
        "H": ["device_seq_1", "device_seq_2", "device_seq_3", "masimo_reading", "poc_hb_value", "lab_hb_value"],
        "I": ["parent_q_39", "parent_q_40", "parent_q_41", "parent_q_42", "parent_q_43", "parent_q_44", "parent_q_45", "parent_q_46"],
        "J": ["child_classification", "ifa_dose", "referral_advised", "investigator_name", "referral_date"],
    }

    profiles = normalize_profile_storage(write_back=True)
    responses = normalize_response_storage()

    latest_response_by_profile = {}
    for row in responses:
        pid = (row.get("profile_id", "") or "").strip().upper()
        if pid:
            latest_response_by_profile[pid] = row

    rows = []
    selected_profiles = [
        p for p in profiles
        if (p.get("profile_id", "") or "").strip().upper() == current_profile_id
    ]

    for idx, p in enumerate(selected_profiles, start=1):
        pid = (p.get("profile_id", "") or "").strip().upper()
        response = latest_response_by_profile.get(pid, {})
        statuses = {letter: _section_status(response, keys) for letter, keys in section_keys.items()}
        rows.append({
            "sno": idx,
            "profile_id": pid,
            "statuses": statuses,
        })

    return render_template(
        "section_status.html",
        rows=rows,
        current_profile_id=current_profile_id,
        section_letters=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
        section_labels={
            "A": {
                "title": "Participant Identification",
                "subs": []
            },
            "B": {
                "title": "Socio Demographic & Socio Economic Details",
                "subs": [
                    "B1. Family Characteristics",
                    "B2. Modified Kuppuswamy Socio-Economic Scale (Urban)",
                    "B3. Additional Socio-Economic Details"
                ]
            },
            "C": {"title": "Monthly Family Income (₹)", "subs": []},
            "D": {"title": "Final Kuppuswamy Score & SES Class (To be Investigator Filled)", "subs": []},
            "E": {"title": "Child Health & Nutrition History", "subs": []},
            "F": {"title": "Dietary Practices", "subs": []},
            "G": {"title": "History of Hemoglobin Testing", "subs": []},
            "H": {"title": "Haemoglobin Measurement Results", "subs": []},
            "I": {"title": "Parental Acceptability", "subs": []},
            "J": {"title": "Referral & Action Taken", "subs": []},
        },
    )


@app.route("/logout")
def logout():
    session.pop("profile_id", None)
    session.pop("scanned_profile", None)
    return redirect(url_for("login"))


# --------------------------------------------------
# ADMIN LOGIN
# --------------------------------------------------
@app.route("/admin-login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        username_norm = username.casefold()
        password = request.form.get("password", "").strip()

        valid = any(
            username_norm == (admin_user or "").strip().casefold() and check_password_hash(admin_pass_hash, password)
            for admin_user, admin_pass_hash in ADMIN_CREDENTIALS
            if admin_user and admin_pass_hash
        )
        if valid:
            session["admin_logged_in"] = True
            session["admin_username"] = username
            return redirect(url_for("admin_dashboard"))

        return "Invalid Admin Credentials"

    return render_template("admin_login.html")


@app.route("/admin-dashboard")
def admin_dashboard():
    if not admin_required():
        return redirect(url_for("admin_login"))
    profiles = sort_profile_rows_by_created_at(normalize_profile_storage(write_back=True), newest_first=True)
    responses = sort_response_rows_by_submitted_at(normalize_response_storage(write_back=True), newest_first=True)
    linked, _ = build_linked_view_data()

    profile_headers = list(profiles[0].keys()) if profiles else PROFILE_FIELDS
    response_headers = RESPONSE_FIELDS if responses else RESPONSE_FIELDS

    return render_template(
        "admin_dashboard.html",
        profiles=profiles,
        responses=responses,
        total_profiles=len(profiles),
        total_responses=len(responses),
        total_linked=len(linked),
        profile_headers=profile_headers,
        response_headers=response_headers,
    )


@app.route("/admin-logout")
def admin_logout():
    session.pop("admin_logged_in", None)
    session.pop("admin_username", None)
    return redirect(url_for("dashboard"))


@app.route("/investigator-logout")
def investigator_logout():
    if investigator_required():
        append_investigator_audit("logout", "Investigator logged out")
    session.pop("investigator_logged_in", None)
    session.pop("investigator_username", None)
    return redirect(url_for("dashboard"))


# --------------------------------------------------
# ADMIN: VIEW PROFILES (SEARCH)
# --------------------------------------------------
@app.route("/admin/profiles")
def admin_profiles():
    if not admin_required():
        return redirect(url_for("admin_login"))

    q = request.args.get("q", "").strip().upper()
    data = sort_profile_rows_by_created_at(normalize_profile_storage(write_back=True), newest_first=True)

    if q:
        data = [r for r in data if r.get("profile_id", "").strip().upper() == q]

    for row in data:
        row["barcode_path"] = ensure_barcode_image(row.get("profile_id", ""))

    return render_template("admin_profiles.html", data=data, q=q)


# --------------------------------------------------
# ADMIN: VIEW RESPONSES (SEARCH)
# --------------------------------------------------
@app.route("/admin/responses")
def admin_responses():
    if not admin_required():
        return redirect(url_for("admin_login"))

    data = sort_response_rows_by_submitted_at(normalize_response_storage(write_back=True), newest_first=True)
    if not data:
        return render_template("admin_responses.html", data=[], q="", headers=RESPONSE_FIELDS)

    response_headers = RESPONSE_FIELDS

    q = request.args.get("q", "").strip().upper()

    if q:
        data = [r for r in data if r.get("profile_id", "").strip().upper() == q]

    return render_template("admin_responses.html", data=data, q=q, headers=response_headers)


@app.route("/admin/investigator-audit")
def admin_investigator_audit():
    if not admin_required():
        return redirect(url_for("admin_login"))

    data = read_csv_as_dict_list(AUDIT_LOG_CSV)
    headers = ["timestamp", "actor_type", "actor", "event", "details"]

    # Ensure stable columns even if file is missing/empty
    normalized = []
    for row in data:
        normalized.append({h: row.get(h, "") for h in headers})

    return render_template(
        "admin_investigator_audit.html",
        data=normalized,
        headers=headers,
    )


@app.route("/admin/response-save-audit")
def admin_response_save_audit():
    if not admin_required():
        return redirect(url_for("admin_login"))

    headers = RESPONSE_SAVE_AUDIT_FIELDS
    data = sort_rows_by_timestamp(
        read_csv_as_dict_list(RESPONSE_SAVE_AUDIT_CSV),
        timestamp_key="saved_at",
        newest_first=True,
    )
    normalized = [{h: row.get(h, "") for h in headers} for row in data]

    return render_template(
        "admin_response_save_audit.html",
        data=normalized,
        headers=headers,
    )


# --------------------------------------------------
# ADMIN: DELETE PROFILE
# --------------------------------------------------
@app.route("/admin/delete-profile/<profile_id>", methods=["POST"])
def admin_delete_profile(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()
    if not delete_profile_related_data(profile_id):
        return "Profile not found"
    return redirect(url_for("admin_profiles"))


@app.route("/admin/delete-linked-profile/<profile_id>", methods=["POST"])
def admin_delete_linked_profile(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()
    if not delete_profile_related_data(profile_id):
        return "Profile not found"

    return redirect(url_for("admin_link_excel"))


# --------------------------------------------------
# ADMIN: DELETE RESPONSE (BY response_id)
# --------------------------------------------------
@app.route("/admin/delete-response/<response_id>", methods=["POST"])
def admin_delete_response(response_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    responses = normalize_response_storage()
    if not responses:
        return "No responses found"

    responses_new = [r for r in responses if r.get("response_id", "") != response_id]

    write_dict_list_to_csv(RESPONSE_CSV, responses_new, RESPONSE_FIELDS)
    update_excel_files()
    return redirect(url_for("admin_responses"))


# --------------------------------------------------
# ADMIN: EDIT PROFILE
# --------------------------------------------------
@app.route("/admin/edit-profile/<profile_id>", methods=["GET", "POST"])
def admin_edit_profile(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()
    profiles = normalize_profile_storage(write_back=True)

    profile_row = None
    for p in profiles:
        if p.get("profile_id", "").strip().upper() == profile_id:
            profile_row = p
            break

    if not profile_row:
        return "Profile not found"

    if request.method == "POST":
        profile_row["name"] = request.form.get("name", "").strip()
        profile_row["dob"] = request.form.get("dob", "").strip()
        profile_row["age"] = request.form.get("age", "").strip()
        profile_row["age_full"] = request.form.get("age_full", "").strip()
        profile_row["gender"] = request.form.get("gender", "").strip()
        profile_row["school"] = request.form.get("school", "").strip()
        profile_row["location"] = request.form.get("location", "").strip()
        profile_row["class"] = request.form.get("class", "").strip()
        profile_row["section"] = request.form.get("section", "").strip()

        validation_error = validate_profile_row(profile_row)
        if validation_error:
            return validation_error

        write_dict_list_to_csv(PROFILE_CSV, profiles, PROFILE_FIELDS)
        update_excel_files()
        return redirect(url_for("admin_profiles"))

    return render_template("admin_edit_profile.html", p=profile_row)


# --------------------------------------------------
# ADMIN: EDIT RESPONSE
# --------------------------------------------------
@app.route("/admin/edit-response/<response_id>", methods=["GET", "POST"])
def admin_edit_response(response_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    responses = normalize_response_storage(write_back=True)
    if not responses:
        return "No responses found"

    response_row = None
    for r in responses:
        if r.get("response_id", "") == response_id:
            response_row = r
            break

    if not response_row:
        return "Response not found"

    if request.method == "POST":
        for key in response_row.keys():
            if key == "response_id":
                continue
            response_row[key] = request.form.get(key, response_row.get(key, "")).strip()
        response_row = sync_response_identifiers(response_row)

        write_dict_list_to_csv(RESPONSE_CSV, responses, RESPONSE_FIELDS)

        update_excel_files()
        return redirect(url_for("admin_responses"))

    return render_template("admin_edit_response.html", r=response_row)


# --------------------------------------------------
# ADMIN: EXPORT FILTERED EXCEL
# --------------------------------------------------
@app.route("/admin/export/<profile_id>")
def admin_export_filtered(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()
    os.makedirs(EXPORT_FOLDER, exist_ok=True)

    profiles = normalize_profile_storage(write_back=True)
    responses = normalize_response_storage()

    profiles_f = [p for p in profiles if p.get("profile_id", "").strip().upper() == profile_id]
    responses_f = sort_response_rows_by_submitted_at(
        [r for r in responses if r.get("profile_id", "").strip().upper() == profile_id],
        newest_first=True,
    )

    export_path = os.path.join(EXPORT_FOLDER, f"filtered_{profile_id}.xlsx")

    with pd.ExcelWriter(export_path, engine="openpyxl") as writer:
        pd.DataFrame(profiles_f).to_excel(writer, sheet_name="Profile", index=False)
        pd.DataFrame(responses_f, columns=RESPONSE_FIELDS).to_excel(writer, sheet_name="Responses", index=False)

    return send_file(export_path, as_attachment=True)


# --------------------------------------------------
# ADMIN: DOWNLOAD FILES
# --------------------------------------------------
@app.route("/admin/download/<filename>")
def admin_download(filename):
    if not admin_required():
        return redirect(url_for("admin_login"))

    allowed_files = {
        "profiles.csv": PROFILE_CSV,
        "responses.csv": RESPONSE_CSV,
        "profiles.xlsx": PROFILE_XLSX,
        "responses.xlsx": RESPONSE_XLSX,
        "linked_data.csv": LINKED_CSV,
        "linked_data.xlsx": LINKED_XLSX,
        "investigator_audit_log.csv": AUDIT_LOG_CSV,
        "response_save_audit.csv": RESPONSE_SAVE_AUDIT_CSV,
        "response_save_audit.xlsx": os.path.join(BASE_DIR, "response_save_audit.xlsx"),
    }

    if filename not in allowed_files:
        return "File not allowed"

    path = allowed_files[filename]
    if not os.path.exists(path):
        return "File not found"

    if filename in ["profiles.csv", "profiles.xlsx", "responses.csv", "responses.xlsx", "response_save_audit.csv", "response_save_audit.xlsx"]:
        normalize_profile_storage(write_back=True)
        normalize_response_storage(write_back=True)
        update_excel_files()

    return send_file(path, as_attachment=True)

@app.route("/admin/download-history")
def download_history():
    if not admin_required():
        return redirect(url_for("admin_login"))

    if not os.path.exists(RESPONSE_HISTORY_CSV):
        return "No history data found"

    return send_file(RESPONSE_HISTORY_CSV, as_attachment=True)


# --------------------------------------------------
# ADMIN: UPLOAD / REPLACE FILES
# --------------------------------------------------
@app.route("/admin-upload", methods=["GET", "POST"])
def admin_upload():
    if not admin_required():
        return redirect(url_for("admin_login"))

    if request.method == "POST":
        file = request.files.get("file")

        if not file or file.filename == "":
            return "No file selected"

        filename = file.filename.strip().lower()

        allowed = [
            "profiles.csv",
            "responses.csv",
            "profiles.xlsx",
            "responses.xlsx",
            "linked_data.csv",
            "linked_data.xlsx",
        ]
        if filename not in allowed:
            return "Only profiles.csv, responses.csv, profiles.xlsx, responses.xlsx, linked_data.csv, linked_data.xlsx allowed"

        save_path = os.path.join(BASE_DIR, filename)
        file.save(save_path)

        if filename in ["responses.csv", "responses.xlsx"]:
            uploaded_rows = read_uploaded_response_rows(save_path)

            # Save to history (ALL)
            if not os.path.exists(RESPONSE_HISTORY_CSV):
                write_dict_list_to_csv(RESPONSE_HISTORY_CSV, [], RESPONSE_FIELDS)

            history_rows = read_csv_as_dict_list(RESPONSE_HISTORY_CSV)
            history_rows.extend(uploaded_rows)
            write_dict_list_to_csv(RESPONSE_HISTORY_CSV, history_rows, RESPONSE_FIELDS)

            # Save latest only
            latest_map = {}
            for row in uploaded_rows:
                pid = (row.get("profile_id", "") or "").strip().upper()
                if pid:
                    clean_row = sanitize_response_row(row)
                    latest_map[pid] = clean_row

            write_dict_list_to_csv(RESPONSE_CSV, list(latest_map.values()), RESPONSE_FIELDS)

        update_excel_files()
        if filename in ["linked_data.csv", "linked_data.xlsx"]:
            update_linked_excel_file()
        return redirect(url_for("admin_dashboard"))

    return render_template("admin_upload.html")


@app.route("/admin/upload", methods=["GET", "POST"])
def admin_upload_alias():
    return admin_upload()


@app.route("/admin/link-excel")
def admin_link_excel():
    if not admin_required():
        return redirect(url_for("admin_login"))

    linked_data, headers = build_linked_view_data()

    # Refresh export files so "Export Data" matches what is shown on this page.
    if linked_data:
        write_dict_list_to_csv(LINKED_CSV, linked_data, headers)
        update_linked_excel_file()

    return render_template(
        "admin_link_excel.html",
        data=linked_data,
        headers=headers,
        error="",
        success="",
    )


@app.route("/horiba", methods=["GET", "POST"])
def horiba():
    redirect_response = machine_redirect_if_unauthorized()
    if redirect_response:
        return redirect_response

    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("Error: No file selected for Horiba upload")
            return redirect(url_for("horiba"))
        try:
            filename = (file.filename or "").lower()
            if filename.endswith(".csv"):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            if df.empty:
                flash("Error: Uploaded file is empty")
                return redirect(url_for("horiba"))

            df = _normalized_df_columns(df)
            profile_col = _find_profile_id_column(df)
            value_col = _find_machine_value_column(df, "horiba", profile_col) if profile_col else None
            horiba_result_cols = {
                source_col: target_col
                for source_col, target_col in HORIBA_UPLOAD_FIELD_MAP.items()
                if source_col in df.columns
            }

            if not profile_col or (not value_col and not horiba_result_cols):
                flash("Error: Required columns missing. Need profile_id/barcode/sampleid and Horiba result columns")
                return redirect(url_for("horiba"))

            linked_rows, _ = build_linked_view_data()
            row_map = {(r.get("profile_id", "") or "").strip().upper(): r for r in linked_rows}
            updates = 0
            changed_ids = []

            for _, raw in df.iterrows():
                pid = str(raw.get(profile_col, "")).strip().upper()
                if not pid or pid == "NAN":
                    continue
                if pid in row_map:
                    changed = False

                    if horiba_result_cols:
                        for source_col, target_col in horiba_result_cols.items():
                            val = raw.get(source_col)
                            if pd.isna(val):
                                continue
                            row_map[pid][target_col] = str(val).strip()
                            changed = True
                        if "hgb" in horiba_result_cols:
                            hgb_val = raw.get("hgb")
                            if not pd.isna(hgb_val):
                                row_map[pid]["horiba"] = str(hgb_val).strip()
                    elif value_col:
                        val = raw.get(value_col)
                        if not pd.isna(val):
                            row_map[pid]["horiba"] = str(val).strip()
                            changed = True

                    if changed:
                        updates += 1
                        changed_ids.append(pid)

            if updates > 0:
                save_linked_rows(list(row_map.values()))
                sample_ids = ", ".join(changed_ids[:15])
                append_investigator_audit(
                    "machine_update",
                    f"Horiba upload updated {updates} profiles from {file.filename}; profile_ids={sample_ids}",
                )
                flash(f"Horiba data updated for {updates} profiles")
            else:
                flash("No matching profiles found for Horiba upload")
        except Exception as e:
            flash(f"Error processing Horiba file: {e}")
        return redirect(url_for("horiba"))

    return render_template("horiba.html")


@app.route("/update-horiba", methods=["POST"])
def update_horiba():
    if not machine_access_required():
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    profile_id = (request.form.get("profile_id", "") or "").strip().upper()
    horiba_value = (request.form.get("horiba", "") or "").strip()

    if not profile_id:
        return jsonify({"success": False, "error": "Profile ID is required"}), 400

    linked_rows = read_csv_as_dict_list(LINKED_CSV)
    if not linked_rows:
        linked_rows, _ = build_linked_view_data()

    updated = False
    old_val = ""
    for row in linked_rows:
        pid = (row.get("profile_id", "") or "").strip().upper()
        if pid == profile_id:
            old_val = str(row.get("horiba", "") or "")
            row["horiba"] = horiba_value
            updated = True
            break

    if not updated:
        return jsonify({"success": False, "error": "Profile not found"}), 404

    save_linked_rows(linked_rows)
    append_investigator_audit(
        "machine_update",
        f"Horiba updated for {profile_id}: '{old_val}' -> '{horiba_value}'",
    )

    return jsonify({"success": True})


# --------------------------------------------------
# RUN APP
# --------------------------------------------------
if __name__ == "__main__":
    os.makedirs(BARCODE_FOLDER, exist_ok=True)
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    app.run(debug=os.getenv("FLASK_DEBUG", "0") == "1")

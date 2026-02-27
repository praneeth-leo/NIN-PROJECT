from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, jsonify, flash
)
import csv
import os
import re
import pandas as pd
from barcode import Code128
from barcode.writer import ImageWriter
from datetime import datetime
import uuid


# --------------------------------------------------
# App setup
# --------------------------------------------------
app = Flask(__name__)
app.secret_key = "survey_secret_key"

# ---------------- INVESTIGATOR SETTINGS ----------------
INVESTIGATOR_CREDENTIALS = [
    (
        os.environ.get("INVESTIGATOR_USERNAME_1", "Jahnavi").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_1", "jah123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_2", "Yeshwanth").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_2", "yes123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_3", "Shailaja").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_3", "sha123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_4", "Samhita").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_4", "sam123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_5", "Kriti").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_5", "kri123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_6", "Nandini").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_6", "nan123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_7", "Ameeta").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_7", "ame123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_8", "Varshini").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_8", "var123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_9", "Sri Teja").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_9", "sri123").strip(),
    ),
    (
        os.environ.get("INVESTIGATOR_USERNAME_10", "valcap").strip(),
        os.environ.get("INVESTIGATOR_PASSWORD_10", "valcap123").strip(),
    ),
]

# ---------------- ADMIN SETTINGS ----------------
ADMIN_CREDENTIALS = [
    (
        os.environ.get("ADMIN_USERNAME_1", "admin").strip(),
        os.environ.get("ADMIN_PASSWORD_1", "admin123").strip(),
    ),
    (
        os.environ.get("ADMIN_USERNAME_2", "admin2").strip(),
        os.environ.get("ADMIN_PASSWORD_2", "admin234").strip(),
    ),
    (
        os.environ.get("ADMIN_USERNAME_3", "admin3").strip(),
        os.environ.get("ADMIN_PASSWORD_3", "admin345").strip(),
    ),
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROFILE_CSV = os.path.join(BASE_DIR, "profiles.csv")
RESPONSE_CSV = os.path.join(BASE_DIR, "responses.csv")
PROFILE_XLSX = os.path.join(BASE_DIR, "profiles.xlsx")
RESPONSE_XLSX = os.path.join(BASE_DIR, "responses.xlsx")
LINKED_CSV = os.path.join(BASE_DIR, "linked_data.csv")
LINKED_XLSX = os.path.join(BASE_DIR, "linked_data.xlsx")
AUDIT_LOG_CSV = os.path.join(BASE_DIR, "investigator_audit_log.csv")

BARCODE_FOLDER = os.path.join(BASE_DIR, "static", "barcodes")
EXPORT_FOLDER = os.path.join(BASE_DIR, "exports")

# ✅ UPDATED: dob + age_full added
PROFILE_FIELDS = [
    "profile_id",
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
            pd.read_csv(PROFILE_CSV).to_excel(PROFILE_XLSX, index=False)
        if os.path.exists(RESPONSE_CSV):
            pd.read_csv(RESPONSE_CSV).to_excel(RESPONSE_XLSX, index=False)
    except Exception as e:
        print("Excel error:", e)


def update_linked_excel_file():
    try:
        if os.path.exists(LINKED_CSV):
            pd.read_csv(LINKED_CSV).to_excel(LINKED_XLSX, index=False)
    except Exception as e:
        print("Linked excel error:", e)


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
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


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
    profiles = read_csv_as_dict_list(PROFILE_CSV)
    linked_rows = read_csv_as_dict_list(LINKED_CSV)
    responses = read_csv_as_dict_list(RESPONSE_CSV)

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

    merged = []
    for base in base_rows:
        row = dict(base)
        pid = (row.get("profile_id", "") or "").strip().upper()
        if not pid:
            continue
        row["profile_id"] = pid

        p = profile_map.get(pid, {})
        row.setdefault("profile_found", "yes" if p else "no")
        row.setdefault("name", p.get("name", ""))
        row.setdefault("school", p.get("school", ""))
        row.setdefault("class", p.get("class", ""))
        row.setdefault("section", p.get("section", ""))

        # Bring latest response fields into linked view
        resp = response_map.get(pid, {})
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
    preferred_order = [
        "profile_id", "profile_found", "name", "school", "class", "section",
        "horiba",
        "submitted_at", "response_id",
    ]
    all_keys = set()
    for r in merged:
        all_keys.update(r.keys())
    extra_keys = [k for k in sorted(all_keys) if k not in preferred_order]
    headers = [k for k in preferred_order if k in all_keys] + extra_keys

    normalized = []
    for r in merged:
        normalized.append({h: r.get(h, "") for h in headers})

    return normalized, headers


def save_linked_rows(rows):
    fields = build_ordered_fieldnames(
        rows,
        preferred=[
            "profile_id", "profile_found", "name", "school", "class", "section",
            "horiba", "submitted_at", "response_id",
        ],
    )
    normalized_rows = [{k: row.get(k, "") for k in fields} for row in rows]
    write_dict_list_to_csv(LINKED_CSV, normalized_rows, fields)
    update_linked_excel_file()


def _normalized_df_columns(df):
    normalized = []
    for col in df.columns:
        key = str(col).strip().lower()
        key = re.sub(r"\s+", "_", key)
        key = re.sub(r"[^a-z0-9_]", "", key)
        normalized.append(key)
    df.columns = normalized
    return df


def _find_profile_id_column(df):
    for candidate in ["profile_id", "barcode", "barcode_id", "participant_id", "id"]:
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

    with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (
                row.get("name", "").strip().lower() == profile["name"].strip().lower()
                and row.get("surname", "").strip().lower() == profile["surname"].strip().lower()
                and row.get("school", "").strip().lower() == profile["school"].strip().lower()
                and row.get("location", "").strip().lower() == profile["location"].strip().lower()
                and row.get("class", "").strip().lower() == profile["class"].strip().lower()
                and row.get("section", "").strip().lower() == profile["section"].strip().lower()
            ):
                return True
    return False


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


def generate_barcode(profile_id):
    barcode = Code128(profile_id, writer=ImageWriter())
    path = os.path.join(BARCODE_FOLDER, profile_id)
    barcode.save(path, options={
        "module_width": 0.18,
        "module_height": 7,
        "quiet_zone": 1.0,
        "font_size": 0,
        "text_distance": 1,
        "write_text": False,
        "dpi": 300,
    })
    return f"barcodes/{profile_id}.png"


# --------------------------------------------------
# USER SECTION
# --------------------------------------------------
@app.route("/")
def home():
    return redirect(url_for("login"))


@app.route("/investigator-login", methods=["GET", "POST"])
def investigator_login():
    next_url = request.args.get("next", "").strip()
    if next_url and not next_url.startswith("/"):
        next_url = ""
    default_next = url_for("form") if session.get("profile_id") else url_for("dashboard")
    force_reauth_for_form = (next_url == url_for("form"))

    if force_reauth_for_form and request.method == "GET":
        session.pop("investigator_logged_in", None)
        session.pop("investigator_username", None)

    if investigator_required() and not force_reauth_for_form:
        return redirect(next_url or default_next)

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        valid = any(
            username == inv_user and password == inv_pass
            for inv_user, inv_pass in INVESTIGATOR_CREDENTIALS
            if inv_user and inv_pass
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
        entered_id = re.sub(r"[^A-Za-z0-9]", "", (raw_id or "")).upper()

        if entered_id == "":
            return render_template("login.html", error="Please enter Barcode ID")

        if not os.path.exists(PROFILE_CSV):
            return render_template("login.html", error="No profiles found. Please create a profile first.")

        found = False
        matched_profile = None
        with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                csv_id = str(row.get("profile_id", "")).strip().upper()
                if csv_id == entered_id:
                    found = True
                    matched_profile = row
                    break

        if not found:
            return render_template("login.html", error=f"Invalid Barcode ID: {entered_id}")

        session["profile_id"] = entered_id
        session["scanned_profile"] = matched_profile or {}
        return redirect(url_for("profile_details"))

    return render_template("login.html")


@app.route("/profile-details")
def profile_details():
    profile_id = session.get("profile_id", "").strip().upper()
    if not profile_id:
        return redirect(url_for("login"))

    profile = session.get("scanned_profile") or {}
    if not profile:
        profiles = read_csv_as_dict_list(PROFILE_CSV)
        for row in profiles:
            if (row.get("profile_id", "") or "").strip().upper() == profile_id:
                profile = row
                break

    if not profile:
        return render_template("login.html", error="Profile not found for scanned barcode.")

    barcode_file = f"{profile_id}.png"
    barcode_fs_path = os.path.join(BARCODE_FOLDER, barcode_file)
    barcode_path = f"barcodes/{barcode_file}" if os.path.exists(barcode_fs_path) else ""

    return render_template(
        "profile_details.html",
        profile=profile,
        profile_id=profile_id,
        barcode_path=barcode_path,
    )


@app.route("/profile-details/<profile_id>")
def profile_details_by_id(profile_id):
    pid = re.sub(r"[^A-Za-z0-9]", "", (profile_id or "")).upper()
    if not pid:
        return render_template("login.html", error="Invalid Barcode ID.")

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    profile = None
    for row in profiles:
        if (row.get("profile_id", "") or "").strip().upper() == pid:
            profile = row
            break

    if not profile:
        return render_template("login.html", error=f"Profile not found for Barcode ID: {pid}")

    barcode_file = f"{pid}.png"
    barcode_fs_path = os.path.join(BARCODE_FOLDER, barcode_file)
    barcode_path = f"barcodes/{barcode_file}" if os.path.exists(barcode_fs_path) else ""

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
    if request.method == "POST":
        form = request.form.to_dict()

        profile_row = {
            "profile_id": "",
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

        age_years = _calculate_age_years(profile_row["dob"])
        if age_years is None:
            return "Invalid DOB. Please enter a valid date of birth."
        if age_years not in [3, 4, 5]:
            return "Only ages 3, 4, and 5 are allowed."

        if profile_exists(profile_row):
            return "Profile already exists. Please use the existing participant barcode/profile."

        profile_id = generate_profile_id(
            profile_row["name"],
            profile_row["surname"],
            profile_row["dob"],
            profile_row["gender"],
            profile_row["school"],
            profile_row["location"]
        )
        profile_row["profile_id"] = profile_id

        existing_rows = read_csv_as_dict_list(PROFILE_CSV)
        existing_rows.append({k: profile_row.get(k, "") for k in PROFILE_FIELDS})
        write_dict_list_to_csv(PROFILE_CSV, existing_rows, PROFILE_FIELDS)

        update_excel_files()
        barcode_path = generate_barcode(profile_id)
        session["profile_id"] = profile_id

        return render_template(
            "profile_view.html",
            profile_id=profile_id,
            barcode_path=barcode_path,
            profile_name=f"{profile_row.get('name', '').strip()} {profile_row.get('surname', '').strip()}".strip()
        )

    return render_template("profile.html")


@app.route("/form", methods=["GET", "POST"])
def form():
    profile_id = session.get("profile_id")
    investigator_username = (session.get("investigator_username") or "").strip()
    if not profile_id:
        return redirect(url_for("login"))
    if not investigator_required():
        return redirect(url_for("investigator_login", next=url_for("form")))

    if request.method == "POST":
        answers = request.form.to_dict()
        if not (answers.get("investigator_username", "") or "").strip():
            answers["investigator_username"] = investigator_username
        if not (answers.get("investigator_name", "") or "").strip():
            answers["investigator_name"] = investigator_username

        # response unique id
        answers["response_id"] = str(uuid.uuid4())
        answers["profile_id"] = profile_id
        answers["submitted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        existing_rows = read_csv_as_dict_list(RESPONSE_CSV)
        existing_rows.append(answers)
        response_fields = build_ordered_fieldnames(
            existing_rows,
            preferred=["response_id", "profile_id", "submitted_at"]
        )
        normalized_rows = [{k: row.get(k, "") for k in response_fields} for row in existing_rows]
        write_dict_list_to_csv(RESPONSE_CSV, normalized_rows, response_fields)

        update_excel_files()
        return redirect(url_for("dashboard"))

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    profile = None
    for p in profiles:
        if (p.get("profile_id", "") or "").strip().upper() == profile_id.strip().upper():
            profile = p
            break

    if not profile:
        return redirect(url_for("login"))

    return render_template("form.html", profile=profile, investigator_username=investigator_username)


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
    pid = re.sub(r"[^A-Za-z0-9]", "", (profile_id or "")).upper()
    if not pid:
        return redirect(url_for("section_status"))

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    selected_profile = None
    for row in profiles:
        if (row.get("profile_id", "") or "").strip().upper() == pid:
            selected_profile = row
            break

    if not selected_profile:
        return redirect(url_for("section_status"))

    session["profile_id"] = pid
    session["scanned_profile"] = selected_profile

    if not investigator_required():
        return redirect(url_for("investigator_login", next=url_for("form")))
    return redirect(url_for("form"))


@app.route("/section-status")
def section_status():
    if not session.get("profile_id"):
        return redirect(url_for("login"))

    section_keys = {
        "A": ["child_id_code", "profile_id_code", "dob", "age_completed", "sex", "birth_order", "siblings_count"],
        "B": ["family_type", "family_members_total", "religion", "caste_category", "edu_head_family", "occupation_head_family", "monthly_income", "kuppuswamy_total_score", "ses_class"],
        "C": [],
        "D": [],
        "E": ["low_birth_weight", "chronic_illness", "chronic_illness_specify", "worm_infestation", "deworming_tablet", "iron_supplementation"],
        "F": ["diet_type", "iron_rich_food_frequency"],
        "G": ["hb_measurement_datetime"],
        "H": ["device_sequence", "masimo_reading_1", "masimo_reading_2", "masimo_reading_3", "masimo_avg_sphb", "capillary_hb_value", "capillary_attempts", "venous_hb_value", "child_response_score"],
        "I": ["comfortable_testing_process", "preferred_method", "repeat_screening", "parent_total"],
        "J": ["child_classification", "ifa_dose", "referral_advised", "referral_destination", "referral_other_specify", "investigator_signature", "referral_date"],
    }

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    responses = read_csv_as_dict_list(RESPONSE_CSV)

    latest_response_by_profile = {}
    for row in responses:
        pid = (row.get("profile_id", "") or "").strip().upper()
        if pid:
            latest_response_by_profile[pid] = row

    rows = []
    for idx, p in enumerate(profiles, start=1):
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
        current_profile_id=session.get("profile_id", "").strip().upper(),
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
    return redirect(url_for("login"))


# --------------------------------------------------
# ADMIN LOGIN
# --------------------------------------------------
@app.route("/admin-login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        valid = any(
            username == admin_user and password == admin_pass
            for admin_user, admin_pass in ADMIN_CREDENTIALS
            if admin_user and admin_pass
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
    profiles = read_csv_as_dict_list(PROFILE_CSV)
    responses = read_csv_as_dict_list(RESPONSE_CSV)
    linked = read_csv_as_dict_list(LINKED_CSV)

    profile_headers = list(profiles[0].keys()) if profiles else PROFILE_FIELDS
    response_headers = list(responses[0].keys()) if responses else []

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
    return redirect(url_for("admin_login"))


@app.route("/investigator-logout")
def investigator_logout():
    if investigator_required():
        append_investigator_audit("logout", "Investigator logged out")
    session.pop("investigator_logged_in", None)
    session.pop("investigator_username", None)
    return redirect(url_for("investigator_login"))


# --------------------------------------------------
# ADMIN: VIEW PROFILES (SEARCH)
# --------------------------------------------------
@app.route("/admin/profiles")
def admin_profiles():
    if not admin_required():
        return redirect(url_for("admin_login"))

    q = request.args.get("q", "").strip().upper()
    data = read_csv_as_dict_list(PROFILE_CSV)

    if q:
        data = [r for r in data if r.get("profile_id", "").strip().upper() == q]

    return render_template("admin_profiles.html", data=data, q=q)


# --------------------------------------------------
# ADMIN: VIEW RESPONSES (SEARCH)
# --------------------------------------------------
@app.route("/admin/responses")
def admin_responses():
    if not admin_required():
        return redirect(url_for("admin_login"))

    data = read_csv_as_dict_list(RESPONSE_CSV)
    if not data:
        return render_template("admin_responses.html", data=[], q="", headers=[])

    # Backfill missing response IDs so rows are editable/deletable and stable.
    updated = False
    for row in data:
        if not (row.get("response_id", "") or "").strip():
            row["response_id"] = str(uuid.uuid4())
            updated = True

    response_headers = build_ordered_fieldnames(
        data,
        preferred=["response_id", "profile_id", "submitted_at"]
    )
    if updated:
        normalized_rows = [{k: row.get(k, "") for k in response_headers} for row in data]
        write_dict_list_to_csv(RESPONSE_CSV, normalized_rows, response_headers)
        data = normalized_rows

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


# --------------------------------------------------
# ADMIN: DELETE PROFILE
# --------------------------------------------------
@app.route("/admin/delete-profile/<profile_id>", methods=["POST"])
def admin_delete_profile(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    profiles_new = [p for p in profiles if p.get("profile_id", "").strip().upper() != profile_id]

    if profiles and len(profiles_new) == len(profiles):
        return "Profile not found"

    write_dict_list_to_csv(PROFILE_CSV, profiles_new, PROFILE_FIELDS)

    # delete responses of that profile
    responses = read_csv_as_dict_list(RESPONSE_CSV)
    if responses:
        response_fields = list(responses[0].keys())
        responses_new = [r for r in responses if r.get("profile_id", "").strip().upper() != profile_id]
        write_dict_list_to_csv(RESPONSE_CSV, responses_new, response_fields)

    # delete barcode image
    barcode_path = os.path.join(BARCODE_FOLDER, f"{profile_id}.png")
    if os.path.exists(barcode_path):
        os.remove(barcode_path)

    update_excel_files()
    return redirect(url_for("admin_profiles"))


# --------------------------------------------------
# ADMIN: DELETE RESPONSE (BY response_id)
# --------------------------------------------------
@app.route("/admin/delete-response/<response_id>", methods=["POST"])
def admin_delete_response(response_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    responses = read_csv_as_dict_list(RESPONSE_CSV)
    if not responses:
        return "No responses found"

    response_fields = list(responses[0].keys())
    responses_new = [r for r in responses if r.get("response_id", "") != response_id]

    write_dict_list_to_csv(RESPONSE_CSV, responses_new, response_fields)
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
    profiles = read_csv_as_dict_list(PROFILE_CSV)

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
        profile_row["class"] = request.form.get("class", "").strip()
        profile_row["section"] = request.form.get("section", "").strip()

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

    responses = read_csv_as_dict_list(RESPONSE_CSV)
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

        response_fields = list(responses[0].keys())
        write_dict_list_to_csv(RESPONSE_CSV, responses, response_fields)

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

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    responses = read_csv_as_dict_list(RESPONSE_CSV)

    profiles_f = [p for p in profiles if p.get("profile_id", "").strip().upper() == profile_id]
    responses_f = [r for r in responses if r.get("profile_id", "").strip().upper() == profile_id]

    export_path = os.path.join(EXPORT_FOLDER, f"filtered_{profile_id}.xlsx")

    with pd.ExcelWriter(export_path, engine="openpyxl") as writer:
        pd.DataFrame(profiles_f).to_excel(writer, sheet_name="Profile", index=False)
        pd.DataFrame(responses_f).to_excel(writer, sheet_name="Responses", index=False)

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
    }

    if filename not in allowed_files:
        return "File not allowed"

    path = allowed_files[filename]
    if not os.path.exists(path):
        return "File not found"

    return send_file(path, as_attachment=True)


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

        allowed = ["profiles.csv", "responses.csv", "profiles.xlsx", "responses.xlsx"]
        if filename not in allowed:
            return "Only profiles.csv, responses.csv, profiles.xlsx, responses.xlsx allowed"

        save_path = os.path.join(BASE_DIR, filename)
        file.save(save_path)

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

            if not profile_col or not value_col:
                flash("Error: Required columns missing. Need profile_id/barcode and horiba value column")
                return redirect(url_for("horiba"))

            linked_rows, _ = build_linked_view_data()
            row_map = {(r.get("profile_id", "") or "").strip().upper(): r for r in linked_rows}
            updates = 0
            changed_ids = []

            for _, raw in df.iterrows():
                pid = str(raw.get(profile_col, "")).strip().upper()
                if not pid or pid == "NAN":
                    continue
                val = raw.get(value_col)
                if pd.isna(val):
                    continue
                if pid in row_map:
                    row_map[pid]["horiba"] = str(val).strip()
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
    app.run(debug=True)

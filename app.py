from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file
)
import csv
import os
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
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)
if os.getenv("FLASK_ENV") == "production":
    app.config["SESSION_COOKIE_SECURE"] = True

# ---------------- ADMIN SETTINGS ----------------
ADMIN_CREDENTIALS = [
    {
        "username": os.getenv("ADMIN1_USERNAME", os.getenv("ADMIN_USERNAME", "admin")),
        "password": os.getenv("ADMIN1_PASSWORD", os.getenv("ADMIN_PASSWORD", "admin123")),
    },
    {
        "username": os.getenv("ADMIN2_USERNAME", "admin2"),
        "password": os.getenv("ADMIN2_PASSWORD", "admin234"),
    },
    {
        "username": os.getenv("ADMIN3_USERNAME", "admin3"),
        "password": os.getenv("ADMIN3_PASSWORD", "admin345"),
    },
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROFILE_CSV = os.path.join(BASE_DIR, "profiles.csv")
RESPONSE_CSV = os.path.join(BASE_DIR, "responses.csv")
PROFILE_XLSX = os.path.join(BASE_DIR, "profiles.xlsx")
RESPONSE_XLSX = os.path.join(BASE_DIR, "responses.xlsx")
LINKED_CSV = os.path.join(BASE_DIR, "linked_data.csv")
LINKED_XLSX = os.path.join(BASE_DIR, "linked_data.xlsx")

BARCODE_FOLDER = os.path.join(BASE_DIR, "static", "barcodes")
EXPORT_FOLDER = os.path.join(BASE_DIR, "exports")

# ✅ UPDATED: dob + age_full added
PROFILE_FIELDS = [
    "profile_id",
    "name",
    "dob",
    "age",
    "age_full",
    "gender",
    "school",
    "class",
    "section"
]


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def admin_required():
    return session.get("admin_logged_in") is True


def is_valid_admin(username, password):
    for admin in ADMIN_CREDENTIALS:
        if username == admin["username"] and password == admin["password"]:
            return True
    return False


def get_csv_headers(path):
    rows = read_csv_as_dict_list(path)
    return list(rows[0].keys()) if rows else []


def update_excel_files():
    try:
        if os.path.exists(PROFILE_CSV):
            pd.read_csv(PROFILE_CSV).to_excel(PROFILE_XLSX, index=False)
        if os.path.exists(RESPONSE_CSV):
            pd.read_csv(RESPONSE_CSV).to_excel(RESPONSE_XLSX, index=False)
    except Exception as e:
        print("Excel error:", e)


def read_csv_as_dict_list(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_dict_list_to_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def update_linked_excel_file():
    try:
        if os.path.exists(LINKED_CSV):
            pd.read_csv(LINKED_CSV).to_excel(LINKED_XLSX, index=False)
    except Exception as e:
        print("Linked excel error:", e)


def build_linked_rows_from_excel(file_obj):
    df = pd.read_excel(file_obj)
    if df.empty:
        return None, "Uploaded Excel is empty."

    df.columns = [str(c).strip() for c in df.columns]
    profile_id_column = None
    for candidate in ["profile_id", "barcode", "barcode_id", "student_barcode"]:
        if candidate in df.columns:
            profile_id_column = candidate
            break

    if not profile_id_column:
        return None, "Excel must contain one column: profile_id (or barcode / barcode_id / student_barcode)."

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    profile_map = {p.get("profile_id", "").strip().upper(): p for p in profiles}

    rows = []
    for raw in df.to_dict(orient="records"):
        raw = {str(k).strip(): ("" if pd.isna(v) else str(v).strip()) for k, v in raw.items()}
        profile_id = raw.get(profile_id_column, "").strip().upper()
        profile = profile_map.get(profile_id, {})

        row = {
            "profile_id": profile_id,
            "profile_found": "yes" if profile else "no",
            "name": profile.get("name", ""),
            "school": profile.get("school", ""),
            "class": profile.get("class", ""),
            "section": profile.get("section", ""),
            "linked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        for k, v in raw.items():
            if k == profile_id_column:
                continue
            row[k] = v

        rows.append(row)

    return rows, None


def profile_exists(profile):
    if not os.path.exists(PROFILE_CSV):
        return False

    with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (
                row.get("name", "").strip().lower() == profile["name"].strip().lower()
                and row.get("school", "").strip().lower() == profile["school"].strip().lower()
                and row.get("class", "").strip().lower() == profile["class"].strip().lower()
                and row.get("section", "").strip().lower() == profile["section"].strip().lower()
            ):
                return True
    return False


def generate_profile_id(name, school):
    name = name.strip().upper()
    school = school.strip().upper()

    name_code = name[:2] if len(name) >= 2 else name.ljust(2, "X")
    school_code = school[:2] if len(school) >= 2 else school.ljust(2, "X")

    count = 0
    if os.path.exists(PROFILE_CSV):
        with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
            count = len(list(csv.reader(f))) - 1
            if count < 0:
                count = 0

    return f"{name_code}{school_code}{count + 1:04d}"


def generate_barcode(profile_id):
    barcode = Code128(profile_id, writer=ImageWriter())
    path = os.path.join(BARCODE_FOLDER, profile_id)
    barcode.save(path)
    return f"barcodes/{profile_id}.png"


# --------------------------------------------------
# USER SECTION
# --------------------------------------------------
@app.route("/")
def home():
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        entered_id = request.form.get("profile_id", "").strip().upper()

        if entered_id == "":
            return "Please enter Barcode ID"

        if not os.path.exists(PROFILE_CSV):
            return "No profiles found. Please create a profile first."

        found = False
        with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                csv_id = str(row.get("profile_id", "")).strip().upper()
                if csv_id == entered_id:
                    found = True
                    break

        if not found:
            return f"Invalid Barcode ID: {entered_id}"

        session["profile_id"] = entered_id
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/dashboard")
def dashboard():
    if "profile_id" not in session:
        return redirect(url_for("login"))
    return render_template("dashboard.html")


@app.route("/profile", methods=["GET", "POST"])
def profile():
    if request.method == "POST":
        form = request.form.to_dict()

        profile_row = {
            "profile_id": "",
            "name": form.get("name", "").strip(),
            "dob": form.get("dob", "").strip(),
            "age": form.get("age", "").strip(),
            "age_full": form.get("age_full", "").strip(),
            "gender": form.get("gender", "").strip(),
            "school": form.get("school", "").strip(),
            "class": form.get("class", "").strip(),
            "section": form.get("section", "").strip(),
        }

        if profile_exists(profile_row):
            return "Profile already exists. Please login using Barcode ID."

        profile_id = generate_profile_id(profile_row["name"], profile_row["school"])
        profile_row["profile_id"] = profile_id

        file_exists = os.path.exists(PROFILE_CSV)
        with open(PROFILE_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=PROFILE_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: profile_row.get(k, "") for k in PROFILE_FIELDS})

        update_excel_files()
        barcode_path = generate_barcode(profile_id)
        session["profile_id"] = profile_id

        return render_template(
            "profile_view.html",
            profile_id=profile_id,
            barcode_path=barcode_path
        )

    return render_template("profile.html")


@app.route("/form", methods=["GET", "POST"])
def form():
    profile_id = session.get("profile_id")
    if not profile_id:
        return redirect(url_for("login"))

    if request.method == "POST":
        answers = request.form.to_dict()

        # response unique id
        answers["response_id"] = str(uuid.uuid4())
        answers["profile_id"] = profile_id
        answers["submitted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        file_exists = os.path.exists(RESPONSE_CSV)
        with open(RESPONSE_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=answers.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(answers)

        update_excel_files()
        return redirect(url_for("dashboard"))

    return render_template("form.html", profile_id=profile_id)


@app.route("/logout")
def logout():
    session.pop("profile_id", None)
    return redirect(url_for("login"))


# --------------------------------------------------
# ADMIN LOGIN
# --------------------------------------------------
@app.route("/admin-login", methods=["GET", "POST"])
def admin_login():
    if admin_required():
        return redirect(url_for("admin_dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if is_valid_admin(username, password):
            session["admin_logged_in"] = True
            session["admin_username"] = username
            return redirect(url_for("admin_dashboard"))

        return render_template("admin_login.html", error="Invalid admin credentials")

    return render_template("admin_login.html")


@app.route("/admin")
def admin_home():
    if not admin_required():
        return redirect(url_for("admin_login"))
    return redirect(url_for("admin_dashboard"))


@app.route("/admin-dashboard")
def admin_dashboard():
    if not admin_required():
        return redirect(url_for("admin_login"))

    profiles = read_csv_as_dict_list(PROFILE_CSV)
    responses = read_csv_as_dict_list(RESPONSE_CSV)

    response_headers = get_csv_headers(RESPONSE_CSV)
    profile_headers = PROFILE_FIELDS

    return render_template(
        "admin_dashboard.html",
        profiles=profiles,
        responses=responses,
        total_profiles=len(profiles),
        total_responses=len(responses),
        total_linked=len(read_csv_as_dict_list(LINKED_CSV)),
        profile_headers=profile_headers,
        response_headers=response_headers,
    )


@app.route("/admin-logout")
def admin_logout():
    session.pop("admin_logged_in", None)
    session.pop("admin_username", None)
    return redirect(url_for("admin_login"))


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

    q = request.args.get("q", "").strip().upper()
    data = read_csv_as_dict_list(RESPONSE_CSV)

    if q:
        data = [r for r in data if r.get("profile_id", "").strip().upper() == q]

    return render_template("admin_responses.html", data=data, q=q)


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
        return redirect(url_for("admin_dashboard"))

    return render_template("admin_upload.html")


@app.route("/admin/upload", methods=["GET", "POST"])
def admin_upload_alias():
    return admin_upload()


# --------------------------------------------------
# ADMIN: UPLOAD EXCEL + LINK WITH BARCODE
# --------------------------------------------------
@app.route("/admin/link-excel", methods=["GET", "POST"])
def admin_link_excel():
    if not admin_required():
        return redirect(url_for("admin_login"))

    error = ""
    success = ""

    if request.method == "POST":
        file = request.files.get("file")

        if not file or file.filename == "":
            error = "No file selected."
        else:
            filename = file.filename.strip().lower()
            if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
                error = "Only Excel files (.xlsx, .xls) are allowed."
            else:
                linked_rows, err = build_linked_rows_from_excel(file)
                if err:
                    error = err
                else:
                    fieldnames = list(linked_rows[0].keys()) if linked_rows else ["profile_id", "linked_at"]
                    write_dict_list_to_csv(LINKED_CSV, linked_rows, fieldnames)
                    update_linked_excel_file()
                    success = f"Excel linked successfully. Rows linked: {len(linked_rows)}"

    linked_data = read_csv_as_dict_list(LINKED_CSV)
    linked_headers = list(linked_data[0].keys()) if linked_data else []

    return render_template(
        "admin_link_excel.html",
        data=linked_data,
        headers=linked_headers,
        error=error,
        success=success,
    )


# --------------------------------------------------
# RUN APP
# --------------------------------------------------
if __name__ == "__main__":
    os.makedirs(BARCODE_FOLDER, exist_ok=True)
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    app.run(debug=True)

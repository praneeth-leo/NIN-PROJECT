from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, flash, jsonify
)
from flask_sqlalchemy import SQLAlchemy
import csv
import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from barcode import Code128
from barcode.writer import ImageWriter
from datetime import datetime
import uuid
from urllib.parse import quote_plus
import re
import traceback

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

# MySQL Configuration
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "")
db_host = os.getenv("DB_HOST", "localhost")
db_name = os.getenv("DB_NAME", "survey_db")

app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{db_user}:{quote_plus(db_password)}@{db_host}/{db_name}"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# --------------------------------------------------
# MODELS
# --------------------------------------------------
class Profile(db.Model):
    __tablename__ = "profiles"
    id = db.Column(db.Integer, primary_key=True)
    profile_id = db.Column(db.String(50), unique=True, nullable=False)
    name = db.Column(db.String(100))
    dob = db.Column(db.String(20))
    age = db.Column(db.String(10))
    age_full = db.Column(db.String(50))
    gender = db.Column(db.String(20))
    school = db.Column(db.String(200))
    class_name = db.Column("class", db.String(50)) # 'class' is a reserved keyword in some contexts, using class_name but mapping to 'class' column
    section = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Response(db.Model):
    __tablename__ = "responses"
    id = db.Column(db.Integer, primary_key=True)
    response_id = db.Column(db.String(50), unique=True, nullable=False)
    profile_id = db.Column(db.String(50), db.ForeignKey("profiles.profile_id"), nullable=False)
    submitted_at = db.Column(db.DateTime, default=datetime.utcnow)
    data = db.Column(db.JSON) # Store all other form fields as JSON for flexibility

class LinkedData(db.Model):
    __tablename__ = "linked_data"
    id = db.Column(db.Integer, primary_key=True)
    profile_id = db.Column(db.String(50))
    profile_found = db.Column(db.String(10))
    name = db.Column(db.String(100))
    school = db.Column(db.String(200))
    class_name = db.Column("class", db.String(50))
    section = db.Column(db.String(50))
    linked_at = db.Column(db.DateTime, default=datetime.utcnow)
    extra_data = db.Column(db.JSON)

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
        # Export Profiles
        profiles = Profile.query.all()
        p_data = []
        for p in profiles:
            p_data.append({
                "profile_id": p.profile_id,
                "name": p.name,
                "dob": p.dob,
                "age": p.age,
                "age_full": p.age_full,
                "gender": p.gender,
                "school": p.school,
                "class": p.class_name,
                "section": p.section
            })
        if p_data:
            df_p = pd.DataFrame(p_data)
            df_p.to_csv(PROFILE_CSV, index=False)
            df_p.to_excel(PROFILE_XLSX, index=False)

        # Export Responses
        responses = Response.query.all()
        r_data = []
        for r in responses:
            item = {
                "response_id": r.response_id,
                "profile_id": r.profile_id,
                "submitted_at": r.submitted_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            if r.data:
                item.update(r.data)
            r_data.append(item)
        if r_data:
            df_r = pd.DataFrame(r_data)
            df_r.to_csv(RESPONSE_CSV, index=False)
            df_r.to_excel(RESPONSE_XLSX, index=False)
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
        linked = LinkedData.query.all()
        l_data = []
        for l in linked:
            item = {
                "profile_id": l.profile_id,
                "profile_found": l.profile_found,
                "name": l.name,
                "school": l.school,
                "class": l.class_name,
                "section": l.section,
                "linked_at": l.linked_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            if l.extra_data:
                item.update(l.extra_data)
            l_data.append(item)
        
        if l_data:
            df_l = pd.DataFrame(l_data)
            df_l.to_csv(LINKED_CSV, index=False)
            df_l.to_excel(LINKED_XLSX, index=False)
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

    # Get all profiles to create a map
    profiles = Profile.query.all()
    profile_map = {p.profile_id.strip().upper(): p for p in profiles}

    rows = []
    for raw in df.to_dict(orient="records"):
        raw = {str(k).strip(): ("" if pd.isna(v) else str(v).strip()) for k, v in raw.items()}
        profile_id = raw.get(profile_id_column, "").strip().upper()
        profile = profile_map.get(profile_id)

        row = {
            "profile_id": profile_id,
            "profile_found": "yes" if profile else "no",
            "name": profile.name if profile else "",
            "school": profile.school if profile else "",
            "class": profile.class_name if profile else "",
            "section": profile.section if profile else "",
            "extra_data": {k: v for k, v in raw.items() if k != profile_id_column}
        }
        rows.append(row)

    return rows, None


def profile_exists(profile):
    return Profile.query.filter(
        Profile.name == profile["name"].strip(),
        Profile.school == profile["school"].strip(),
        Profile.class_name == profile["class"].strip(),
        Profile.section == profile["section"].strip()
    ).first() is not None


def generate_profile_id(name, school):
    name = name.strip().upper()
    school = school.strip().upper()

    name_code = name[:2] if len(name) >= 2 else name.ljust(2, "X")
    school_code = school[:2] if len(school) >= 2 else school.ljust(2, "X")

    count = Profile.query.count()

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

        profile = Profile.query.filter_by(profile_id=entered_id).first()

        if not profile:
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

        profile_data = {
            "name": form.get("name", "").strip(),
            "dob": form.get("dob", "").strip(),
            "age": form.get("age", "").strip(),
            "age_full": form.get("age_full", "").strip(),
            "gender": form.get("gender", "").strip(),
            "school": form.get("school", "").strip(),
            "class": form.get("class", "").strip(),
            "section": form.get("section", "").strip(),
        }

        if profile_exists(profile_data):
            return "Profile already exists. Please login using Barcode ID."

        profile_id = generate_profile_id(profile_data["name"], profile_data["school"])
        
        new_profile = Profile(
            profile_id=profile_id,
            name=profile_data["name"],
            dob=profile_data["dob"],
            age=profile_data["age"],
            age_full=profile_data["age_full"],
            gender=profile_data["gender"],
            school=profile_data["school"],
            class_name=profile_data["class"],
            section=profile_data["section"]
        )

        db.session.add(new_profile)
        db.session.commit()

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

    profile = Profile.query.filter_by(profile_id=profile_id).first()
    if not profile:
        return redirect(url_for("login"))

    if request.method == "POST":
        answers = request.form.to_dict()
        
        response_id = str(uuid.uuid4())
        
        new_response = Response(
            response_id=response_id,
            profile_id=profile_id,
            data=answers
        )

        db.session.add(new_response)
        db.session.commit()

        update_excel_files()
        return redirect(url_for("dashboard"))

    return render_template("form.html", profile=profile)


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

    profiles_count = Profile.query.count()
    responses_count = Response.query.count()
    linked_count = LinkedData.query.count()

    # Fetch Data for display
    profiles = Profile.query.all()
    responses = Response.query.all()
    
    # Create a mapping of profile_id -> response data for easy merging
    resp_map = {}
    # We only care about these 3 machine keys for the Profiles table
    machine_keys = ["masimo", "hemocue", "horiba"]
    
    for r in responses:
        if r.data:
            # Extract only machine data for the map
            machine_data = {k: v for k, v in r.data.items() if k in machine_keys}
            if machine_data:
                resp_map[r.profile_id] = machine_data

    p_list = []
    for p in profiles:
        item = {
            "profile_id": p.profile_id,
            "name": p.name,
            "dob": p.dob,
            "age": p.age,
            "age_full": p.age_full,
            "gender": p.gender,
            "school": p.school,
            "class": p.class_name,
            "section": p.section
        }
        # Merge only machine data if it exists
        if p.profile_id in resp_map:
            item.update(resp_map[p.profile_id])
        p_list.append(item)

    r_list = []
    all_keys = set(["response_id", "profile_id", "submitted_at"])
    for r in responses:
        item = {
            "response_id": r.response_id,
            "profile_id": r.profile_id,
            "submitted_at": r.submitted_at.strftime("%Y-%m-%d %H:%M:%S")
        }
        if r.data:
            item.update(r.data)
            all_keys.update(r.data.keys())
        r_list.append(item)

    # Get headers for profiles: Base fields + the 3 machines
    profile_headers = PROFILE_FIELDS + ["masimo", "hemocue", "horiba"]
    
    # Organize response headers: base keys first, then all other discovered keys
    base_headers = ["response_id", "profile_id", "submitted_at"]
    extra_headers = sorted(list(all_keys - set(base_headers)))
    response_headers = base_headers + extra_headers

    return render_template(
        "admin_dashboard.html",
        total_profiles=profiles_count,
        total_responses=responses_count,
        total_linked=linked_count,
        profiles=p_list,
        responses=r_list,
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
    if q:
        profiles = Profile.query.filter_by(profile_id=q).all()
    else:
        profiles = Profile.query.all()

    # Convert to list of dicts for template
    data = []
    for p in profiles:
        data.append({
            "profile_id": p.profile_id,
            "name": p.name,
            "dob": p.dob,
            "age": p.age,
            "age_full": p.age_full,
            "gender": p.gender,
            "school": p.school,
            "class": p.class_name,
            "section": p.section
        })

    return render_template("admin_profiles.html", data=data, q=q)


# --------------------------------------------------
# ADMIN: VIEW RESPONSES (SEARCH)
# --------------------------------------------------
@app.route("/admin/responses")
def admin_responses():
    if not admin_required():
        return redirect(url_for("admin_login"))

    q = request.args.get("q", "").strip().upper()
    if q:
        responses = Response.query.filter_by(profile_id=q).all()
    else:
        responses = Response.query.all()

    data = []
    for r in responses:
        item = {
            "response_id": r.response_id,
            "profile_id": r.profile_id,
            "submitted_at": r.submitted_at.strftime("%Y-%m-%d %H:%M:%S")
        }
        if r.data:
            item.update(r.data)
        data.append(item)

    return render_template("admin_responses.html", data=data, q=q)


# --------------------------------------------------
# ADMIN: DELETE PROFILE
# --------------------------------------------------
@app.route("/admin/delete-profile/<profile_id>", methods=["POST"])
def admin_delete_profile(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()
    profile = Profile.query.filter_by(profile_id=profile_id).first()

    if not profile:
        return "Profile not found"

    # Delete responses first (cascade or manual)
    Response.query.filter_by(profile_id=profile_id).delete()
    
    db.session.delete(profile)
    db.session.commit()

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

    response = Response.query.filter_by(response_id=response_id).first()
    if not response:
        return "Response not found"

    db.session.delete(response)
    db.session.commit()
    
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
    profile = Profile.query.filter_by(profile_id=profile_id).first()

    if not profile:
        return "Profile not found"

    if request.method == "POST":
        profile.name = request.form.get("name", "").strip()
        profile.dob = request.form.get("dob", "").strip()
        profile.age = request.form.get("age", "").strip()
        profile.age_full = request.form.get("age_full", "").strip()
        profile.gender = request.form.get("gender", "").strip()
        profile.school = request.form.get("school", "").strip()
        profile.class_name = request.form.get("class", "").strip()
        profile.section = request.form.get("section", "").strip()

        db.session.commit()
        update_excel_files()
        return redirect(url_for("admin_profiles"))

    return render_template("admin_edit_profile.html", p=profile)


# --------------------------------------------------
# ADMIN: EDIT RESPONSE
# --------------------------------------------------
@app.route("/admin/edit-response/<response_id>", methods=["GET", "POST"])
def admin_edit_response(response_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    response = Response.query.filter_by(response_id=response_id).first()
    if not response:
        return "Response not found"

    if request.method == "POST":
        new_data = response.data.copy() if response.data else {}
        for key in request.form.keys():
            if key == "response_id":
                continue
            new_data[key] = request.form.get(key).strip()
        
        response.data = new_data
        db.session.commit()

        update_excel_files()
        return redirect(url_for("admin_responses"))

    # Prepare data for template
    r_dict = {
        "response_id": response.response_id,
        "profile_id": response.profile_id,
        "submitted_at": response.submitted_at.strftime("%Y-%m-%d %H:%M:%S")
    }
    if response.data:
        r_dict.update(response.data)

    return render_template("admin_edit_response.html", r=r_dict)


# --------------------------------------------------
# ADMIN: EXPORT FILTERED EXCEL
# --------------------------------------------------
@app.route("/admin/export/<profile_id>")
def admin_export_filtered(profile_id):
    if not admin_required():
        return redirect(url_for("admin_login"))

    profile_id = profile_id.strip().upper()
    os.makedirs(EXPORT_FOLDER, exist_ok=True)

    profile = Profile.query.filter_by(profile_id=profile_id).first()
    responses = Response.query.filter_by(profile_id=profile_id).all()

    p_data = []
    if profile:
        p_data.append({
            "profile_id": profile.profile_id,
            "name": profile.name,
            "dob": profile.dob,
            "age": profile.age,
            "age_full": profile.age_full,
            "gender": profile.gender,
            "school": profile.school,
            "class": profile.class_name,
            "section": profile.section
        })

    r_data = []
    for r in responses:
        item = {
            "response_id": r.response_id,
            "profile_id": r.profile_id,
            "submitted_at": r.submitted_at.strftime("%Y-%m-%d %H:%M:%S")
        }
        if r.data:
            item.update(r.data)
        r_data.append(item)

    export_path = os.path.join(EXPORT_FOLDER, f"filtered_{profile_id}.xlsx")

    with pd.ExcelWriter(export_path, engine="openpyxl") as writer:
        pd.DataFrame(p_data).to_excel(writer, sheet_name="Profile", index=False)
        pd.DataFrame(r_data).to_excel(writer, sheet_name="Responses", index=False)

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

        # After upload, suggest or perform migration
        return redirect(url_for("admin_migrate_data"))

    return render_template("admin_upload.html")


@app.route("/admin/upload", methods=["GET", "POST"])
def admin_upload_alias():
    return admin_upload()


# --------------------------------------------------
# ADMIN: MIGRATE DATA (CSV -> DB)
# --------------------------------------------------
@app.route("/admin/migrate-data")
def admin_migrate_data():
    if not admin_required():
        return redirect(url_for("admin_login"))

    # Optional reset of database tables
    if request.args.get("reset") == "1":
        db.drop_all()
        db.create_all()
        # Fall through to migrate anyway

    try:
        # Migrate Profiles
        profiles_processed = 0
        with open(PROFILE_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if not row or not any(row): continue
                p_id = row[0].strip().upper()
                if not p_id: continue
                
                # Basic check for profile_id pattern to avoid junk
                if not re.match(r"^[A-Z]{2,5}\d{1,6}$", p_id):
                    # Try searching for it in the row
                    found_id = None
                    for val in row:
                        val = val.strip().upper()
                        if re.match(r"^[A-Z]{4}\d{4}$", val):
                            found_id = val
                            break
                    if found_id:
                        p_id = found_id
                    else:
                        continue # Still no valid profile ID

                if not Profile.query.filter_by(profile_id=p_id).first():
                    if len(row) == 7:
                        # old format: id, name, age, gender, school, class, section
                        new_profile = Profile(
                            profile_id=p_id,
                            name=row[1],
                            age=row[2],
                            gender=row[3],
                            school=row[4],
                            class_name=row[5],
                            section=row[6]
                        )
                    elif len(row) >= 9:
                        # new format: id, name, dob, age, age_full, gender, school, class, section
                        new_profile = Profile(
                            profile_id=p_id,
                            name=row[1],
                            dob=row[2],
                            age=row[3],
                            age_full=row[4],
                            gender=row[5],
                            school=row[6],
                            class_name=row[7],
                            section=row[8]
                        )
                    else:
                        new_profile = Profile(profile_id=p_id, name=row[1] if len(row) > 1 else "")
                    
                    db.session.add(new_profile)
                    profiles_processed += 1
        
        # Flush to allow responses to reference profiles if needed (foreign key)
        db.session.flush()

        # Migrate Responses
        responses_processed = 0
        with open(RESPONSE_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header:
                for row in reader:
                    if not row or not any(row): continue
                    
                    data = {}
                    for i, val in enumerate(row):
                        key = header[i] if i < len(header) else f"extra_{i}"
                        data[key] = val
                    
                    resp_id = data.get("response_id")
                    prof_id = data.get("profile_id")
                    sub_at_str = data.get("submitted_at")
                    
                    # Heuristic for missing response_id/profile_id or shifted columns
                    # 1. Clean up prof_id if it looks like a UUID or timestamp
                    if prof_id and (len(prof_id) > 20 or "-" in prof_id or ":" in prof_id):
                        prof_id = None

                    # 2. Extract resp_id (UUID format)
                    if not resp_id or len(resp_id) != 36:
                        resp_id = None
                        for val in row:
                            val = val.strip()
                            if len(val) == 36 and "-" in val:
                                resp_id = val
                                break
                    
                    # 3. Extract prof_id (Pattern format)
                    if not prof_id:
                        for val in row:
                            val = val.strip().upper()
                            if re.match(r"^[A-Z]{4}\d{4}$", val):
                                prof_id = val
                                break
                    
                    if not resp_id:
                        resp_id = str(uuid.uuid4())
                    
                    if not prof_id:
                        # Last ditch attempt: check index 0 as it often contains profile_id
                        first_val = row[0].strip().upper()
                        if re.match(r"^[A-Z]{2,5}\d{1,6}$", first_val):
                            prof_id = first_val

                    if not prof_id:
                        continue

                    # Ensure profile exists in DB (to satisfy foreign key)
                    if not Profile.query.filter_by(profile_id=prof_id).first():
                        # Create a stub profile if missing
                        db.session.add(Profile(profile_id=prof_id, name="Auto Migrated"))
                        db.session.flush()

                    if not Response.query.filter_by(response_id=resp_id).first():
                        survey_data = {k: v for k, v in data.items() if k not in ["response_id", "profile_id", "submitted_at"]}
                        new_response = Response(
                            response_id=resp_id,
                            profile_id=prof_id,
                            data=survey_data
                        )
                        if sub_at_str:
                            try:
                                # Try common formats
                                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"]:
                                    try:
                                        new_response.submitted_at = datetime.strptime(sub_at_str, fmt)
                                        break
                                    except:
                                        continue
                            except:
                                pass
                        db.session.add(new_response)
                        responses_processed += 1

        # Migrate Linked Data
        linked_processed = 0
        if os.path.exists(LINKED_CSV):
            with open(LINKED_CSV, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    for row in reader:
                        if not row or not any(row): continue
                        data = {}
                        for i, val in enumerate(row):
                            key = header[i] if i < len(header) else f"extra_{i}"
                            data[key] = val
                        
                        prof_id = data.get("profile_id")
                        if not prof_id: continue
                        
                        extra = {k: v for k, v in data.items() if k not in ["profile_id", "profile_found", "name", "school", "class", "section", "linked_at"]}
                        new_linked = LinkedData(
                            profile_id=prof_id,
                            profile_found=data.get("profile_found"),
                            name=data.get("name"),
                            school=data.get("school"),
                            class_name=data.get("class"),
                            section=data.get("section"),
                            extra_data=extra
                        )
                        sub_at_str = data.get("linked_at")
                        if sub_at_str:
                            try:
                                new_linked.linked_at = datetime.strptime(sub_at_str, "%Y-%m-%d %H:%M:%S")
                            except:
                                pass
                        db.session.add(new_linked)
                        linked_processed += 1

        db.session.commit()
        return f"Migration successful! Profiles: {profiles_processed}, Responses: {responses_processed}, Linked: {linked_processed}"
    except Exception as e:
        db.session.rollback()
        return f"Migration failed: {str(e)}<br><pre>{traceback.format_exc()}</pre>"


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
                    # Save to database
                    for row in linked_rows:
                        new_linked = LinkedData(
                            profile_id=row["profile_id"],
                            profile_found=row["profile_found"],
                            name=row["name"],
                            school=row["school"],
                            class_name=row["class"],
                            section=row["section"],
                            extra_data=row["extra_data"]
                        )
                        db.session.add(new_linked)
                    
                    db.session.commit()
                    update_linked_excel_file()
                    success = f"Excel linked successfully. Rows linked: {len(linked_rows)}"

    linked_data = LinkedData.query.all()
    # Prepare for template
    data = []
    headers = ["profile_id", "profile_found", "name", "school", "class", "section", "linked_at"]
    for l in linked_data:
        item = {
            "profile_id": l.profile_id,
            "profile_found": l.profile_found,
            "name": l.name,
            "school": l.school,
            "class": l.class_name,
            "section": l.section,
            "linked_at": l.linked_at.strftime("%Y-%m-%d %H:%M:%S")
        }
        if l.extra_data:
            item.update(l.extra_data)
        data.append(item)
    
    if data and not headers: # fallback
        headers = list(data[0].keys())

    return render_template(
        "admin_link_excel.html",
        data=data,
        headers=headers,
        error=error,
        success=success,
    )


# --------------------------------------------------
# ADMIN: MACHINE VIEWS
# --------------------------------------------------
def handle_machine_upload(machine_name):
    if not admin_required():
        return redirect(url_for("admin_login"))

    if request.method == "POST":
        if 'file' not in request.files:
            flash(f"No file part for {machine_name}")
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash(f"No selected file for {machine_name}")
            return redirect(request.url)

        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
                
            # Normalize columns
            original_cols = list(df.columns)
            df.columns = [str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
            
            # Identify profile_id column
            id_col = None
            id_patterns = ["profile_id", "barcode", "barcode_id", "student_barcode", "id", "study_id"]
            for col in id_patterns:
                if col in df.columns:
                    id_col = col
                    break
            
            if not id_col:
                flash(f"Error: Missing ID column in {machine_name} upload.")
                return redirect(request.url)
            
            # Identify target column: only look for exactly the machine name
            target_col = machine_name.lower()
            if target_col not in df.columns:
                # Fallback: if there's only one other column, use it, or fail
                other_cols = [c for c in df.columns if c != id_col]
                if len(other_cols) == 1:
                    target_col = other_cols[0]
                else:
                    flash(f"Error: Could not find '{target_col}' column in the file.")
                    return redirect(request.url)
                
            updated_count = 0
            for _, row in df.iterrows():
                profile_id = str(row[id_col]).strip().upper()
                resp = Response.query.filter_by(profile_id=profile_id).first()
                if resp:
                    current_data = resp.data.copy() if resp.data else {}
                    val = row[target_col]
                    if pd.notna(val):
                        # Use the machine_name as the standard key
                        current_data[machine_name.lower()] = str(val)
                        resp.data = current_data
                        updated_count += 1
            
            db.session.commit()
            flash(f"Successfully updated {updated_count} records with {machine_name} data.")
        except Exception as e:
            db.session.rollback()
            flash(f"Error processing {machine_name} upload: {str(e)}")
            
        return redirect(request.url)
    
    return render_template(f"{machine_name.lower()}.html")


@app.route("/masimo", methods=["GET", "POST"])
def masimo():
    return handle_machine_upload("Masimo")


@app.route("/hemocue", methods=["GET", "POST"])
def hemocue():
    return handle_machine_upload("Hemocue")


@app.route("/horiba")
def horiba():
    if not admin_required():
        return redirect(url_for("admin_login"))
    
    # Fetch all profiles and merge machine data for the table
    profiles = Profile.query.all()
    responses = Response.query.all()
    resp_map = {r.profile_id: r.data for r in responses if r.data}
    
    p_entries = []
    for p in profiles:
        data = resp_map.get(p.profile_id, {})
        p_entries.append({
            "profile_id": p.profile_id,
            "name": p.name,
            "masimo": data.get("masimo", ""),
            "hemocue": data.get("hemocue", ""),
            "horiba": data.get("horiba", "")
        })
        
    return render_template("horiba.html", profiles=p_entries)


@app.route("/admin/update-horiba", methods=["POST"])
def update_horiba():
    if not admin_required():
        return jsonify({"success": False, "error": "Unauthorized"}), 403
        
    profile_id = request.form.get("profile_id")
    horiba_val = request.form.get("horiba")
    
    if not profile_id:
        return jsonify({"success": False, "error": "Missing Profile ID"}), 400
        
    resp = Response.query.filter_by(profile_id=profile_id).first()
    if not resp:
        return jsonify({"success": False, "error": "Response record not found for this profile"}), 404
        
    try:
        current_data = resp.data.copy() if resp.data else {}
        current_data["horiba"] = horiba_val
        resp.data = current_data
        db.session.commit()
        return jsonify({"success": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "error": str(e)}), 500


# --------------------------------------------------
# RUN APP
# --------------------------------------------------
if __name__ == "__main__":
    os.makedirs(BARCODE_FOLDER, exist_ok=True)
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    with app.app_context():
        db.create_all()
    app.run(debug=True)

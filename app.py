from flask import Flask, render_template, request, redirect, url_for, session
import csv
import os
import pandas as pd
from barcode import Code128
from barcode.writer import ImageWriter

# --------------------------------------------------
# App setup
# --------------------------------------------------
app = Flask(__name__)
app.secret_key = "survey_secret_key"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROFILE_CSV = os.path.join(BASE_DIR, "profiles.csv")
RESPONSE_CSV = os.path.join(BASE_DIR, "responses.csv")
PROFILE_XLSX = os.path.join(BASE_DIR, "profiles.xlsx")
RESPONSE_XLSX = os.path.join(BASE_DIR, "responses.xlsx")

BARCODE_FOLDER = os.path.join(BASE_DIR, "static", "barcodes")

PROFILE_FIELDS = [
    "profile_id", "name", "age", "gender", "school", "class", "section"
]

# --------------------------------------------------
# CSV → EXCEL AUTO UPDATE
# --------------------------------------------------
def update_excel_files():
    try:
        if os.path.exists(PROFILE_CSV):
            pd.read_csv(PROFILE_CSV).to_excel(PROFILE_XLSX, index=False)
        if os.path.exists(RESPONSE_CSV):
            pd.read_csv(RESPONSE_CSV).to_excel(RESPONSE_XLSX, index=False)
    except Exception as e:
        print("Excel error:", e)

# --------------------------------------------------
# DUPLICATE PROFILE CHECK
# --------------------------------------------------
def profile_exists(profile):
    if not os.path.exists(PROFILE_CSV):
        return False

    with open(PROFILE_CSV, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (
                row["name"] == profile["name"] and
                row["school"] == profile["school"] and
                row["class"] == profile["class"] and
                row["section"] == profile["section"]
            ):
                return True
    return False

# --------------------------------------------------
# PROFILE ID GENERATOR
# --------------------------------------------------
def generate_profile_id(name, school):
    name = name.strip().upper()
    school = school.strip().upper()

    name_code = name[:2] if len(name) >= 2 else name.ljust(2, "X")
    school_code = school[:2] if len(school) >= 2 else school.ljust(2, "X")

    count = 0
    if os.path.exists(PROFILE_CSV):
        with open(PROFILE_CSV, "r") as f:
            count = len(list(csv.reader(f))) - 1
            if count < 0:
                count = 0

    return f"{name_code}{school_code}{count + 1:04d}"

# --------------------------------------------------
# BARCODE GENERATION (ONLY ONCE)
# --------------------------------------------------
def generate_barcode(profile_id):
    barcode = Code128(profile_id, writer=ImageWriter())
    path = os.path.join(BARCODE_FOLDER, profile_id)
    barcode.save(path)
    return f"barcodes/{profile_id}.png"

# --------------------------------------------------
# DASHBOARD
# --------------------------------------------------
@app.route("/")
def dashboard():
    return render_template("dashboard.html")

# --------------------------------------------------
# PROFILE PAGE (BARCODE GENERATED HERE)
# --------------------------------------------------
@app.route("/profile", methods=["GET", "POST"])
def profile():
    if request.method == "POST":
        form = request.form.to_dict()

        profile_row = {
            "profile_id": "",
            "name": form.get("name"),
            "age": form.get("age"),
            "gender": form.get("gender"),
            "school": form.get("school"),
            "class": form.get("class"),
            "section": form.get("section"),
        }

        if profile_exists(profile_row):
            return "Profile already exists. Do not submit again."

        profile_id = generate_profile_id(
            profile_row["name"], profile_row["school"]
        )
        profile_row["profile_id"] = profile_id

        file_exists = os.path.exists(PROFILE_CSV)
        with open(PROFILE_CSV, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=PROFILE_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(profile_row)

        update_excel_files()

        # ✅ BARCODE GENERATED HERE (ONLY PLACE)
        barcode_path = generate_barcode(profile_id)
        session["barcode_path"] = barcode_path
        session["profile_id"] = profile_id

        return redirect(url_for("profile_view"))

    return render_template("profile.html")

# --------------------------------------------------
# SURVEY FORM (NO BARCODE HERE)
# --------------------------------------------------
@app.route("/form", methods=["GET", "POST"])
def form():
    if request.method == "POST":
        profile_id = session.get("profile_id")
        if not profile_id:
            return redirect(url_for("profile"))

        answers = request.form.to_dict()
        answers["profile_id"] = profile_id

        file_exists = os.path.exists(RESPONSE_CSV)
        with open(RESPONSE_CSV, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=answers.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(answers)

        update_excel_files()

        return redirect(url_for("dashboard"))

    return render_template("form.html")

# --------------------------------------------------
# PROFILE VIEW (SHOW BARCODE)
# --------------------------------------------------
@app.route("/profile-view")
def profile_view():
    return render_template(
        "profile_view.html",
        profile_id=session.get("profile_id"),
        barcode_path=session.get("barcode_path")
    )

# --------------------------------------------------
# MACHINE STATIC PAGES
# --------------------------------------------------
@app.route("/machine1")
def machine1():
    return render_template("machine1.html")

@app.route("/machine2")
def machine2():
    return render_template("machine2.html")

@app.route("/machine3")
def machine3():
    return render_template("machine3.html")

# --------------------------------------------------
# RUN APP
# --------------------------------------------------
if __name__ == "__main__":
    os.makedirs(BARCODE_FOLDER, exist_ok=True)
    app.run(debug=True)

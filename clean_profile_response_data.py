from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


DOWNLOADS_DIR = Path(r"C:\Users\OMEN\Downloads")
PROFILES_PATH = DOWNLOADS_DIR / "profiles.csv"
RESPONSES_PATH = DOWNLOADS_DIR / "responses (1).csv"
AUDIT_PATH = DOWNLOADS_DIR / "response_save_audit.csv"

OUTPUT_DIR = Path(__file__).resolve().parent / "cleaned_output"
OUTPUT_PROFILES_PATH = OUTPUT_DIR / "profiles.csv"
OUTPUT_RESPONSE_PATH = OUTPUT_DIR / "response.csv"
OUTPUT_REPORT_PATH = OUTPUT_DIR / "cleanup_report.txt"


@dataclass
class CleanupStats:
    response_rows: int = 0
    response_rows_from_main: int = 0
    response_rows_from_audit_only: int = 0
    response_blank_fields_filled_from_audit: int = 0
    profiles_created_at_filled: int = 0
    profiles_name_filled: int = 0
    profiles_surname_filled: int = 0
    profiles_dob_filled: int = 0
    profiles_age_filled: int = 0
    profiles_age_full_filled: int = 0
    profiles_gender_filled: int = 0
    profiles_school_filled: int = 0
    profiles_location_filled: int = 0


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = [{key: (value or "").strip() for key, value in row.items()} for row in reader]
        return list(reader.fieldnames or []), rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_date(value: str) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.date()
        except ValueError:
            continue
    return None


def format_age_full(dob_value: str, ref_value: str) -> str:
    dob = parse_date(dob_value)
    ref = parse_date(ref_value)
    if not dob or not ref or ref < dob:
        return ""

    years = ref.year - dob.year
    months = ref.month - dob.month
    days = ref.day - dob.day

    if days < 0:
        months -= 1
        previous_month = ref.month - 1 or 12
        previous_year = ref.year if ref.month > 1 else ref.year - 1
        if previous_month == 12:
            next_month = date(previous_year + 1, 1, 1)
        else:
            next_month = date(previous_year, previous_month + 1, 1)
        current_month = date(previous_year, previous_month, 1)
        days_in_previous_month = (next_month - current_month).days
        days += days_in_previous_month

    if months < 0:
        years -= 1
        months += 12

    return f"{years} years {months} months {days} days"


def split_name(full_name: str, existing_name: str) -> tuple[str, str]:
    full = " ".join((full_name or "").split())
    current_name = " ".join((existing_name or "").split())
    if not full:
        return current_name, ""

    parts = full.split()
    if not current_name:
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])

    normalized_full = full.casefold()
    normalized_name = current_name.casefold()
    if normalized_full == normalized_name:
        return current_name, ""

    if normalized_full.startswith(normalized_name + " "):
        return current_name, full[len(current_name):].strip()

    return current_name, ""


def infer_gender_from_profile_id(profile_id: str) -> str:
    match = re.match(r"^[A-Za-z]{2}\d{6}([MF])", (profile_id or "").strip())
    if not match:
        return ""
    return "Male" if match.group(1) == "M" else "Female"


def choose_response_rows(
    response_fields: list[str],
    response_rows: list[dict[str, str]],
    audit_rows: list[dict[str, str]],
    stats: CleanupStats,
) -> dict[str, dict[str, str]]:
    merged = {row["profile_id"]: dict(row) for row in response_rows}
    audit_by_profile = {row["profile_id"]: dict(row) for row in audit_rows}

    stats.response_rows_from_main = len(merged)

    for profile_id, audit_row in audit_by_profile.items():
        if profile_id not in merged:
            merged[profile_id] = {field: audit_row.get(field, "") for field in response_fields}
            stats.response_rows_from_audit_only += 1
            continue

        current = merged[profile_id]
        for field in response_fields:
            if current.get(field, ""):
                continue
            audit_value = audit_row.get(field, "")
            if audit_value:
                current[field] = audit_value
                stats.response_blank_fields_filled_from_audit += 1

    stats.response_rows = len(merged)
    return merged


def fill_profile_row(profile: dict[str, str], response: dict[str, str] | None, stats: CleanupStats) -> dict[str, str]:
    row = dict(profile)
    response = response or {}

    participant_name = response.get("participant_name", "")
    first_name, inferred_surname = split_name(participant_name, row.get("name", ""))

    if not row.get("name", "") and first_name:
        row["name"] = first_name
        stats.profiles_name_filled += 1

    if not row.get("surname", "") and inferred_surname:
        row["surname"] = inferred_surname
        stats.profiles_surname_filled += 1

    if not row.get("created_at", "") and response.get("submitted_at", ""):
        row["created_at"] = response["submitted_at"]
        stats.profiles_created_at_filled += 1

    if not row.get("dob", "") and response.get("dob", ""):
        row["dob"] = response["dob"]
        stats.profiles_dob_filled += 1

    if not row.get("gender", ""):
        replacement = response.get("sex", "") or infer_gender_from_profile_id(row.get("profile_id", ""))
        if replacement:
            row["gender"] = replacement
            stats.profiles_gender_filled += 1

    if not row.get("school", "") and response.get("school_anganwadi_name", ""):
        row["school"] = response["school_anganwadi_name"]
        stats.profiles_school_filled += 1

    if not row.get("location", "") and response.get("location_type", ""):
        row["location"] = response["location_type"]
        stats.profiles_location_filled += 1

    if not row.get("age_full", ""):
        replacement = response.get("age_full", "") or format_age_full(row.get("dob", ""), row.get("created_at", ""))
        if replacement:
            row["age_full"] = replacement
            stats.profiles_age_full_filled += 1

    if not row.get("age", ""):
        replacement = response.get("age_completed", "")
        if not replacement and row.get("age_full", ""):
            match = re.match(r"^(\d+)", row["age_full"])
            replacement = match.group(1) if match else ""
        if not replacement:
            computed_age = format_age_full(row.get("dob", ""), row.get("created_at", ""))
            match = re.match(r"^(\d+)", computed_age)
            replacement = match.group(1) if match else ""
        if replacement:
            row["age"] = replacement
            stats.profiles_age_filled += 1

    return row


def build_report(
    profiles_rows: list[dict[str, str]],
    merged_response_rows: dict[str, dict[str, str]],
    stats: CleanupStats,
) -> str:
    missing_response_ids = [row["profile_id"] for row in profiles_rows if row["profile_id"] not in merged_response_rows]
    suspicious_rows = []
    for row in profiles_rows:
        profile_id = row.get("profile_id", "")
        created_at = row.get("created_at", "")
        if profile_id in {"34WEEK", "MASIMOHEMOCUEHORIBA"}:
            suspicious_rows.append(profile_id)
        elif created_at and not created_at.startswith("202"):
            suspicious_rows.append(profile_id)

    lines = [
        "CSV cleanup summary",
        "",
        f"Merged response rows: {stats.response_rows}",
        f"Rows kept from responses (1).csv: {stats.response_rows_from_main}",
        f"Rows added from response_save_audit.csv only: {stats.response_rows_from_audit_only}",
        f"Blank response fields backfilled from audit: {stats.response_blank_fields_filled_from_audit}",
        "",
        "Profile fields backfilled:",
        f"created_at: {stats.profiles_created_at_filled}",
        f"name: {stats.profiles_name_filled}",
        f"surname: {stats.profiles_surname_filled}",
        f"dob: {stats.profiles_dob_filled}",
        f"age: {stats.profiles_age_filled}",
        f"age_full: {stats.profiles_age_full_filled}",
        f"gender: {stats.profiles_gender_filled}",
        f"school: {stats.profiles_school_filled}",
        f"location: {stats.profiles_location_filled}",
        "",
        f"Profiles without any response row after merge: {len(missing_response_ids)}",
        ", ".join(missing_response_ids[:50]) if missing_response_ids else "None",
        "",
        f"Suspicious profile rows that still need manual review: {len(suspicious_rows)}",
        ", ".join(suspicious_rows) if suspicious_rows else "None",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    profile_fields, profile_rows = read_csv(PROFILES_PATH)
    response_fields, response_rows = read_csv(RESPONSES_PATH)
    _, audit_rows = read_csv(AUDIT_PATH)

    stats = CleanupStats()
    merged_responses = choose_response_rows(response_fields, response_rows, audit_rows, stats)

    cleaned_profiles = []
    for row in profile_rows:
        cleaned_profiles.append(fill_profile_row(row, merged_responses.get(row["profile_id"]), stats))

    ordered_response_rows = []
    seen = set()
    for row in cleaned_profiles:
        profile_id = row["profile_id"]
        if profile_id in merged_responses:
            ordered_response_rows.append({field: merged_responses[profile_id].get(field, "") for field in response_fields})
            seen.add(profile_id)
    for profile_id, response_row in merged_responses.items():
        if profile_id not in seen:
            ordered_response_rows.append({field: response_row.get(field, "") for field in response_fields})

    write_csv(OUTPUT_PROFILES_PATH, profile_fields, cleaned_profiles)
    write_csv(OUTPUT_RESPONSE_PATH, response_fields, ordered_response_rows)
    OUTPUT_REPORT_PATH.write_text(build_report(cleaned_profiles, merged_responses, stats), encoding="utf-8")


if __name__ == "__main__":
    main()

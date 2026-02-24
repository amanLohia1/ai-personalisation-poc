"""
People extractor — pure functions for extracting and normalising people
records from a raw Backyard JSONL dump.

Inferred entities
-----------------
- People         - core entity
- Country        — normalised from user_country
- WorkDivision   — from user_work_division
- Department     — identity = (name, division_name)

Division / Department null-handling strategy
--------------------------------------------
- dept present, division present  → normal
- dept present, division null     → division becomes "_UNASSIGNED_"
- dept null     (any division)    → no department entity for this person
- both null                       → no org-structure edges for this person

Usage (notebook or script):
    from src.extractors.people_extractor import extract_people, write_normalized
    result = extract_people(raw_file)
    write_normalized(result, output_dir)
"""

import json
from pathlib import Path

from src.utils.utils import normalize_country_string


# =====================================================
# Constants
# =====================================================

UNASSIGNED_DIVISION = "_UNASSIGNED_"

PERSON_KEYS = [
    "id",
    "user_name",
    "user_work_title",
    "user_about",
    "is_active",
    "user_department",
    "user_manager_id",
    "user_work_division",
    "user_country",
]


# =====================================================
# Helpers
# =====================================================

def clean_string(value):
    """Strip and discard empty / placeholder strings."""
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value.startswith("@@"):
        return None
    return value


def normalize_person(obj):
    """
    Pick the required keys from a raw record, clean strings,
    normalise country, and apply division/department null-handling.
    """
    normalized = {}

    for key in PERSON_KEYS:
        value = obj.get(key)
        if isinstance(value, str):
            value = clean_string(value)
        normalized[key] = value

    # Country normalisation
    raw_country = normalized.get("user_country")
    normalized["user_normalized_country"] = normalize_country_string(raw_country)

    # Division / Department null-handling
    dept = normalized["user_department"]
    div  = normalized["user_work_division"]

    if dept is not None and div is None:
        normalized["user_work_division"] = UNASSIGNED_DIVISION

    return normalized


# =====================================================
# Core extraction
# =====================================================

def extract_people(raw_file):
    """
    Read *raw_file* (JSONL) and return an extraction result dict:
        {
            "people":          [list of normalised person dicts],
            "countries":       {set of country strings},
            "work_divisions":  {set of division strings},
            "departments":     {set of (dept_name, division_name) tuples},
        }
    """
    raw_file = Path(raw_file)

    people = []
    countries = set()
    work_divisions = set()
    departments = set()           # identity = (name, division)

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            if obj.get("object_type") != "people":
                continue

            person = normalize_person(obj)

            if not person["id"]:
                continue

            people.append(person)

            # --- Collect inferred entities ---

            if person["user_normalized_country"]:
                countries.add(person["user_normalized_country"])

            div = person["user_work_division"]
            if div:
                work_divisions.add(div)

            dept = person["user_department"]
            if dept:
                # div is guaranteed non-null here (normalize_person handles it)
                departments.add((dept, div))

    return {
        "people": people,
        "countries": countries,
        "work_divisions": work_divisions,
        "departments": departments,
    }


# =====================================================
# Persist normalised artefacts
# =====================================================

def write_normalized(result, output_dir):
    """
    Write the extraction *result* dict to JSONL files under *output_dir*.
    Creates the directory if it doesn't exist.

    Files written:
        people.jsonl, countries.jsonl, work_divisions.jsonl, departments.jsonl
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    people         = result["people"]
    countries      = result["countries"]
    work_divisions = result["work_divisions"]
    departments    = result["departments"]

    # People
    with open(output_dir / "people.jsonl", "w", encoding="utf-8") as f:
        for p in people:
            f.write(json.dumps(p) + "\n")

    # Countries
    with open(output_dir / "countries.jsonl", "w", encoding="utf-8") as f:
        for c in sorted(countries):
            f.write(json.dumps({"name": c}) + "\n")

    # Work divisions
    with open(output_dir / "work_divisions.jsonl", "w", encoding="utf-8") as f:
        for w in sorted(work_divisions):
            f.write(json.dumps({"name": w}) + "\n")

    # Departments (scoped to division)
    with open(output_dir / "departments.jsonl", "w", encoding="utf-8") as f:
        for dept_name, div_name in sorted(
            departments, key=lambda x: (x[0] or "", x[1] or "")
        ):
            f.write(
                json.dumps({"name": dept_name, "division_name": div_name})
                + "\n"
            )

"""
Site extractor — pure functions for extracting and normalising site
records from a raw Backyard JSONL dump.

Inferred entities
-----------------
- Site           — core entity
- SiteCategory   — from site_category_name / site_category_id (1:1, no conflicts)

Cross-entity references (to People, already ingested)
-----------------------------------------------------
- createdbyid      → People.id
- lastmodifiedbyid → People.id

Site node fields
----------------
id, site_name, site_category_name, site_category_id, site_type,
site_is_featured, site_description, is_active,
site_member_count, site_owner_manager_count, site_follower_count,
createddate, lastmodifieddate, createdbyid, lastmodifiedbyid

Usage:
    from src.extractors.site_extractor import extract_sites, write_normalized
    result = extract_sites(raw_file)
    write_normalized(result, output_dir)
"""

import json
from pathlib import Path


# =====================================================
# Constants
# =====================================================

SITE_KEYS = [
    "id",
    "site_name",
    "site_category_name",
    "site_category_id",
    "site_type",
    "site_is_featured",
    "site_description",
    "is_active",
    "site_member_count",
    "site_owner_manager_count",
    "site_follower_count",
    "createddate",
    "lastmodifieddate",
    "createdbyid",
    "lastmodifiedbyid",
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


def normalize_site(obj):
    """
    Pick the required keys from a raw record and clean string fields.
    """
    normalized = {}

    for key in SITE_KEYS:
        value = obj.get(key)
        if isinstance(value, str):
            value = clean_string(value)
        normalized[key] = value

    return normalized


# =====================================================
# Core extraction
# =====================================================

def extract_sites(raw_file):
    """
    Read *raw_file* (JSONL) and return an extraction result dict:
        {
            "sites":            [list of normalised site dicts],
            "site_categories":  {set of (category_name, category_id) tuples},
        }
    """
    raw_file = Path(raw_file)

    sites = []
    site_categories = set()

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            if obj.get("object_type") != "site":
                continue

            site = normalize_site(obj)

            if not site["id"]:
                continue

            sites.append(site)

            # --- Collect inferred entities ---
            cat_name = site["site_category_name"]
            cat_id = site["site_category_id"]
            if cat_name and cat_id:
                site_categories.add((cat_name, cat_id))

    return {
        "sites": sites,
        "site_categories": site_categories,
    }


# =====================================================
# Persist normalised artefacts
# =====================================================

def write_normalized(result, output_dir):
    """
    Write the extraction *result* dict to JSONL files under *output_dir*.
    Creates the directory if it doesn't exist.

    Files written:
        sites.jsonl, site_categories.jsonl
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sites = result["sites"]
    site_categories = result["site_categories"]

    # Sites
    with open(output_dir / "sites.jsonl", "w", encoding="utf-8") as f:
        for s in sites:
            f.write(json.dumps(s) + "\n")

    # Site categories
    with open(output_dir / "site_categories.jsonl", "w", encoding="utf-8") as f:
        for cat_name, cat_id in sorted(site_categories, key=lambda x: x[0]):
            f.write(json.dumps({"name": cat_name, "id": cat_id}) + "\n")

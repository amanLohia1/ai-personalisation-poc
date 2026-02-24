"""
Content extractor — pure functions for extracting and normalising content
records from a raw Backyard JSONL dump.

Inferred entities
-----------------
- Content           — core entity
- SitePageCategory  — from content_page_category_id / content_page_category_name
                      (site-scoped; identity = page_category_id, with site_id stored)

Cross-entity references (to People / Site / File, already ingested)
-------------------------------------------------------------------
- createdbyid         → People.id
- lastmodifiedbyid    → People.id
- content_site_id     → Site.id
- content_files[*].file_id → File.id  (mostly images, excluded in file pipeline)

:Content node fields
--------------------
id, content_title, content_type, content_sub_type, content_status,
content_is_featured, content_is_must_read, is_deleted,
_aggregated_locations, createddate, lastmodifieddate,
content_first_published_on, content_publish_on_date, content_validated_on

Fields intentionally ignored
-----------------------------
content_body, content_snippet, content_summary,
content_topics, content_audiences, content_rsvp, content_rsvp_details,
content_list_of_album_media, content_img_*, content_site_*,
content_authored_by_name, content_modified_by_name,
content_event_timezone_*, content_exclude_from_search,
content_skip_smart_answer, content_onboarding_status,
content_dismiss_validation_warning, content_is_restricted,
content_likes_count, content_is_parent, parent_content_id,
language, original_language,
manual_translation_enabled, translated_contents, autocomplete,
object_type

Usage:
    from src.extractors.content_extractor import extract_contents, write_normalized
    result = extract_contents(raw_file)
    write_normalized(result, output_dir)
"""

import json
from pathlib import Path


# =====================================================
# Constants
# =====================================================

CONTENT_KEYS = [
    "id",
    "content_title",
    "content_type",
    "content_sub_type",
    "content_status",
    "content_is_featured",
    "content_is_must_read",
    "is_deleted",
    "_aggregated_locations",
    "createddate",
    "lastmodifieddate",
    "content_first_published_on",
    "content_publish_on_date",
    "content_validated_on",
    # relationship FKs — kept on the record so the loader can build edges
    "createdbyid",
    "lastmodifiedbyid",
    "content_site_id",
    "content_page_category_id",
    "content_page_category_name",
    "content_files",
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


def extract_file_ids(content_files):
    """Extract unique file IDs from the content_files list-of-dicts."""
    if not isinstance(content_files, list):
        return []

    seen = set()
    result = []
    for item in content_files:
        if not isinstance(item, dict):
            continue
        file_id = clean_string(item.get("file_id"))
        if file_id and file_id not in seen:
            seen.add(file_id)
            result.append(file_id)

    return result


def normalize_content(obj):
    """
    Pick the required keys from a raw record, clean strings,
    and flatten content_files to a list of file IDs.
    """
    normalized = {}

    for key in CONTENT_KEYS:
        value = obj.get(key)
        if isinstance(value, str):
            value = clean_string(value)
        normalized[key] = value

    # Flatten content_files to a list of file IDs
    normalized["content_file_ids"] = extract_file_ids(normalized.pop("content_files", None))

    return normalized


# =====================================================
# Core extraction
# =====================================================

def extract_contents(raw_file):
    """
    Read *raw_file* (JSONL) and return an extraction result dict::

        {
            "contents":          [list of normalised content dicts],
            "page_categories":   {set of (site_id, category_id, category_name)},
            "num_no_site":       int,
            "num_no_category":   int,
            "num_with_files":    int,
        }

    Only ``object_type == "content"`` records are considered.
    """
    raw_file = Path(raw_file)

    contents = []
    page_categories = set()
    num_no_site = 0
    num_no_category = 0
    num_with_files = 0

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            if obj.get("object_type") != "content":
                continue

            record = normalize_content(obj)

            if not record["id"]:
                continue

            # Track stats
            if not record.get("content_site_id"):
                num_no_site += 1

            cat_id = record.get("content_page_category_id")
            cat_name = record.get("content_page_category_name")
            site_id = record.get("content_site_id")

            if cat_id and site_id:
                page_categories.add((site_id, cat_id, cat_name))
            else:
                num_no_category += 1

            if record.get("content_file_ids"):
                num_with_files += 1

            contents.append(record)

    return {
        "contents": contents,
        "page_categories": page_categories,
        "num_no_site": num_no_site,
        "num_no_category": num_no_category,
        "num_with_files": num_with_files,
    }


# =====================================================
# Persist normalised artefacts
# =====================================================

def write_normalized(result, output_dir):
    """
    Write the extraction *result* dict to JSONL files under *output_dir*.
    Creates the directory if it doesn't exist.

    Files written:
        contents.jsonl
        content_page_categories.jsonl
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Contents
    contents = result["contents"]
    with open(output_dir / "contents.jsonl", "w", encoding="utf-8") as f:
        for record in contents:
            f.write(json.dumps(record) + "\n")

    print(f"Written {len(contents)} content records → {output_dir / 'contents.jsonl'}")

    # Page categories (site-scoped)
    cats = sorted(result["page_categories"])
    with open(output_dir / "content_page_categories.jsonl", "w", encoding="utf-8") as f:
        for site_id, cat_id, cat_name in cats:
            f.write(json.dumps({"site_id": site_id, "id": cat_id, "name": cat_name}) + "\n")

    print(f"Written {len(cats)} page category records → {output_dir / 'content_page_categories.jsonl'}")

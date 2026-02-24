"""
File extractor — pure functions for extracting and normalising file
records from a raw Backyard JSONL dump.

Extraction rules
----------------
- Only records where ``object_type == "file"`` are processed.
- Images (``file_is_image == True``) are **excluded** — they provide no
  semantic value in the graph.
- Orphan files (no ``file_site_mapping`` entries) are kept as isolated
  :File nodes; site relationships are created only when the site exists.

:File node fields
-----------------
id, file_name, file_type, file_mime_type, file_provider,
is_active, is_deleted, file_url,
_aggregated_locations, createddate, lastmodifieddate

Cross-entity references (to People / Site, already ingested)
------------------------------------------------------------
- createdbyid        → People.id
- file_site_mapping  → [Site.id, ...]   (list — one :File may map to N sites)
- file_content_mappings → [Content.id, ...] (union of file_content_id + file_content_mapping[*].id)

Fields intentionally ignored
-----------------------------
file_data, file_s3_path, file_size, file_page_count, file_thumbnail_url,
file_external_id, file_title, file_author,
file_creation_date, file_language, file_keywords, file_transcript,
extracted_info, file_owner_id (always == createdbyid),
file_site_id (redundant with file_site_mapping),
file_site_name, file_site_type, file_site_is_active,
file_owner_name, autocomplete,
_s3_orchestrator_processing_file_key,
_s3_orchestrator_processing_bucket_key

Usage:
    from src.extractors.file_extractor import extract_files, write_normalized
    result = extract_files(raw_file)
    write_normalized(result, output_dir)
"""

import json
from pathlib import Path


# =====================================================
# Constants
# =====================================================

FILE_KEYS = [
    "id",
    "file_name",
    "file_type",
    "file_mime_type",
    "file_provider",
    "is_active",
    "is_deleted",
    "file_url",
    "_aggregated_locations",
    "createddate",
    "lastmodifieddate",
    # relationship FKs — kept on the record so the loader can build edges
    "createdbyid",
    "file_site_mapping",
    "file_content_id",
    "file_content_mapping",
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


def clean_id_list(values):
    """Return a clean list[str] from possibly-null/mixed list values."""
    if not isinstance(values, list):
        return []

    cleaned = []
    for value in values:
        if not isinstance(value, str):
            continue
        value = clean_string(value)
        if value:
            cleaned.append(value)

    return cleaned


def clean_content_mapping_ids(values):
    """Extract clean content IDs from raw file_content_mapping list[object]."""
    if not isinstance(values, list):
        return []

    cleaned = []
    for value in values:
        if not isinstance(value, dict):
            continue

        content_id = clean_string(value.get("id"))
        if content_id:
            cleaned.append(content_id)

    return cleaned


def build_content_mappings(file_content_id, file_content_mapping):
    """Build sorted unique list of content IDs from scalar + list fields."""
    combined = []

    scalar_id = clean_string(file_content_id) if isinstance(file_content_id, str) else None
    if scalar_id:
        combined.append(scalar_id)

    combined.extend(clean_content_mapping_ids(file_content_mapping))

    seen = set()
    unique = []
    for value in combined:
        if value not in seen:
            seen.add(value)
            unique.append(value)

    return unique


def normalize_file(obj):
    """
    Pick the required keys from a raw record and clean string fields.
    Non-string scalars (bool, int) and list fields are kept as-is.
    """
    normalized = {}

    for key in FILE_KEYS:
        value = obj.get(key)
        if isinstance(value, str):
            value = clean_string(value)
        normalized[key] = value

    normalized["file_site_mapping"] = clean_id_list(normalized.get("file_site_mapping"))
    normalized["file_content_mappings"] = build_content_mappings(
        normalized.get("file_content_id"),
        normalized.get("file_content_mapping"),
    )

    # Keep only the unified field in normalized output
    normalized.pop("file_content_id", None)
    normalized.pop("file_content_mapping", None)

    return normalized


# =====================================================
# Core extraction
# =====================================================

def extract_files(raw_file):
    """
    Read *raw_file* (JSONL) and return an extraction result dict::

        {
            "files":            [list of normalised file dicts],
            "num_skipped_images":   int,   # records excluded because file_is_image=True
            "num_orphan_files":     int,   # files with no file_site_mapping entries
            "num_nonempty_content_mappings": int,
            "num_empty_content_mappings": int,
        }

    Only ``object_type == "file"`` records are considered.
    Records with ``file_is_image == True`` are skipped.
    """
    raw_file = Path(raw_file)

    files = []
    num_skipped_images = 0
    num_orphan_files = 0
    num_nonempty_content_mappings = 0
    num_empty_content_mappings = 0

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            if obj.get("object_type") != "file":
                continue

            # Exclude image files
            if obj.get("file_is_image") is True:
                num_skipped_images += 1
                continue

            file_record = normalize_file(obj)

            if not file_record["id"]:
                continue

            if not file_record["file_site_mapping"]:
                num_orphan_files += 1

            if file_record.get("file_content_mappings"):
                num_nonempty_content_mappings += 1
            else:
                num_empty_content_mappings += 1

            files.append(file_record)

    return {
        "files": files,
        "num_skipped_images": num_skipped_images,
        "num_orphan_files": num_orphan_files,
        "num_nonempty_content_mappings": num_nonempty_content_mappings,
        "num_empty_content_mappings": num_empty_content_mappings,
    }


# =====================================================
# Persist normalised artefacts
# =====================================================

def write_normalized(result, output_dir):
    """
    Write the extraction *result* dict to JSONL files under *output_dir*.
    Creates the directory if it doesn't exist.

    Files written:
        files.jsonl
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = result["files"]

    with open(output_dir / "files.jsonl", "w", encoding="utf-8") as f:
        for record in files:
            f.write(json.dumps(record) + "\n")

    print(f"Written {len(files)} file records → {output_dir / 'files.jsonl'}")

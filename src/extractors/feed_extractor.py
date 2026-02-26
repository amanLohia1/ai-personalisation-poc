"""
Feed extractor — pure functions for extracting and normalising feed
records from a raw Backyard JSONL dump.

Scope enforced in this phase
----------------------------
- Include only object_type == "feed"
- Include only feed_type in {"post", "recognition"}
- For feed_type == "post", include only feed_variant == "standard"
- Exclude feed_type == "timeline"
- Exclude post variants {"shared_content", "shared_recognition"}

Inferred entities
-----------------
- Post
- Recognition

Cross-entity references (to already-ingested entities)
-------------------------------------------------------
- createdbyid / modifiedbyid           -> People.id
- feed_site_id                         -> Site.id
- feed_content_id                      -> Content.id
- feed_list_of_mentions[*]             -> People/Site/Content (by mention type)
- feed_associated_entity.recognitionAwardedToIds[*].userId -> People.id

Usage:
    from src.extractors.feed_extractor import extract_feeds, write_normalized
    result = extract_feeds(raw_file)
    write_normalized(result, output_dir)
"""

import json
from pathlib import Path


# =====================================================
# Constants
# =====================================================

ALLOWED_FEED_TYPES = {"post", "recognition"}
ALLOWED_POST_VARIANTS = {"standard"}

POST_KEYS = [
    "id",
    "is_deleted",
    "feed_type",
    "feed_variant",
    "feed_ref_data",
    "feed_author_name",
    "feed_site_id",
    "feed_site_name",
    "feed_content_id",
    "feed_content_title",
    "feed_content_is_deleted",
    "createddate",
    "lastmodifieddate",
    "createdbyid",
    "modifiedbyid",
]

RECOGNITION_KEYS = [
    "id",
    "is_deleted",
    "feed_type",
    "feed_variant",
    "feed_author_name",
    "createddate",
    "lastmodifieddate",
    "createdbyid",
    "modifiedbyid",
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


def normalize_mentions(mentions):
    """
    Normalize feed_list_of_mentions to:
        [{"id": "...", "type": "people|site|content", "name": "..."}, ...]
    """
    if not isinstance(mentions, list):
        return []

    result = []
    seen = set()

    for item in mentions:
        if not isinstance(item, dict):
            continue

        mention_id = clean_string(item.get("id"))
        mention_type = clean_string(item.get("type"))
        mention_name = clean_string(item.get("name"))

        if mention_type:
            mention_type = mention_type.lower()

        if not mention_id or not mention_type:
            continue

        key = (mention_id, mention_type)
        if key in seen:
            continue
        seen.add(key)

        result.append(
            {
                "id": mention_id,
                "type": mention_type,
                "name": mention_name,
            }
        )

    return result


def normalize_recipients(obj):
    """
    Extract recognition recipients from:
        feed_associated_entity.recognitionAwardedToIds[*].userId
    Returns list of dicts: [{"user_id": "...", "name": "..."}, ...]
    """
    associated = obj.get("feed_associated_entity")
    if not isinstance(associated, dict):
        return []

    recipients = associated.get("recognitionAwardedToIds")
    if not isinstance(recipients, list):
        return []

    result = []
    seen = set()

    for item in recipients:
        if not isinstance(item, dict):
            continue

        user_id = clean_string(item.get("userId"))
        name = clean_string(item.get("name"))
        if not user_id:
            continue

        if user_id in seen:
            continue
        seen.add(user_id)

        result.append({"user_id": user_id, "name": name})

    return result


def keep_record(obj):
    """Apply scope filters for this phase."""
    if obj.get("object_type") != "feed":
        return False

    feed_type = obj.get("feed_type")
    feed_variant = obj.get("feed_variant")

    if feed_type not in ALLOWED_FEED_TYPES:
        return False

    if feed_type == "post" and feed_variant not in ALLOWED_POST_VARIANTS:
        return False

    return True


def normalize_post(obj):
    """Normalize a post feed row into Post record schema."""
    normalized = {}
    for key in POST_KEYS:
        value = obj.get(key)
        if isinstance(value, str):
            value = clean_string(value)
        normalized[key] = value

    normalized["mentions"] = normalize_mentions(obj.get("feed_list_of_mentions"))
    return normalized


def normalize_recognition(obj):
    """Normalize a recognition feed row into Recognition record schema."""
    normalized = {}
    for key in RECOGNITION_KEYS:
        value = obj.get(key)
        if isinstance(value, str):
            value = clean_string(value)
        normalized[key] = value

    normalized["recipients"] = normalize_recipients(obj)

    return normalized


# =====================================================
# Core extraction
# =====================================================

def extract_feeds(raw_file):
    """
    Read *raw_file* (JSONL) and return an extraction result dict:

        {
            "posts": [normalised post records],
            "recognitions": [normalised recognition records],
            "stats": {
                ...
            }
        }
    """
    raw_file = Path(raw_file)

    posts = []
    recognitions = []

    stats = {
        "raw_feed_rows": 0,
        "kept_rows": 0,
        "excluded_timeline": 0,
        "excluded_post_non_standard": 0,
        "excluded_other": 0,
        "posts": 0,
        "recognitions": 0,
    }

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            if obj.get("object_type") != "feed":
                continue

            stats["raw_feed_rows"] += 1

            feed_type = obj.get("feed_type")
            feed_variant = obj.get("feed_variant")

            if not keep_record(obj):
                if feed_type == "timeline":
                    stats["excluded_timeline"] += 1
                elif feed_type == "post" and feed_variant not in ALLOWED_POST_VARIANTS:
                    stats["excluded_post_non_standard"] += 1
                else:
                    stats["excluded_other"] += 1
                continue

            if not obj.get("id"):
                continue

            stats["kept_rows"] += 1

            if feed_type == "post":
                posts.append(normalize_post(obj))
                stats["posts"] += 1
            elif feed_type == "recognition":
                recognitions.append(normalize_recognition(obj))
                stats["recognitions"] += 1

    return {
        "posts": posts,
        "recognitions": recognitions,
        "stats": stats,
    }


# =====================================================
# Persist normalised artefacts
# =====================================================

def write_normalized(result, output_dir):
    """
    Write extraction artefacts to JSONL files under *output_dir*:
      - posts.jsonl
      - recognitions.jsonl
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    posts = result["posts"]
    recognitions = result["recognitions"]

    with open(output_dir / "posts.jsonl", "w", encoding="utf-8") as f:
        for record in posts:
            f.write(json.dumps(record) + "\n")

    with open(output_dir / "recognitions.jsonl", "w", encoding="utf-8") as f:
        for record in recognitions:
            f.write(json.dumps(record) + "\n")

    print(f"Written {len(posts)} post records → {output_dir / 'posts.jsonl'}")
    print(f"Written {len(recognitions)} recognition records → {output_dir / 'recognitions.jsonl'}")

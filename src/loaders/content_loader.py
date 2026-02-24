"""
Content loader — pure functions for ingesting normalised content data
into Neo4j.

Graph model
-----------
Nodes:
    (:Content)          — id, content_title, content_type, content_sub_type,
                          content_status, content_is_featured, content_is_must_read,
                          is_deleted, _aggregated_locations, createddate,
                          lastmodifieddate, content_first_published_on,
                          content_publish_on_date, content_validated_on
    (:SitePageCategory) — id, name, site_id

Relationships:
    (Content)-[:BELONGS_TO_SITE]->(Site)
    (People)-[:CREATED]->(Content)
    (People)-[:LAST_MODIFIED]->(Content)
    (SitePageCategory)-[:HAS_CONTENT]->(Content)
    (SitePageCategory)-[:BELONGS_TO_SITE]->(Site)
    (Content)-[:HAS_FILE]->(File)                # union of both file signals

File linkage (union of both signals)
------------------------------------
Signal 1: ``content_file_ids`` on each content record
    — extracted from raw ``content_files[*].file_id``
Signal 2: ``file_content_mappings`` in ``files.jsonl``
    — inverted to content_id → {file_ids}
Both use MERGE so duplicate edges are harmless.

Assumes:
    - `:People` nodes are already loaded (pipeline 01).
    - `:Site`   nodes are already loaded (pipeline 02).
    - `:File`   nodes are already loaded (pipeline 03).

Batch mode
----------
All loader functions accept ``batch=True`` to use UNWIND-based bulk
ingestion instead of one-by-one queries.

Usage:
    from src.loaders.content_loader import load_content_graph
    load_content_graph(client, normalized_dir)                # one-by-one
    load_content_graph(client, normalized_dir, batch=True)    # batch mode
"""

import json
from collections import defaultdict
from pathlib import Path

from src.utils.neo4j_client import Neo4jClient


# =====================================================
# Utility
# =====================================================

def load_jsonl(path):
    """Yield records from a JSONL file."""
    with open(path, "r") as f:
        for line in f:
            yield json.loads(line)


def load_jsonl_list(path):
    """Load all records from a JSONL file into a list."""
    return list(load_jsonl(path))


def build_file_to_content_map(normalized_dir):
    """
    Read files.jsonl and invert ``file_content_mappings`` into
    content_id → set(file_id).

    Returns a dict[str, set[str]].
    """
    path = Path(normalized_dir) / "files.jsonl"
    if not path.exists():
        return {}

    content_to_files = defaultdict(set)
    for record in load_jsonl(path):
        file_id = record.get("id")
        for content_id in record.get("file_content_mappings", []):
            if file_id and content_id:
                content_to_files[content_id].add(file_id)

    return dict(content_to_files)


# =====================================================
# Constraints & Indexes
# =====================================================

def create_constraints(client: Neo4jClient):
    """Ensure uniqueness constraints exist before loading."""
    constraints = [
        """
        CREATE CONSTRAINT content_id IF NOT EXISTS
        FOR (c:Content)
        REQUIRE c.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT site_page_category_id IF NOT EXISTS
        FOR (spc:SitePageCategory)
        REQUIRE spc.id IS UNIQUE
        """,
    ]

    for c in constraints:
        client.execute(c)

    print("✅ Constraints ensured.")


# =====================================================
# Node loaders
# =====================================================

def load_page_categories(client: Neo4jClient, normalized_dir, batch=False):
    """Merge SitePageCategory nodes."""
    path = Path(normalized_dir) / "content_page_categories.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (spc:SitePageCategory {id: row.id})
            SET spc.name    = row.name,
                spc.site_id = row.site_id
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (spc:SitePageCategory {id: $id})
                SET spc.name    = $name,
                    spc.site_id = $site_id
                """,
                {"id": record["id"], "name": record["name"], "site_id": record["site_id"]},
            )

    print(f"✅ SitePageCategory nodes loaded: {len(records)}")
    return len(records)


def load_contents(client: Neo4jClient, normalized_dir, batch=False):
    """Merge Content nodes with their properties."""
    path = Path(normalized_dir) / "contents.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (c:Content {id: row.id})
            SET c.content_title              = row.content_title,
                c.content_type               = row.content_type,
                c.content_sub_type           = row.content_sub_type,
                c.content_status             = row.content_status,
                c.content_is_featured        = row.content_is_featured,
                c.content_is_must_read       = row.content_is_must_read,
                c.is_deleted                 = row.is_deleted,
                c._aggregated_locations      = row._aggregated_locations,
                c.createddate                = row.createddate,
                c.lastmodifieddate           = row.lastmodifieddate,
                c.content_first_published_on = row.content_first_published_on,
                c.content_publish_on_date    = row.content_publish_on_date,
                c.content_validated_on       = row.content_validated_on
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (c:Content {id: $id})
                SET c.content_title              = $content_title,
                    c.content_type               = $content_type,
                    c.content_sub_type           = $content_sub_type,
                    c.content_status             = $content_status,
                    c.content_is_featured        = $content_is_featured,
                    c.content_is_must_read       = $content_is_must_read,
                    c.is_deleted                 = $is_deleted,
                    c._aggregated_locations      = $_aggregated_locations,
                    c.createddate                = $createddate,
                    c.lastmodifieddate           = $lastmodifieddate,
                    c.content_first_published_on = $content_first_published_on,
                    c.content_publish_on_date    = $content_publish_on_date,
                    c.content_validated_on       = $content_validated_on
                """,
                {
                    "id":                         record["id"],
                    "content_title":              record.get("content_title"),
                    "content_type":               record.get("content_type"),
                    "content_sub_type":           record.get("content_sub_type"),
                    "content_status":             record.get("content_status"),
                    "content_is_featured":        record.get("content_is_featured"),
                    "content_is_must_read":       record.get("content_is_must_read"),
                    "is_deleted":                 record.get("is_deleted"),
                    "_aggregated_locations":      record.get("_aggregated_locations"),
                    "createddate":                record.get("createddate"),
                    "lastmodifieddate":           record.get("lastmodifieddate"),
                    "content_first_published_on": record.get("content_first_published_on"),
                    "content_publish_on_date":    record.get("content_publish_on_date"),
                    "content_validated_on":       record.get("content_validated_on"),
                },
            )

    print(f"✅ Content nodes loaded: {len(records)}")
    return len(records)


# =====================================================
# Relationship loaders
# =====================================================

def link_contents_to_sites(client: Neo4jClient, normalized_dir, batch=False):
    """Create (Content)-[:BELONGS_TO_SITE]->(Site) relationships."""
    path = Path(normalized_dir) / "contents.jsonl"
    records = [r for r in load_jsonl(path) if r.get("content_site_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (c:Content {id: row.id})
            MATCH (s:Site {id: row.content_site_id})
            MERGE (c)-[:BELONGS_TO_SITE]->(s)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (c:Content {id: $content_id})
                MATCH (s:Site {id: $site_id})
                MERGE (c)-[:BELONGS_TO_SITE]->(s)
                """,
                {"content_id": record["id"], "site_id": record["content_site_id"]},
            )

    print(f"✅ BELONGS_TO_SITE relationships: {len(records)}")
    return len(records)


def link_creators(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:CREATED]->(Content) relationships."""
    path = Path(normalized_dir) / "contents.jsonl"
    records = [r for r in load_jsonl(path) if r.get("createdbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.createdbyid})
            MATCH (c:Content {id: row.id})
            MERGE (p)-[:CREATED]->(c)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $person_id})
                MATCH (c:Content {id: $content_id})
                MERGE (p)-[:CREATED]->(c)
                """,
                {"person_id": record["createdbyid"], "content_id": record["id"]},
            )

    print(f"✅ CREATED relationships: {len(records)}")
    return len(records)


def link_modifiers(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:LAST_MODIFIED]->(Content) relationships."""
    path = Path(normalized_dir) / "contents.jsonl"
    records = [r for r in load_jsonl(path) if r.get("lastmodifiedbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.lastmodifiedbyid})
            MATCH (c:Content {id: row.id})
            MERGE (p)-[:LAST_MODIFIED]->(c)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $person_id})
                MATCH (c:Content {id: $content_id})
                MERGE (p)-[:LAST_MODIFIED]->(c)
                """,
                {"person_id": record["lastmodifiedbyid"], "content_id": record["id"]},
            )

    print(f"✅ LAST_MODIFIED relationships: {len(records)}")
    return len(records)


def link_categories(client: Neo4jClient, normalized_dir, batch=False):
    """
    Create (SitePageCategory)-[:HAS_CONTENT]->(Content) relationships.

    Skips content records with null page_category_id.
    """
    path = Path(normalized_dir) / "contents.jsonl"
    records = [r for r in load_jsonl(path) if r.get("content_page_category_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (spc:SitePageCategory {id: row.content_page_category_id})
            MATCH (c:Content {id: row.id})
            MERGE (spc)-[:HAS_CONTENT]->(c)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (spc:SitePageCategory {id: $cat_id})
                MATCH (c:Content {id: $content_id})
                MERGE (spc)-[:HAS_CONTENT]->(c)
                """,
                {"content_id": record["id"], "cat_id": record["content_page_category_id"]},
            )

    print(f"✅ HAS_CONTENT relationships: {len(records)}")
    return len(records)


def link_categories_to_sites(client: Neo4jClient, normalized_dir, batch=False):
    """Create (SitePageCategory)-[:BELONGS_TO_SITE]->(Site) relationships."""
    path = Path(normalized_dir) / "content_page_categories.jsonl"
    records = [r for r in load_jsonl(path) if r.get("site_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (spc:SitePageCategory {id: row.id})
            MATCH (s:Site {id: row.site_id})
            MERGE (spc)-[:BELONGS_TO_SITE]->(s)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (spc:SitePageCategory {id: $cat_id})
                MATCH (s:Site {id: $site_id})
                MERGE (spc)-[:BELONGS_TO_SITE]->(s)
                """,
                {"cat_id": record["id"], "site_id": record["site_id"]},
            )

    print(f"✅ SitePageCategory BELONGS_TO_SITE relationships: {len(records)}")
    return len(records)


def link_content_files(client: Neo4jClient, normalized_dir, batch=False):
    """
    Create (Content)-[:HAS_FILE]->(File) relationships from content_file_ids.

    Signal 1: content_file_ids on each content record (from raw content_files[*].file_id).
    Only creates edges where the target :File node exists in Neo4j
    (most content_file_ids point to images excluded in the file pipeline).
    """
    path = Path(normalized_dir) / "contents.jsonl"

    # Flatten to (content_id, file_id) pairs
    edges = []
    for record in load_jsonl(path):
        content_id = record.get("id")
        for file_id in record.get("content_file_ids", []):
            edges.append({"content_id": content_id, "file_id": file_id})

    if not edges:
        print("✅ HAS_FILE relationships (from content_files): 0")
        return 0

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (c:Content {id: row.content_id})
            MATCH (f:File {id: row.file_id})
            MERGE (c)-[:HAS_FILE]->(f)
            """,
            edges,
        )
    else:
        for edge in edges:
            client.execute(
                """
                MATCH (c:Content {id: $content_id})
                MATCH (f:File {id: $file_id})
                MERGE (c)-[:HAS_FILE]->(f)
                """,
                edge,
            )

    print(f"✅ HAS_FILE relationships (from content_files): {len(edges)} pairs attempted")
    return len(edges)


def link_file_content_mappings(client: Neo4jClient, normalized_dir, batch=False):
    """
    Create (Content)-[:HAS_FILE]->(File) relationships from file_content_mappings.

    Signal 2: inverts file_content_mappings in files.jsonl to get
    content_id → {file_ids}.  Only creates edges where both :Content and
    :File nodes exist in Neo4j.  Uses MERGE so duplicates with Signal 1
    are harmless.
    """
    content_to_files = build_file_to_content_map(normalized_dir)

    # Flatten to (content_id, file_id) pairs
    edges = []
    for content_id, file_ids in content_to_files.items():
        for file_id in file_ids:
            edges.append({"content_id": content_id, "file_id": file_id})

    if not edges:
        print("✅ HAS_FILE relationships (from file_content_mappings): 0")
        return 0

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (c:Content {id: row.content_id})
            MATCH (f:File {id: row.file_id})
            MERGE (c)-[:HAS_FILE]->(f)
            """,
            edges,
        )
    else:
        for edge in edges:
            client.execute(
                """
                MATCH (c:Content {id: $content_id})
                MATCH (f:File {id: $file_id})
                MERGE (c)-[:HAS_FILE]->(f)
                """,
                edge,
            )

    print(f"✅ HAS_FILE relationships (from file_content_mappings): {len(edges)} pairs attempted")
    return len(edges)


# =====================================================
# Convenience wrapper
# =====================================================

def load_content_graph(client: Neo4jClient, normalized_dir, batch=False):
    """
    One-call loader: constraints → nodes → relationships.

    Assumes People (01), Site (02) and File (03) nodes are already loaded.
    Set ``batch=True`` to use UNWIND-based bulk ingestion (much faster).
    """
    normalized_dir = Path(normalized_dir)

    # 1. Constraints
    create_constraints(client)

    # 2. Nodes
    load_page_categories(client, normalized_dir, batch=batch)
    load_contents(client, normalized_dir, batch=batch)

    # 3. Relationships
    link_contents_to_sites(client, normalized_dir, batch=batch)
    link_creators(client, normalized_dir, batch=batch)
    link_modifiers(client, normalized_dir, batch=batch)
    link_categories(client, normalized_dir, batch=batch)
    link_categories_to_sites(client, normalized_dir, batch=batch)
    link_content_files(client, normalized_dir, batch=batch)
    link_file_content_mappings(client, normalized_dir, batch=batch)

    print("\n✅ Content graph fully loaded.")

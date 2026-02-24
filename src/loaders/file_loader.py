"""
File loader — pure functions for ingesting normalised file data
into Neo4j.

Graph model
-----------
Nodes:
    (:File)   — id, file_name, file_type, file_mime_type, file_provider,
                is_active, is_deleted, file_url,
                _aggregated_locations, createddate, lastmodifieddate

Deferred (not ingested in this pipeline):
    file_content_mappings (list[Content.id])

Relationships:
    (People)-[:CREATED]->(File)          via createdbyid
    (File)-[:BELONGS_TO_SITE]->(Site)    via file_site_mapping (one per site id)

Assumes:
    - `:People` nodes are already loaded (pipeline 01).
    - `:Site`   nodes are already loaded (pipeline 02).

Missing-site handling
---------------------
Before batching BELONGS_TO_SITE edges the loader resolves which site IDs
actually exist in Neo4j.  Any file_site_mapping entry that references a
non-existent site is logged and silently skipped — no error is raised.

Batch mode
----------
All loader functions accept ``batch=True`` to use UNWIND-based bulk
ingestion instead of one-by-one queries.

Usage:
    from src.loaders.file_loader import load_file_graph
    load_file_graph(client, normalized_dir)                # one-by-one
    load_file_graph(client, normalized_dir, batch=True)    # batch mode
"""

import json
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


# =====================================================
# Constraints & Indexes
# =====================================================

def create_constraints(client: Neo4jClient):
    """Ensure uniqueness constraints exist before loading."""
    constraints = [
        """
        CREATE CONSTRAINT file_id IF NOT EXISTS
        FOR (f:File)
        REQUIRE f.id IS UNIQUE
        """,
    ]

    for c in constraints:
        client.execute(c)

    print("✅ Constraints ensured.")


# =====================================================
# Node loaders
# =====================================================

def load_files(client: Neo4jClient, normalized_dir, batch=False):
    """Merge File nodes with their properties (excluding file_content_mappings)."""
    path = Path(normalized_dir) / "files.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (f:File {id: row.id})
            SET f.file_name              = row.file_name,
                f.file_type              = row.file_type,
                f.file_mime_type         = row.file_mime_type,
                f.file_provider          = row.file_provider,
                f.is_active              = row.is_active,
                f.is_deleted             = row.is_deleted,
                f.file_url               = row.file_url,
                f._aggregated_locations  = row._aggregated_locations,
                f.createddate            = row.createddate,
                f.lastmodifieddate       = row.lastmodifieddate
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (f:File {id: $id})
                SET f.file_name              = $file_name,
                    f.file_type              = $file_type,
                    f.file_mime_type         = $file_mime_type,
                    f.file_provider          = $file_provider,
                    f.is_active              = $is_active,
                    f.is_deleted             = $is_deleted,
                    f.file_url               = $file_url,
                    f._aggregated_locations  = $_aggregated_locations,
                    f.createddate            = $createddate,
                    f.lastmodifieddate       = $lastmodifieddate
                """,
                {
                    "id":                    record["id"],
                    "file_name":             record.get("file_name"),
                    "file_type":             record.get("file_type"),
                    "file_mime_type":        record.get("file_mime_type"),
                    "file_provider":         record.get("file_provider"),
                    "is_active":             record.get("is_active"),
                    "is_deleted":            record.get("is_deleted"),
                    "file_url":              record.get("file_url"),
                    "_aggregated_locations": record.get("_aggregated_locations"),
                    "createddate":           record.get("createddate"),
                    "lastmodifieddate":      record.get("lastmodifieddate"),
                },
            )

    print(f"✅ Files loaded: {len(records)}")
    print("ℹ️  file_content_mappings is preserved in files.jsonl and deferred to content ingestion.")
    return len(records)


# =====================================================
# Relationship loaders
# =====================================================

def link_creators(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:CREATED]->(File) relationships."""
    path = Path(normalized_dir) / "files.jsonl"
    records = [r for r in load_jsonl(path) if r.get("createdbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.createdbyid})
            MATCH (f:File {id: row.id})
            MERGE (p)-[:CREATED]->(f)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $person_id})
                MATCH (f:File {id: $file_id})
                MERGE (p)-[:CREATED]->(f)
                """,
                {"person_id": record["createdbyid"], "file_id": record["id"]},
            )

    print(f"✅ CREATED relationships: {len(records)}")
    return len(records)


def link_files_to_sites(client: Neo4jClient, normalized_dir, batch=False):
    """
    Create (File)-[:BELONGS_TO_SITE]->(Site) relationships.

    Uses ``file_site_mapping`` (list of site IDs).  Before ingestion the
    loader resolves which site IDs actually exist in Neo4j; any unknown
    site IDs are logged as warnings and silently skipped.
    """
    path = Path(normalized_dir) / "files.jsonl"
    records = load_jsonl_list(path)

    # ------------------------------------------------------------------
    # 1. Resolve which Site IDs actually exist in Neo4j
    # ------------------------------------------------------------------
    existing_sites_result = client.query("MATCH (s:Site) RETURN s.id AS id")
    existing_site_ids = {row["id"] for row in existing_sites_result}

    # ------------------------------------------------------------------
    # 2. Expand file_site_mapping into (file_id, site_id) pairs,
    #    flagging any missing site IDs
    # ------------------------------------------------------------------
    pairs = []          # valid pairs ready for ingestion
    missing_edges = []  # (file_id, site_id) where site is not in Neo4j

    for record in records:
        file_id = record["id"]
        for site_id in (record.get("file_site_mapping") or []):
            if site_id in existing_site_ids:
                pairs.append({"file_id": file_id, "site_id": site_id})
            else:
                missing_edges.append((file_id, site_id))

    # ------------------------------------------------------------------
    # 3. Report missing site edges
    # ------------------------------------------------------------------
    if missing_edges:
        print(f"⚠️  {len(missing_edges)} site reference(s) could not be resolved "
              f"(site not found in Neo4j — skipping, no error raised):")
        for file_id, site_id in missing_edges[:20]:   # cap output to 20 lines
            print(f"   file_id={file_id}  →  site_id={site_id}  [NOT FOUND]")
        if len(missing_edges) > 20:
            print(f"   ... and {len(missing_edges) - 20} more")
    else:
        print("✅ All file_site_mapping site IDs resolved.")

    if not pairs:
        print("ℹ️  No BELONGS_TO_SITE relationships to create.")
        return 0

    # ------------------------------------------------------------------
    # 4. Ingest valid pairs
    # ------------------------------------------------------------------
    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (f:File {id: row.file_id})
            MATCH (s:Site {id: row.site_id})
            MERGE (f)-[:BELONGS_TO_SITE]->(s)
            """,
            pairs,
        )
    else:
        for pair in pairs:
            client.execute(
                """
                MATCH (f:File {id: $file_id})
                MATCH (s:Site {id: $site_id})
                MERGE (f)-[:BELONGS_TO_SITE]->(s)
                """,
                pair,
            )

    print(f"✅ BELONGS_TO_SITE relationships: {len(pairs)}")
    return len(pairs)


# =====================================================
# Convenience wrapper
# =====================================================

def load_file_graph(client: Neo4jClient, normalized_dir, batch=False):
    """
    One-call loader: constraints → nodes → relationships.
    Assumes People (pipeline 01) and Site (pipeline 02) are already loaded.

    Set ``batch=True`` to use UNWIND-based bulk ingestion (much faster).
    """
    normalized_dir = Path(normalized_dir)

    # 1. Constraints
    create_constraints(client)

    # 2. Nodes
    load_files(client, normalized_dir, batch=batch)

    # 3. Relationships
    link_creators(client, normalized_dir, batch=batch)
    link_files_to_sites(client, normalized_dir, batch=batch)

    print("\n✅ File graph fully loaded.")

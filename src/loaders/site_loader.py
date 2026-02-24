"""
Site loader — pure functions for ingesting normalised site data
into Neo4j.

Graph model
-----------
Nodes:
    (:Site)          — id, site_name, site_type, site_is_featured,
                       site_description, is_active, site_member_count,
                       site_owner_manager_count, site_follower_count,
                       createddate, lastmodifieddate
    (:SiteCategory)  — id, name

Relationships:
    (Site)-[:BELONGS_TO_CATEGORY]->(SiteCategory)
    (People)-[:CREATED]->(Site)
    (People)-[:LAST_MODIFIED]->(Site)

Assumes:
    - `:People` nodes are already loaded (pipeline 01).
    - `createdbyid` / `lastmodifiedbyid` reference existing People.id values.

Batch mode
----------
All loader functions accept ``batch=True`` to use UNWIND-based bulk
ingestion instead of one-by-one queries.

Usage:
    from src.loaders.site_loader import load_site_graph
    load_site_graph(client, normalized_dir)                # one-by-one
    load_site_graph(client, normalized_dir, batch=True)    # batch mode
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
        CREATE CONSTRAINT site_id IF NOT EXISTS
        FOR (s:Site)
        REQUIRE s.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT site_category_id IF NOT EXISTS
        FOR (sc:SiteCategory)
        REQUIRE sc.id IS UNIQUE
        """,
    ]

    for c in constraints:
        client.execute(c)

    print("✅ Constraints ensured.")


# =====================================================
# Node loaders
# =====================================================

def load_site_categories(client: Neo4jClient, normalized_dir, batch=False):
    """Merge SiteCategory nodes."""
    path = Path(normalized_dir) / "site_categories.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (sc:SiteCategory {id: row.id})
            SET sc.name = row.name
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (sc:SiteCategory {id: $id})
                SET sc.name = $name
                """,
                {"id": record["id"], "name": record["name"]},
            )

    print(f"✅ Site categories loaded: {len(records)}")
    return len(records)


def load_sites(client: Neo4jClient, normalized_dir, batch=False):
    """Merge Site nodes with their properties."""
    path = Path(normalized_dir) / "sites.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (s:Site {id: row.id})
            SET s.site_name               = row.site_name,
                s.site_type               = row.site_type,
                s.site_is_featured        = row.site_is_featured,
                s.site_description        = row.site_description,
                s.is_active               = row.is_active,
                s.site_member_count       = row.site_member_count,
                s.site_owner_manager_count = row.site_owner_manager_count,
                s.site_follower_count     = row.site_follower_count,
                s.createddate             = row.createddate,
                s.lastmodifieddate        = row.lastmodifieddate
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (s:Site {id: $id})
                SET s.site_name               = $site_name,
                    s.site_type               = $site_type,
                    s.site_is_featured        = $site_is_featured,
                    s.site_description        = $site_description,
                    s.is_active               = $is_active,
                    s.site_member_count       = $site_member_count,
                    s.site_owner_manager_count = $site_owner_manager_count,
                    s.site_follower_count     = $site_follower_count,
                    s.createddate             = $createddate,
                    s.lastmodifieddate        = $lastmodifieddate
                """,
                {
                    "id":                       record["id"],
                    "site_name":                record.get("site_name"),
                    "site_type":                record.get("site_type"),
                    "site_is_featured":         record.get("site_is_featured"),
                    "site_description":         record.get("site_description"),
                    "is_active":                record.get("is_active"),
                    "site_member_count":        record.get("site_member_count"),
                    "site_owner_manager_count": record.get("site_owner_manager_count"),
                    "site_follower_count":      record.get("site_follower_count"),
                    "createddate":              record.get("createddate"),
                    "lastmodifieddate":         record.get("lastmodifieddate"),
                },
            )

    print(f"✅ Sites loaded: {len(records)}")
    return len(records)


# =====================================================
# Relationship loaders
# =====================================================

def link_sites_to_categories(client: Neo4jClient, normalized_dir, batch=False):
    """Create (Site)-[:BELONGS_TO_CATEGORY]->(SiteCategory) relationships."""
    path = Path(normalized_dir) / "sites.jsonl"
    records = [r for r in load_jsonl(path) if r.get("site_category_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (s:Site {id: row.id})
            MATCH (sc:SiteCategory {id: row.site_category_id})
            MERGE (s)-[:BELONGS_TO_CATEGORY]->(sc)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (s:Site {id: $site_id})
                MATCH (sc:SiteCategory {id: $cat_id})
                MERGE (s)-[:BELONGS_TO_CATEGORY]->(sc)
                """,
                {"site_id": record["id"], "cat_id": record["site_category_id"]},
            )

    print(f"✅ BELONGS_TO_CATEGORY relationships: {len(records)}")
    return len(records)


def link_creators(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:CREATED]->(Site) relationships."""
    path = Path(normalized_dir) / "sites.jsonl"
    records = [r for r in load_jsonl(path) if r.get("createdbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.createdbyid})
            MATCH (s:Site {id: row.id})
            MERGE (p)-[:CREATED]->(s)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $person_id})
                MATCH (s:Site {id: $site_id})
                MERGE (p)-[:CREATED]->(s)
                """,
                {"person_id": record["createdbyid"], "site_id": record["id"]},
            )

    print(f"✅ CREATED relationships: {len(records)}")
    return len(records)


def link_modifiers(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:LAST_MODIFIED]->(Site) relationships."""
    path = Path(normalized_dir) / "sites.jsonl"
    records = [r for r in load_jsonl(path) if r.get("lastmodifiedbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.lastmodifiedbyid})
            MATCH (s:Site {id: row.id})
            MERGE (p)-[:LAST_MODIFIED]->(s)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $person_id})
                MATCH (s:Site {id: $site_id})
                MERGE (p)-[:LAST_MODIFIED]->(s)
                """,
                {"person_id": record["lastmodifiedbyid"], "site_id": record["id"]},
            )

    print(f"✅ LAST_MODIFIED relationships: {len(records)}")
    return len(records)


# =====================================================
# Convenience wrapper
# =====================================================

def load_site_graph(client: Neo4jClient, normalized_dir, batch=False):
    """
    One-call loader: constraints → nodes → relationships.
    Assumes People nodes are already loaded (pipeline 01).

    Set ``batch=True`` to use UNWIND-based bulk ingestion (much faster).
    """
    normalized_dir = Path(normalized_dir)

    # 1. Constraints
    create_constraints(client)

    # 2. Nodes
    load_site_categories(client, normalized_dir, batch=batch)
    load_sites(client, normalized_dir, batch=batch)

    # 3. Relationships
    link_sites_to_categories(client, normalized_dir, batch=batch)
    link_creators(client, normalized_dir, batch=batch)
    link_modifiers(client, normalized_dir, batch=batch)

    print("\n✅ Site graph fully loaded.")

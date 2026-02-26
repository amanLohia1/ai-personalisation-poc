"""
Feed loader — pure functions for ingesting normalised feed data into Neo4j.

Graph model
-----------
Nodes:
    (:Post)          — standard post feeds (scoped)
    (:Recognition)   — recognition feeds

Relationships:
    (People)-[:CREATED]->(Post|Recognition)
    (People)-[:LAST_MODIFIED]->(Post|Recognition)
    (Post)-[:BELONGS_TO_SITE]->(Site)
    (Post)-[:BELONGS_TO_CONTENT]->(Content)
    (Post)-[:MENTIONS]->(People|Site|Content)
    (Recognition)-[:RECOGNIZES]->(People)

Assumes:
    - :People, :Site, :Content are already loaded by earlier pipelines.
    - Normalized files are present:
        posts.jsonl
        recognitions.jsonl

Batch mode
----------
All loader functions accept ``batch=True`` to use UNWIND-based bulk ingestion.
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
# Constraints
# =====================================================

def create_constraints(client: Neo4jClient):
    """Ensure uniqueness constraints exist before loading."""
    constraints = [
        """
        CREATE CONSTRAINT post_id IF NOT EXISTS
        FOR (p:Post)
        REQUIRE p.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT recognition_id IF NOT EXISTS
        FOR (r:Recognition)
        REQUIRE r.id IS UNIQUE
        """,
    ]

    for c in constraints:
        client.execute(c)

    print("✅ Constraints ensured.")


# =====================================================
# Node loaders
# =====================================================

def load_posts(client: Neo4jClient, normalized_dir, batch=False):
    """Merge Post nodes with their properties."""
    path = Path(normalized_dir) / "posts.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (p:Post {id: row.id})
            SET p.feed_type               = row.feed_type,
                p.feed_variant            = row.feed_variant,
                p.is_deleted              = row.is_deleted,
                p.feed_ref_data           = row.feed_ref_data,
                p.feed_author_name        = row.feed_author_name,
                p.feed_site_name          = row.feed_site_name,
                p.feed_content_title      = row.feed_content_title,
                p.feed_content_is_deleted = row.feed_content_is_deleted,
                p.createddate             = row.createddate,
                p.lastmodifieddate        = row.lastmodifieddate
            """,
            records,
        )
    else:
        for row in records:
            client.execute(
                """
                MERGE (p:Post {id: $id})
                SET p.feed_type               = $feed_type,
                    p.feed_variant            = $feed_variant,
                    p.is_deleted              = $is_deleted,
                    p.feed_ref_data           = $feed_ref_data,
                    p.feed_author_name        = $feed_author_name,
                    p.feed_site_name          = $feed_site_name,
                    p.feed_content_title      = $feed_content_title,
                    p.feed_content_is_deleted = $feed_content_is_deleted,
                    p.createddate             = $createddate,
                    p.lastmodifieddate        = $lastmodifieddate
                """,
                {
                    "id": row["id"],
                    "feed_type": row.get("feed_type"),
                    "feed_variant": row.get("feed_variant"),
                    "is_deleted": row.get("is_deleted"),
                    "feed_ref_data": row.get("feed_ref_data"),
                    "feed_author_name": row.get("feed_author_name"),
                    "feed_site_name": row.get("feed_site_name"),
                    "feed_content_title": row.get("feed_content_title"),
                    "feed_content_is_deleted": row.get("feed_content_is_deleted"),
                    "createddate": row.get("createddate"),
                    "lastmodifieddate": row.get("lastmodifieddate"),
                },
            )

    print(f"✅ Post nodes loaded: {len(records)}")
    return len(records)


def load_recognitions(client: Neo4jClient, normalized_dir, batch=False):
    """Merge Recognition nodes with their properties."""
    path = Path(normalized_dir) / "recognitions.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (r:Recognition {id: row.id})
            SET r.feed_type        = row.feed_type,
                r.feed_variant     = row.feed_variant,
                r.is_deleted       = row.is_deleted,
                r.feed_author_name = row.feed_author_name,
                r.createddate      = row.createddate,
                r.lastmodifieddate = row.lastmodifieddate
            """,
            records,
        )
    else:
        for row in records:
            client.execute(
                """
                MERGE (r:Recognition {id: $id})
                SET r.feed_type        = $feed_type,
                    r.feed_variant     = $feed_variant,
                    r.is_deleted       = $is_deleted,
                    r.feed_author_name = $feed_author_name,
                    r.createddate      = $createddate,
                    r.lastmodifieddate = $lastmodifieddate
                """,
                {
                    "id": row["id"],
                    "feed_type": row.get("feed_type"),
                    "feed_variant": row.get("feed_variant"),
                    "is_deleted": row.get("is_deleted"),
                    "feed_author_name": row.get("feed_author_name"),
                    "createddate": row.get("createddate"),
                    "lastmodifieddate": row.get("lastmodifieddate"),
                },
            )

    print(f"✅ Recognition nodes loaded: {len(records)}")
    return len(records)


# =====================================================
# Relationship loaders: creators / modifiers
# =====================================================

def link_post_creators(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "posts.jsonl"
    records = [r for r in load_jsonl(path) if r.get("createdbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (u:People {id: row.createdbyid})
            MATCH (p:Post {id: row.id})
            MERGE (u)-[:CREATED]->(p)
            """,
            records,
        )
    else:
        for r in records:
            client.execute(
                """
                MATCH (u:People {id: $user_id})
                MATCH (p:Post {id: $post_id})
                MERGE (u)-[:CREATED]->(p)
                """,
                {"user_id": r["createdbyid"], "post_id": r["id"]},
            )

    print(f"✅ CREATED (People→Post): {len(records)}")
    return len(records)


def link_post_modifiers(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "posts.jsonl"
    records = [r for r in load_jsonl(path) if r.get("modifiedbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (u:People {id: row.modifiedbyid})
            MATCH (p:Post {id: row.id})
            MERGE (u)-[:LAST_MODIFIED]->(p)
            """,
            records,
        )
    else:
        for r in records:
            client.execute(
                """
                MATCH (u:People {id: $user_id})
                MATCH (p:Post {id: $post_id})
                MERGE (u)-[:LAST_MODIFIED]->(p)
                """,
                {"user_id": r["modifiedbyid"], "post_id": r["id"]},
            )

    print(f"✅ LAST_MODIFIED (People→Post): {len(records)}")
    return len(records)


def link_recognition_creators(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "recognitions.jsonl"
    records = [r for r in load_jsonl(path) if r.get("createdbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (u:People {id: row.createdbyid})
            MATCH (r:Recognition {id: row.id})
            MERGE (u)-[:CREATED]->(r)
            """,
            records,
        )
    else:
        for r in records:
            client.execute(
                """
                MATCH (u:People {id: $user_id})
                MATCH (r:Recognition {id: $recognition_id})
                MERGE (u)-[:CREATED]->(r)
                """,
                {"user_id": r["createdbyid"], "recognition_id": r["id"]},
            )

    print(f"✅ CREATED (People→Recognition): {len(records)}")
    return len(records)


def link_recognition_modifiers(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "recognitions.jsonl"
    records = [r for r in load_jsonl(path) if r.get("modifiedbyid")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (u:People {id: row.modifiedbyid})
            MATCH (r:Recognition {id: row.id})
            MERGE (u)-[:LAST_MODIFIED]->(r)
            """,
            records,
        )
    else:
        for r in records:
            client.execute(
                """
                MATCH (u:People {id: $user_id})
                MATCH (r:Recognition {id: $recognition_id})
                MERGE (u)-[:LAST_MODIFIED]->(r)
                """,
                {"user_id": r["modifiedbyid"], "recognition_id": r["id"]},
            )

    print(f"✅ LAST_MODIFIED (People→Recognition): {len(records)}")
    return len(records)


# =====================================================
# Relationship loaders: post anchors
# =====================================================

def link_posts_to_sites(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "posts.jsonl"
    records = [r for r in load_jsonl(path) if r.get("feed_site_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:Post {id: row.id})
            MATCH (s:Site {id: row.feed_site_id})
            MERGE (p)-[:BELONGS_TO_SITE]->(s)
            """,
            records,
        )
    else:
        for r in records:
            client.execute(
                """
                MATCH (p:Post {id: $post_id})
                MATCH (s:Site {id: $site_id})
                MERGE (p)-[:BELONGS_TO_SITE]->(s)
                """,
                {"post_id": r["id"], "site_id": r["feed_site_id"]},
            )

    print(f"✅ BELONGS_TO_SITE (Post→Site): {len(records)}")
    return len(records)


def link_posts_to_content(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "posts.jsonl"
    records = [r for r in load_jsonl(path) if r.get("feed_content_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:Post {id: row.id})
            MATCH (c:Content {id: row.feed_content_id})
            MERGE (p)-[:BELONGS_TO_CONTENT]->(c)
            """,
            records,
        )
    else:
        for r in records:
            client.execute(
                """
                MATCH (p:Post {id: $post_id})
                MATCH (c:Content {id: $content_id})
                MERGE (p)-[:BELONGS_TO_CONTENT]->(c)
                """,
                {"post_id": r["id"], "content_id": r["feed_content_id"]},
            )

    print(f"✅ BELONGS_TO_CONTENT (Post→Content): {len(records)}")
    return len(records)


# =====================================================
# Relationship loaders: recognition recipients
# =====================================================

def link_recognition_recipients(client: Neo4jClient, normalized_dir, batch=False):
    """Create one-way: (Recognition)-[:RECOGNIZES]->(People)."""
    path = Path(normalized_dir) / "recognitions.jsonl"

    edges = []
    for record in load_jsonl(path):
        recognition_id = record.get("id")
        for recipient in record.get("recipients") or []:
            user_id = recipient.get("user_id") if isinstance(recipient, dict) else None
            if recognition_id and user_id:
                edges.append({"recognition_id": recognition_id, "user_id": user_id})

    if not edges:
        print("✅ RECOGNIZES edges: 0")
        return 0

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (r:Recognition {id: row.recognition_id})
            MATCH (u:People {id: row.user_id})
            MERGE (r)-[:RECOGNIZES]->(u)
            """,
            edges,
        )
    else:
        for e in edges:
            client.execute(
                """
                MATCH (r:Recognition {id: $recognition_id})
                MATCH (u:People {id: $user_id})
                MERGE (r)-[:RECOGNIZES]->(u)
                """,
                e,
            )

    print(f"✅ RECOGNIZES (Recognition→People): {len(edges)}")
    return len(edges)


# =====================================================
# Relationship loaders: mentions (posts only)
# =====================================================

def _extract_mention_edges(records, source_key):
    """
    Flatten mentions list into typed edge rows:
      {source_id, mention_id, mention_type}
    """
    rows = []
    for record in records:
        source_id = record.get(source_key)
        if not source_id:
            continue

        for mention in record.get("mentions") or []:
            if not isinstance(mention, dict):
                continue
            mention_id = mention.get("id")
            mention_type = (mention.get("type") or "").lower()
            if mention_id and mention_type in {"people", "site", "content"}:
                rows.append(
                    {
                        "source_id": source_id,
                        "mention_id": mention_id,
                        "mention_type": mention_type,
                    }
                )
    return rows


def _link_mentions_for_label(client, rows, label, batch=False):
    if not rows:
        return 0

    people_rows = [r for r in rows if r["mention_type"] == "people"]
    site_rows = [r for r in rows if r["mention_type"] == "site"]
    content_rows = [r for r in rows if r["mention_type"] == "content"]

    if batch:
        if people_rows:
            client.execute_batch(
                f"""
                UNWIND $batch AS row
                MATCH (n:{label} {{id: row.source_id}})
                MATCH (p:People {{id: row.mention_id}})
                MERGE (n)-[:MENTIONS]->(p)
                """,
                people_rows,
            )
        if site_rows:
            client.execute_batch(
                f"""
                UNWIND $batch AS row
                MATCH (n:{label} {{id: row.source_id}})
                MATCH (s:Site {{id: row.mention_id}})
                MERGE (n)-[:MENTIONS]->(s)
                """,
                site_rows,
            )
        if content_rows:
            client.execute_batch(
                f"""
                UNWIND $batch AS row
                MATCH (n:{label} {{id: row.source_id}})
                MATCH (c:Content {{id: row.mention_id}})
                MERGE (n)-[:MENTIONS]->(c)
                """,
                content_rows,
            )
    else:
        for row in people_rows:
            client.execute(
                f"""
                MATCH (n:{label} {{id: $source_id}})
                MATCH (p:People {{id: $mention_id}})
                MERGE (n)-[:MENTIONS]->(p)
                """,
                row,
            )
        for row in site_rows:
            client.execute(
                f"""
                MATCH (n:{label} {{id: $source_id}})
                MATCH (s:Site {{id: $mention_id}})
                MERGE (n)-[:MENTIONS]->(s)
                """,
                row,
            )
        for row in content_rows:
            client.execute(
                f"""
                MATCH (n:{label} {{id: $source_id}})
                MATCH (c:Content {{id: $mention_id}})
                MERGE (n)-[:MENTIONS]->(c)
                """,
                row,
            )

    return len(rows)


def link_post_mentions(client: Neo4jClient, normalized_dir, batch=False):
    path = Path(normalized_dir) / "posts.jsonl"
    rows = _extract_mention_edges(load_jsonl(path), source_key="id")
    n = _link_mentions_for_label(client, rows, label="Post", batch=batch)
    print(f"✅ MENTIONS (Post→People|Site|Content): {n}")
    return n


# =====================================================
# Convenience wrapper
# =====================================================

def load_feed_graph(client: Neo4jClient, normalized_dir, batch=False):
    """
    One-call loader: constraints → nodes → relationships.
    Assumes People, Site and Content nodes already exist.
    """
    normalized_dir = Path(normalized_dir)

    create_constraints(client)

    load_posts(client, normalized_dir, batch=batch)
    load_recognitions(client, normalized_dir, batch=batch)

    link_post_creators(client, normalized_dir, batch=batch)
    link_post_modifiers(client, normalized_dir, batch=batch)
    link_recognition_creators(client, normalized_dir, batch=batch)
    link_recognition_modifiers(client, normalized_dir, batch=batch)

    link_posts_to_sites(client, normalized_dir, batch=batch)
    link_posts_to_content(client, normalized_dir, batch=batch)

    link_recognition_recipients(client, normalized_dir, batch=batch)

    link_post_mentions(client, normalized_dir, batch=batch)

    print("\n✅ Feed graph fully loaded.")

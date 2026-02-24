"""
People loader — pure functions for ingesting normalised people data
into Neo4j.

Graph model
-----------
Nodes:
    (:People)        — id, user_name, user_work_title, user_about, is_active
    (:Country)       — name
    (:WorkDivision)  — name
    (:Department)    — name, division_name   (composite uniqueness)

Relationships:
    (People)-[:LOCATED_IN]->(Country)
    (People)-[:WORKS_IN_DEPARTMENT]->(Department)
    (People)-[:WORKS_IN_DIVISION]->(WorkDivision)
    (People)-[:MANAGES]->(People)          manager → direct report
    (WorkDivision)-[:HAS_DEPARTMENT]->(Department)

Batch mode
----------
All loader functions accept ``batch=True`` to use UNWIND-based bulk
ingestion instead of one-by-one queries.  The convenience wrapper
``load_people_graph`` forwards this flag to every step.

Usage (notebook or script):
    from src.loaders.people_loader import load_people_graph
    load_people_graph(client, normalized_dir)                # one-by-one
    load_people_graph(client, normalized_dir, batch=True)    # batch mode
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
        CREATE CONSTRAINT people_id IF NOT EXISTS
        FOR (p:People)
        REQUIRE p.id IS UNIQUE
        """,
        """
        CREATE CONSTRAINT country_name IF NOT EXISTS
        FOR (c:Country)
        REQUIRE c.name IS UNIQUE
        """,
        """
        CREATE CONSTRAINT division_name IF NOT EXISTS
        FOR (w:WorkDivision)
        REQUIRE w.name IS UNIQUE
        """,
        """
        CREATE CONSTRAINT department_identity IF NOT EXISTS
        FOR (d:Department)
        REQUIRE (d.name, d.division_name) IS UNIQUE
        """,
    ]

    for c in constraints:
        client.execute(c)

    print("✅ Constraints ensured.")


# =====================================================
# Node loaders
# =====================================================

def load_countries(client: Neo4jClient, normalized_dir, batch=False):
    """Merge Country nodes."""
    path = Path(normalized_dir) / "countries.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            "UNWIND $batch AS row MERGE (c:Country {name: row.name})",
            records,
        )
    else:
        for record in records:
            client.execute(
                "MERGE (c:Country {name: $name})",
                {"name": record["name"]},
            )

    print(f"✅ Countries loaded: {len(records)}")
    return len(records)


def load_work_divisions(client: Neo4jClient, normalized_dir, batch=False):
    """Merge WorkDivision nodes."""
    path = Path(normalized_dir) / "work_divisions.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            "UNWIND $batch AS row MERGE (w:WorkDivision {name: row.name})",
            records,
        )
    else:
        for record in records:
            client.execute(
                "MERGE (w:WorkDivision {name: $name})",
                {"name": record["name"]},
            )

    print(f"✅ Work divisions loaded: {len(records)}")
    return len(records)


def load_departments(client: Neo4jClient, normalized_dir, batch=False):
    """Merge Department nodes and create (WorkDivision)-[:HAS_DEPARTMENT]->(Department)."""
    path = Path(normalized_dir) / "departments.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (d:Department {name: row.name, division_name: row.division_name})
            WITH d, row
            MATCH (w:WorkDivision {name: row.division_name})
            MERGE (w)-[:HAS_DEPARTMENT]->(d)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (d:Department {name: $name, division_name: $division_name})
                WITH d
                MATCH (w:WorkDivision {name: $division_name})
                MERGE (w)-[:HAS_DEPARTMENT]->(d)
                """,
                {
                    "name": record["name"],
                    "division_name": record["division_name"],
                },
            )

    print(f"✅ Departments loaded: {len(records)}")
    return len(records)


def load_people(client: Neo4jClient, normalized_dir, batch=False):
    """Merge People nodes with their properties."""
    path = Path(normalized_dir) / "people.jsonl"
    records = load_jsonl_list(path)

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MERGE (p:People {id: row.id})
            SET p.user_name       = row.user_name,
                p.user_work_title = row.user_work_title,
                p.user_about      = row.user_about,
                p.is_active       = row.is_active
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MERGE (p:People {id: $id})
                SET p.user_name       = $user_name,
                    p.user_work_title = $user_work_title,
                    p.user_about      = $user_about,
                    p.is_active       = $is_active
                """,
                {
                    "id":              record["id"],
                    "user_name":       record.get("user_name"),
                    "user_work_title": record.get("user_work_title"),
                    "user_about":      record.get("user_about"),
                    "is_active":       record.get("is_active"),
                },
            )

    print(f"✅ People loaded: {len(records)}")
    return len(records)


# =====================================================
# Relationship loaders
# =====================================================

def link_people_to_countries(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:LOCATED_IN]->(Country) relationships."""
    path = Path(normalized_dir) / "people.jsonl"
    records = [r for r in load_jsonl(path) if r.get("user_normalized_country")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.id})
            MATCH (c:Country {name: row.user_normalized_country})
            MERGE (p)-[:LOCATED_IN]->(c)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $id})
                MATCH (c:Country {name: $country})
                MERGE (p)-[:LOCATED_IN]->(c)
                """,
                {"id": record["id"], "country": record["user_normalized_country"]},
            )

    print(f"✅ LOCATED_IN relationships: {len(records)}")
    return len(records)


def link_people_to_departments(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:WORKS_IN_DEPARTMENT]->(Department) relationships."""
    path = Path(normalized_dir) / "people.jsonl"
    records = [r for r in load_jsonl(path) if r.get("user_department")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.id})
            MATCH (d:Department {name: row.user_department, division_name: row.user_work_division})
            MERGE (p)-[:WORKS_IN_DEPARTMENT]->(d)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $id})
                MATCH (d:Department {name: $dept, division_name: $div})
                MERGE (p)-[:WORKS_IN_DEPARTMENT]->(d)
                """,
                {"id": record["id"], "dept": record["user_department"], "div": record["user_work_division"]},
            )

    print(f"✅ WORKS_IN_DEPARTMENT relationships: {len(records)}")
    return len(records)


def link_people_to_divisions(client: Neo4jClient, normalized_dir, batch=False):
    """Create (People)-[:WORKS_IN_DIVISION]->(WorkDivision) relationships."""
    path = Path(normalized_dir) / "people.jsonl"
    records = [r for r in load_jsonl(path) if r.get("user_work_division")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (p:People {id: row.id})
            MATCH (w:WorkDivision {name: row.user_work_division})
            MERGE (p)-[:WORKS_IN_DIVISION]->(w)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (p:People {id: $id})
                MATCH (w:WorkDivision {name: $div})
                MERGE (p)-[:WORKS_IN_DIVISION]->(w)
                """,
                {"id": record["id"], "div": record["user_work_division"]},
            )

    print(f"✅ WORKS_IN_DIVISION relationships: {len(records)}")
    return len(records)


def link_managers(client: Neo4jClient, normalized_dir, batch=False):
    """Create (Manager:People)-[:MANAGES]->(People) relationships."""
    path = Path(normalized_dir) / "people.jsonl"
    records = [r for r in load_jsonl(path) if r.get("user_manager_id")]

    if batch:
        client.execute_batch(
            """
            UNWIND $batch AS row
            MATCH (mgr:People {id: row.user_manager_id})
            MATCH (p:People {id: row.id})
            MERGE (mgr)-[:MANAGES]->(p)
            """,
            records,
        )
    else:
        for record in records:
            client.execute(
                """
                MATCH (mgr:People {id: $manager_id})
                MATCH (p:People {id: $person_id})
                MERGE (mgr)-[:MANAGES]->(p)
                """,
                {"manager_id": record["user_manager_id"], "person_id": record["id"]},
            )

    print(f"✅ MANAGES relationships: {len(records)}")
    return len(records)


# =====================================================
# Convenience wrapper
# =====================================================

def load_people_graph(client: Neo4jClient, normalized_dir, batch=False):
    """
    One-call loader: constraints → nodes → relationships.
    Assumes the graph is empty (or idempotent via MERGE).

    Set ``batch=True`` to use UNWIND-based bulk ingestion (much faster).
    """
    normalized_dir = Path(normalized_dir)

    # 1. Constraints
    create_constraints(client)

    # 2. Nodes
    load_countries(client, normalized_dir, batch=batch)
    load_work_divisions(client, normalized_dir, batch=batch)
    load_departments(client, normalized_dir, batch=batch)
    load_people(client, normalized_dir, batch=batch)

    # 3. Relationships
    link_people_to_countries(client, normalized_dir, batch=batch)
    link_people_to_departments(client, normalized_dir, batch=batch)
    link_people_to_divisions(client, normalized_dir, batch=batch)
    link_managers(client, normalized_dir, batch=batch)

    print("\n✅ People graph fully loaded.")

"""
Feed pipeline — headless script for feed extraction + Neo4j ingestion.

Scope enforced in this phase
----------------------------
- object_type == "feed"
- feed_type in {"post", "recognition"}
- post restricted to feed_variant == "standard"
- timeline + non-standard post variants excluded

Graph model loaded
------------------
- Nodes: Post, Recognition
- Relationships:
    CREATED, LAST_MODIFIED,
    BELONGS_TO_SITE, BELONGS_TO_CONTENT,
    MENTIONS,
    RECOGNIZES

Usage:
    cd graph_experiments
    python -m scripts.feed_pipeline                    # extract + ingest (single)
    python -m scripts.feed_pipeline --batch            # extract + ingest (batch)
    python -m scripts.feed_pipeline --extract          # extract only
    python -m scripts.feed_pipeline --ingest           # ingest only (single)
    python -m scripts.feed_pipeline --ingest --batch   # ingest only (batch)
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import RAW_FILE, NORMALIZED_DIR, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from src.extractors.feed_extractor import extract_feeds, write_normalized


def run_extraction():
    print("=" * 50)
    print("EXTRACTION — Feeds")
    print("=" * 50)

    result = extract_feeds(RAW_FILE)
    stats = result["stats"]

    print(f"Raw feed rows             : {stats['raw_feed_rows']}")
    print(f"Rows kept in scope        : {stats['kept_rows']}")
    print(f"Excluded timeline         : {stats['excluded_timeline']}")
    print(f"Excluded post non-standard: {stats['excluded_post_non_standard']}")
    print(f"Excluded other            : {stats['excluded_other']}")
    print(f"Posts extracted           : {stats['posts']}")
    print(f"Recognitions extracted    : {stats['recognitions']}")

    write_normalized(result, NORMALIZED_DIR)
    print(f"\nNormalised files written to: {NORMALIZED_DIR}")


def run_ingestion(batch=False):
    from src.loaders.feed_loader import load_feed_graph
    from src.utils.neo4j_client import Neo4jClient

    print("=" * 50)
    print(f"INGESTION — Feeds ({'batch' if batch else 'single'})")
    print("=" * 50)

    client = Neo4jClient(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
    try:
        load_feed_graph(client, NORMALIZED_DIR, batch=batch)
    finally:
        client.close()
        print("Neo4j connection closed.")


def main():
    parser = argparse.ArgumentParser(description="Feed pipeline")
    parser.add_argument("--extract", action="store_true", help="Run extraction only")
    parser.add_argument("--ingest", action="store_true", help="Run ingestion only")
    parser.add_argument("--batch", action="store_true", help="Use UNWIND batch ingestion")
    args = parser.parse_args()

    run_all = not args.extract and not args.ingest

    if args.extract or run_all:
        run_extraction()
        print()

    if args.ingest or run_all:
        run_ingestion(batch=args.batch)


if __name__ == "__main__":
    main()

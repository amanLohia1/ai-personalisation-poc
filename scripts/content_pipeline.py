"""
Content pipeline — headless script equivalent of the content notebook.

Runs extraction + normalisation + Neo4j ingestion without any analysis.

Graph model: Content, SitePageCategory
Relationships:
    BELONGS_TO_SITE  (Content→Site, SitePageCategory→Site)
    CREATED          (People→Content)
    LAST_MODIFIED    (People→Content)
    HAS_CONTENT      (SitePageCategory→Content)
    HAS_FILE         (Content→File)  — union of content_files + file_content_mappings

Assumes:
    - People graph (pipeline 01) is already loaded.
    - Site graph   (pipeline 02) is already loaded.
    - File graph   (pipeline 03) is already loaded.

Usage:
    cd graph_experiments
    python -m scripts.content_pipeline                    # extract + ingest (single)
    python -m scripts.content_pipeline --batch            # extract + ingest (batch)
    python -m scripts.content_pipeline --extract          # extract only
    python -m scripts.content_pipeline --ingest           # ingest only (single)
    python -m scripts.content_pipeline --ingest --batch   # ingest only (batch)
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import RAW_FILE, NORMALIZED_DIR, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from src.extractors.content_extractor import extract_contents, write_normalized
from src.loaders.content_loader import load_content_graph
from src.utils.neo4j_client import Neo4jClient


def run_extraction():
    print("=" * 50)
    print("EXTRACTION — Contents")
    print("=" * 50)

    result = extract_contents(RAW_FILE)

    print(f"Contents extracted         : {len(result['contents'])}")
    print(f"Page categories (unique)   : {len(result['page_categories'])}")
    print(f"Contents without site      : {result['num_no_site']}")
    print(f"Contents without category  : {result['num_no_category']}")
    print(f"Contents with file refs    : {result['num_with_files']}")

    write_normalized(result, NORMALIZED_DIR)
    print(f"\nNormalised files written to: {NORMALIZED_DIR}")


def run_ingestion(batch=False):
    print("=" * 50)
    print(f"INGESTION — Contents ({'batch' if batch else 'single'})")
    print("=" * 50)

    client = Neo4jClient(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

    try:
        load_content_graph(client, NORMALIZED_DIR, batch=batch)
    finally:
        client.close()
        print("Neo4j connection closed.")


def main():
    parser = argparse.ArgumentParser(description="Content pipeline")
    parser.add_argument("--extract", action="store_true", help="Run extraction only")
    parser.add_argument("--ingest",  action="store_true", help="Run ingestion only")
    parser.add_argument("--batch",   action="store_true", help="Use UNWIND batch ingestion (faster)")
    args = parser.parse_args()

    # If neither flag is set, run both
    run_all = not args.extract and not args.ingest

    if args.extract or run_all:
        run_extraction()
        print()

    if args.ingest or run_all:
        run_ingestion(batch=args.batch)


if __name__ == "__main__":
    main()

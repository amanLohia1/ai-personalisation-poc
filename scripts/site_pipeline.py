"""
Site pipeline — headless script equivalent of the site notebook.

Runs extraction + normalisation + Neo4j ingestion without any analysis.

Graph model: Site, SiteCategory
Relationships: BELONGS_TO_CATEGORY, CREATED, LAST_MODIFIED

Assumes: People graph (pipeline 01) is already loaded.

Usage:
    cd graph_experiments
    python -m scripts.site_pipeline                    # extract + ingest (single)
    python -m scripts.site_pipeline --batch            # extract + ingest (batch)
    python -m scripts.site_pipeline --extract          # extract only
    python -m scripts.site_pipeline --ingest           # ingest only (single)
    python -m scripts.site_pipeline --ingest --batch   # ingest only (batch)
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import RAW_FILE, NORMALIZED_DIR, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from src.extractors.site_extractor import extract_sites, write_normalized
from src.loaders.site_loader import load_site_graph
from src.utils.neo4j_client import Neo4jClient


def run_extraction():
    print("=" * 50)
    print("EXTRACTION — Sites")
    print("=" * 50)

    result = extract_sites(RAW_FILE)

    print(f"Sites:           {len(result['sites'])}")
    print(f"Site categories: {len(result['site_categories'])}")

    write_normalized(result, NORMALIZED_DIR)
    print(f"\nNormalised files written to: {NORMALIZED_DIR}")


def run_ingestion(batch=False):
    print("=" * 50)
    print(f"INGESTION — Sites ({'batch' if batch else 'single'})")
    print("=" * 50)

    client = Neo4jClient(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

    try:
        load_site_graph(client, NORMALIZED_DIR, batch=batch)
    finally:
        client.close()
        print("Neo4j connection closed.")


def main():
    parser = argparse.ArgumentParser(description="Site pipeline")
    parser.add_argument("--extract", action="store_true", help="Run extraction only")
    parser.add_argument("--ingest", action="store_true", help="Run ingestion only")
    parser.add_argument("--batch", action="store_true", help="Use UNWIND batch ingestion (faster)")
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

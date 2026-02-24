"""
File pipeline — headless script equivalent of the file notebook.

Runs extraction + normalisation + Neo4j ingestion without any analysis.

Graph model: File
Relationships: CREATED (People→File), BELONGS_TO_SITE (File→Site)

Assumes:
    - People graph (pipeline 01) is already loaded.
    - Site graph   (pipeline 02) is already loaded.

Usage:
    cd graph_experiments
    python -m scripts.file_pipeline                    # extract + ingest (single)
    python -m scripts.file_pipeline --batch            # extract + ingest (batch)
    python -m scripts.file_pipeline --extract          # extract only
    python -m scripts.file_pipeline --ingest           # ingest only (single)
    python -m scripts.file_pipeline --ingest --batch   # ingest only (batch)
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import RAW_FILE, NORMALIZED_DIR, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from src.extractors.file_extractor import extract_files, write_normalized
from src.loaders.file_loader import load_file_graph
from src.utils.neo4j_client import Neo4jClient


def run_extraction():
    print("=" * 50)
    print("EXTRACTION — Files")
    print("=" * 50)

    result = extract_files(RAW_FILE)

    print(f"Files extracted:  {len(result['files'])}")
    print(f"Images skipped:   {result['num_skipped_images']}")
    print(f"Orphan files:     {result['num_orphan_files']}  (no site mapping — ingested without site edges)")
    print(f"file_content_mappings non-empty: {result['num_nonempty_content_mappings']}")
    print(f"file_content_mappings empty:     {result['num_empty_content_mappings']}")

    write_normalized(result, NORMALIZED_DIR)
    print(f"\nNormalised files written to: {NORMALIZED_DIR}")


def run_ingestion(batch=False):
    print("=" * 50)
    print(f"INGESTION — Files ({'batch' if batch else 'single'})")
    print("=" * 50)

    client = Neo4jClient(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)

    try:
        load_file_graph(client, NORMALIZED_DIR, batch=batch)
    finally:
        client.close()
        print("Neo4j connection closed.")


def main():
    parser = argparse.ArgumentParser(description="File pipeline")
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

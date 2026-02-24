from pathlib import Path

# ==============================
# Base Paths
# ==============================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
NORMALIZED_DIR = DATA_DIR / "normalized"

RAW_FILE = RAW_DIR / "backyard_dump_19022026_172648.jsonl"

# ==============================
# Neo4j
# ==============================

NEO4J_URI = "bolt://172.31.22.63:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "graphrag@2026"

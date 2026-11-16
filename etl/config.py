"""Paths and connection settings. Override any of them with environment variables."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_DIR = PROJECT_ROOT / "contracts"
DATA_DIR = Path(os.environ.get("ETL_DATA_DIR", PROJECT_ROOT / "data"))
RUNS_DIR = Path(os.environ.get("ETL_RUNS_DIR", PROJECT_ROOT / "runs"))

# The official input, in this fixed order (Contract 4).
DEFAULT_INPUT_FILES = [
    DATA_DIR / "NASA_access_log_Jul95",
    DATA_DIR / "NASA_access_log_Aug95",
]

PG_DSN = os.environ.get(
    "ETL_PG_DSN",
    f"host=localhost port={os.environ.get('PG_PORT', '5433')} dbname=nasa_etl user=etl password=etl",
)
MONGO_URI = os.environ.get("ETL_MONGO_URI", f"mongodb://localhost:{os.environ.get('MONGO_PORT', '27017')}")
HIVE_JDBC = os.environ.get("ETL_HIVE_JDBC", "jdbc:hive2://localhost:10000/")

# docker-compose.yml mounts ./runs at /runs and ./data at /input inside the containers.
CONTAINER_RUNS_DIR = "/runs"
CONTAINER_DATA_DIR = "/input"


def to_container_path(host_path: Path) -> str:
    """Translate a path under ./runs on the laptop to the same path inside the Hive container."""
    rel = Path(host_path).resolve().relative_to(RUNS_DIR.resolve())
    return f"{CONTAINER_RUNS_DIR}/{rel.as_posix()}"

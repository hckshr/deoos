"""Put the Python examples on sys.path for the behavioral tests."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for name in ("devto-etl", "document-index", "payment", "hacker-news",
             "nightly-batch-resume", "agent-run-resume", "offline-approval-ledger",
             "multi-arch-handoff", "scheduled-etl", "warehouse-backfill",
             "partition-sensor", "dbt-duckdb", "airflow-compare"):
    folder = str(ROOT / "python" / name)
    if folder not in sys.path:
        sys.path.insert(0, folder)

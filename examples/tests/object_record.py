"""Staged object upload recorded in Postgres: crashes between steps, then compensation."""
import argparse
import os
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "python" / "object-record"))
from demo import main

main(argparse.Namespace(
    endpoint=os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000"),
    database_url=os.environ.get(
        "OBJECT_RECORD_DATABASE_URL",
        "postgresql://objectrecord:object-record-local-only@127.0.0.1:54329/object_record",
    ),
    directory=root / "outputs" / "object-record",
))

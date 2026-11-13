"""Run dbt seed, dbt run, and dbt test as separate deoos steps.

A failing test raises. The task ends failed and the dbt run step stays
committed, so a retry does not rebuild the model. DuckDB is the warehouse, so
the project does not need a server.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from deoos import Client

HANDLER = "dbt.orders.v1"
PROJECT = Path(__file__).resolve().parent / "project"
PASSING_ROWS = [{"order_id": "1", "amount": 100}, {"order_id": "2", "amount": 250}]


def valid_id(value):
    if (not isinstance(value, str) or not value or len(value) > 128
            or value in (".", "..")
            or any(not (char.isascii() and (char.isalnum() or char in "_.-")) for char in value)):
        raise ValueError("ID must use 1-128 ASCII letters, digits, dot, underscore, or hyphen")
    return value


def create_client():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        url = os.environ.get("ENGINE_URL")
        if not url:
            raise ValueError("ENGINE_URL is required when DEOOS_MODE=server")
        return Client.remote(url, token=os.environ.get("ENGINE_TOKEN"))
    if mode == "library":
        bucket = os.environ.get("DEOOS_STORAGE_BUCKET")
        if not bucket:
            raise ValueError("DEOOS_STORAGE_BUCKET is required in library mode")
        return Client(provider=os.environ.get("DEOOS_STORAGE_PROVIDER", "s3"), bucket=bucket,
                      prefix=os.environ.get("EXECUTION_PREFIX", "deoos"))
    raise ValueError("DEOOS_MODE must be 'library' or 'server'")


def write_seed(project_dir, rows):
    path = Path(project_dir) / "seeds" / "raw_orders.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["order_id", "amount"])
        writer.writeheader()
        for row in rows:
            writer.writerow({"order_id": row["order_id"], "amount": row["amount"]})
    return {"rows": len(rows), "seed": str(path)}


def mark(counter_dir, name):
    if not counter_dir:
        return
    path = Path(counter_dir) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("run\n")
        handle.flush()
        os.fsync(handle.fileno())


def run_dbt(project_dir, database, arguments):
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = str(database)
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    env["DO_NOT_TRACK"] = "1"
    log_dir = Path(project_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    dbt_bin = Path(sys.executable).with_name("dbt")
    command = [
        str(dbt_bin), *arguments,
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
        "--log-path", str(log_dir),
        "--no-version-check",
        "--no-use-colors",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, env=env, timeout=180)
    output = ((completed.stdout or "") + "\n" + (completed.stderr or "")).strip()
    if completed.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(arguments)} failed\n{output[-2000:]}")
    return {"arguments": list(arguments), "returncode": 0}


def dbt_job(ctx, inputs):
    project = inputs["project_dir"]
    database = inputs["database"]
    counter_dir = inputs.get("counter_dir") or ""

    def seed():
        mark(counter_dir, "dbt-seed")
        write_seed(project, inputs["rows"])
        return run_dbt(project, database, ["seed", "--full-refresh"])

    def build():
        mark(counter_dir, "dbt-run")
        return run_dbt(project, database, ["run"])

    def test():
        mark(counter_dir, "dbt-test")
        return run_dbt(project, database, ["test"])

    ctx.step("dbt-seed", seed)
    ctx.step("dbt-run", build)
    return ctx.step("dbt-test", test)


HANDLERS = {HANDLER: dbt_job}


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    submit = actions.add_parser("submit")
    submit.add_argument("--id", required=True)
    submit.add_argument("--project-dir", default=str(PROJECT))
    submit.add_argument("--database", required=True)
    submit.add_argument("--negative", action="store_true", help="include a row that fails assert_amount_positive")
    submit.add_argument("--counter-dir", default="")
    actions.add_parser("work")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--id", required=True)
    return parser


def rows_for(negative):
    rows = [dict(row) for row in PASSING_ROWS]
    if negative:
        rows.append({"order_id": "3", "amount": -5})
    return rows


def main():
    args = make_parser().parse_args()
    client = None
    try:
        client = create_client()
        if args.action == "submit":
            inputs = {
                "project_dir": str(Path(args.project_dir).resolve()),
                "database": str(Path(args.database).resolve()),
                "rows": rows_for(args.negative),
                "counter_dir": args.counter_dir,
            }
            attempts = 2 if args.negative else 1
            result = client.submit(valid_id(args.id), HANDLER, inputs, max_attempts=attempts, retry_ms=0)
        elif args.action == "inspect":
            result = client.inspect(valid_id(args.id))
        else:
            result = None
            try:
                while True:
                    try:
                        worked = client.run_once(HANDLERS)
                    except Exception as error:
                        print(str(error), file=sys.stderr)
                        time.sleep(0.2)
                        continue
                    if not worked:
                        time.sleep(0.2)
            except KeyboardInterrupt:
                pass
        if result is not None:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())

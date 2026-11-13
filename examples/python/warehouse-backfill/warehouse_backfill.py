"""Backfill one DuckDB partition per day, idempotently.

Each occurrence id is derived from the schedule id and the interval timestamp,
so a second backfill of the same range submits the same tasks. A day that
already completed is not executed again. The load replaces that day's rows
inside one transaction, so a crash after the commit and before the checkpoint
rewrites the same rows.
"""
import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import signal
import sys
import time

from deoos import Client

HANDLER = "warehouse.day.v1"
SCHEDULE_ID = "orders-daily"
DAY_MS = 86_400_000
START_MS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
DAY_COUNT = 4
ROWS_PER_DAY = 3


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


def occurrence_id(schedule_id, at_ms):
    """Same id the engine uses for one schedule occurrence."""
    digest = hashlib.sha256(f"{schedule_id}/{at_ms}".encode()).hexdigest()
    return "schedule-" + digest[:32]


def day_key(at_ms):
    return datetime.fromtimestamp(at_ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def orders_for(day):
    return [
        {"order_id": f"{day}-{index}", "day": day, "cents": 100 * (index + 1)}
        for index in range(ROWS_PER_DAY)
    ]


def replace_day(database, day, orders):
    import duckdb
    path = Path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR PRIMARY KEY,
                day VARCHAR NOT NULL,
                cents INTEGER NOT NULL
            )""")
        connection.execute("BEGIN TRANSACTION")
        try:
            # Primary key is order_id. A second execution of this day replaces those
            # rows instead of inserting another copy.
            connection.executemany(
                "INSERT OR REPLACE INTO orders (order_id, day, cents) VALUES (?, ?, ?)",
                [(row["order_id"], row["day"], row["cents"]) for row in orders],
            )
            connection.execute(
                """DELETE FROM orders
                   WHERE day = ? AND order_id NOT IN (SELECT UNNEST(?))""",
                [day, [row["order_id"] for row in orders]],
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()


def note_execution(log_path, day):
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(day + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_day(ctx, inputs):
    schedule = ctx.task.get("schedule") or {}
    at_ms = schedule.get("scheduled_at")
    if isinstance(at_ms, bool) or not isinstance(at_ms, int):
        raise RuntimeError("this handler runs only as a schedule occurrence")
    day = day_key(at_ms)

    def write():
        replace_day(inputs["database"], day, orders_for(day))
        note_execution(inputs["execution_log"], day)
        marker = inputs.get("crash_marker")
        # Commit is durable. Dying before the checkpoint forces the step to run again.
        if inputs.get("crash_on") == day and marker and not Path(marker).exists():
            path = Path(marker)
            path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.write(fd, day.encode())
            os.fsync(fd)
            os.close(fd)
            os.kill(os.getpid(), signal.SIGKILL)
        return {"day": day, "rows": ROWS_PER_DAY}

    return ctx.step("load", write)


HANDLERS = {HANDLER: load_day}


def backfill_range():
    return START_MS, START_MS + DAY_COUNT * DAY_MS


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    schedule = actions.add_parser("schedule")
    schedule.add_argument("--id", default=SCHEDULE_ID)
    schedule.add_argument("--database", required=True)
    schedule.add_argument("--execution-log", required=True)
    schedule.add_argument("--crash-on", default="")
    schedule.add_argument("--crash-marker", default="")
    fill = actions.add_parser("backfill")
    fill.add_argument("--id", default=SCHEDULE_ID)
    actions.add_parser("work")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--id", required=True)
    return parser


def main():
    args = make_parser().parse_args()
    client = None
    try:
        client = create_client()
        if args.action == "schedule":
            inputs = {
                "database": str(Path(args.database).resolve()),
                "execution_log": str(Path(args.execution_log).resolve()),
                "crash_on": args.crash_on,
                "crash_marker": args.crash_marker,
            }
            result = client.schedule(
                valid_id(args.id), HANDLER, inputs, DAY_MS, first_due_ms=START_MS,
                missed="latest", overlap="skip", max_attempts=3, retry_ms=0,
            )
        elif args.action == "backfill":
            start_ms, end_ms = backfill_range()
            result = client.backfill(valid_id(args.id), start_ms, end_ms, limit=DAY_COUNT)
        elif args.action == "inspect":
            result = client.inspect_schedule(valid_id(args.id))
        else:
            result = None
            stop = os.environ.get("BACKFILL_STOP")
            try:
                while True:
                    if stop and Path(stop).exists():
                        break
                    try:
                        worked = client.run_once(HANDLERS)
                    except Exception as error:
                        print(str(error), file=sys.stderr)
                        time.sleep(0.05)
                        continue
                    if not worked:
                        time.sleep(0.05)
            except KeyboardInterrupt:
                pass
        if result is not None:
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())

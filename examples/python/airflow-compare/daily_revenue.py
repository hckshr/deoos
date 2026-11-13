"""The same daily paid-revenue job as daily_revenue_dag.py, on deoos.

Child tasks are the dependency edges. Step results are the XCom equivalent.
max_attempts=2 matches Airflow retries=1 (one retry). A fixed 86400000 ms
interval with overlap=skip is the closest schedule to ``0 6 * * *`` plus
max_active_runs=1. It is not that cron, and it has no timezone.
"""
import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

from deoos import Client

DAILY = "compare.daily.v1"
EXTRACT = "compare.extract.v1"
TRANSFORM = "compare.transform.v1"
LOAD = "compare.load.v1"
DAY_MS = 86_400_000


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


def read_orders(path):
    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    cleaned = []
    for row in rows:
        cleaned.append({
            "order_id": row["order_id"],
            "day": row["day"],
            "status": row["status"],
            "cents": int(row["cents"]),
        })
    return cleaned


def revenue(rows):
    paid = [row for row in rows if row["status"] == "paid"]
    return {"paid_orders": len(paid), "cents": sum(row["cents"] for row in paid)}


def logical_day(ctx, inputs):
    schedule = ctx.task.get("schedule") or {}
    at_ms = schedule.get("scheduled_at")
    if isinstance(at_ms, int) and not isinstance(at_ms, bool):
        return datetime.fromtimestamp(at_ms / 1000, timezone.utc).strftime("%Y-%m-%d")
    day = inputs.get("day")
    if not isinstance(day, str):
        raise ValueError("day is required when the task is not a schedule occurrence")
    return day


def extract(ctx, inputs):
    def pull():
        return [row for row in read_orders(inputs["orders"]) if row["day"] == inputs["day"]]

    return ctx.step("extract", pull)


def transform(ctx, inputs):
    return ctx.step("transform", lambda: revenue(inputs["rows"]))


def load(ctx, inputs):
    def write():
        payload = {
            "day": inputs["day"],
            "paid_orders": inputs["summary"]["paid_orders"],
            "cents": inputs["summary"]["cents"],
        }
        path = Path(inputs["output"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        return payload

    return ctx.step("load", write)


def daily(ctx, inputs):
    day = logical_day(ctx, inputs)
    child = {"orders": inputs["orders"], "output": inputs["output"], "day": day}
    rows = ctx.join("extracted", [ctx.spawn("extract", EXTRACT, child)])[0]
    summary = ctx.join("transformed", [ctx.spawn(
        "transform", TRANSFORM, {**child, "rows": rows},
    )])[0]
    return ctx.join("loaded", [ctx.spawn(
        "load", LOAD, {**child, "summary": summary},
    )])[0]


HANDLERS = {DAILY: daily, EXTRACT: extract, TRANSFORM: transform, LOAD: load}


def schedule_daily(client, schedule_id, orders, output, first_due_ms):
    """Interval stand-in for the Airflow cron. Not a 06:00 schedule."""
    return client.schedule(
        schedule_id, DAILY,
        {"orders": orders, "output": output},
        DAY_MS, first_due_ms=first_due_ms, missed="latest", overlap="skip",
        max_attempts=2, retry_ms=1000,
    )


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    submit = actions.add_parser("submit")
    submit.add_argument("--id", required=True)
    submit.add_argument("--day", required=True)
    submit.add_argument("--orders", required=True)
    submit.add_argument("--output", required=True)
    actions.add_parser("work")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--id", required=True)
    return parser


def main():
    args = make_parser().parse_args()
    client = None
    try:
        client = create_client()
        if args.action == "submit":
            inputs = {
                "day": args.day,
                "orders": str(Path(args.orders).resolve()),
                "output": str(Path(args.output).resolve()),
            }
            result = client.submit(valid_id(args.id), DAILY, inputs, max_attempts=2, retry_ms=1000)
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

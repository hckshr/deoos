"""Daily orders ETL: extract, then transform and audit, then load.

The schedule is a fixed interval with overlap=skip. deoos has no cron
expression and no timezone. ``hold_ms`` keeps one occurrence active so a later
interval can be skipped; a real load leaves it at 0. ``fail_once`` makes the
extract step raise on its first attempt so the task retry is visible.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import sys
import time

from deoos import Client

HANDLER = "etl.pipeline.v1"
EXTRACT = "etl.extract.v1"
TRANSFORM = "etl.transform.v1"
AUDIT = "etl.audit.v1"
LOAD = "etl.load.v1"
DEFAULT_SOURCE = str(Path(__file__).resolve().parent / "orders.json")


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


def record(log_path, event):
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(event + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_orders(source):
    rows = json.loads(Path(source).read_text(encoding="utf-8"))
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("orders file must be a JSON array of objects")
    cleaned = []
    for row in rows:
        if not isinstance(row.get("id"), str) or not isinstance(row.get("status"), str):
            raise ValueError("each order needs a string id and status")
        cents = row.get("cents")
        if isinstance(cents, bool) or not isinstance(cents, int):
            raise ValueError("each order needs an integer cents")
        cleaned.append({"id": row["id"], "status": row["status"], "cents": cents})
    return cleaned


def pipeline_inputs(source, output_dir, log_path, attempt_dir, hold_ms=0, fail_once=False):
    if hold_ms < 0:
        raise ValueError("hold_ms must be >= 0")
    return {
        "source": str(source),
        "output_dir": str(output_dir),
        "log_path": str(log_path),
        "attempt_dir": str(attempt_dir),
        "hold_ms": hold_ms,
        "fail_once": bool(fail_once),
    }


def extract(ctx, inputs):
    def pull():
        record(inputs["log_path"], "extract")
        marker = Path(inputs["attempt_dir"]) / ctx.task["id"]
        seen = int(marker.read_text(encoding="utf-8")) if marker.exists() else 0
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(str(seen + 1), encoding="utf-8")
        if inputs.get("fail_once") and seen == 0:
            raise RuntimeError("extract upstream blip")
        return read_orders(inputs["source"])

    return ctx.step("pull", pull)


def transform(ctx, inputs):
    def paid():
        record(inputs["log_path"], "transform")
        return [row for row in inputs["rows"] if row["status"] == "paid"]

    return ctx.step("paid", paid)


def audit(ctx, inputs):
    def count():
        record(inputs["log_path"], "audit")
        rows = inputs["rows"]
        return {"rows": len(rows), "paid": sum(row["status"] == "paid" for row in rows)}

    return ctx.step("count", count)


def load(ctx, inputs):
    def write():
        record(inputs["log_path"], "load")
        rows = inputs["rows"]
        audit_result = inputs["audit"]
        if audit_result["paid"] != len(rows):
            raise RuntimeError("audit count does not match transformed rows")
        output = Path(inputs["output_dir"]) / f'{ctx.task["id"]}.csv'
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(".csv.tmp")
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["id", "status", "cents"])
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
        return {"rows": len(rows), "cents": sum(row["cents"] for row in rows), "output": str(output)}

    return ctx.step("write", write)


def pipeline(ctx, inputs):
    # extract must finish before the two downstream tasks, which then both finish before load.
    child = {key: inputs[key] for key in ("source", "output_dir", "log_path", "attempt_dir", "fail_once")}
    extracted = ctx.join("extracted", [ctx.spawn("extract", EXTRACT, child, max_attempts=3, retry_ms=200)])[0]
    branched = {**child, "rows": extracted}
    transform_id = ctx.spawn("transform", TRANSFORM, branched)
    audit_id = ctx.spawn("audit", AUDIT, branched)
    transformed, audited = ctx.join("checked", [transform_id, audit_id])
    loaded = ctx.join("loaded", [ctx.spawn(
        "load", LOAD, {**child, "rows": transformed, "audit": audited},
    )])[0]
    hold_ms = inputs.get("hold_ms") or 0
    if hold_ms:
        ctx.sleep("hold", hold_ms)
    return loaded


HANDLERS = {
    HANDLER: pipeline,
    EXTRACT: extract,
    TRANSFORM: transform,
    AUDIT: audit,
    LOAD: load,
}


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    schedule = actions.add_parser("schedule", help="create the fixed-interval ETL schedule")
    schedule.add_argument("--id", required=True)
    schedule.add_argument("--interval-ms", type=int, default=86_400_000)
    schedule.add_argument("--first-due-ms", type=int)
    schedule.add_argument("--source", default=DEFAULT_SOURCE)
    schedule.add_argument("--output-dir", required=True)
    schedule.add_argument("--log", required=True)
    schedule.add_argument("--attempt-dir", required=True)
    schedule.add_argument("--hold-ms", type=int, default=0)
    schedule.add_argument("--fail-once", action="store_true")
    actions.add_parser("work", help="poll and run ETL tasks")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--id", required=True)
    inspect.add_argument("--schedule", action="store_true")
    return parser


def main():
    args = make_parser().parse_args()
    client = None
    try:
        client = create_client()
        if args.action == "schedule":
            inputs = pipeline_inputs(args.source, args.output_dir, args.log, args.attempt_dir,
                                     args.hold_ms, args.fail_once)
            result = client.schedule(
                valid_id(args.id), HANDLER, inputs, args.interval_ms,
                first_due_ms=args.first_due_ms, missed="latest", overlap="skip",
                max_attempts=3, retry_ms=200,
            )
        elif args.action == "inspect":
            result = (client.inspect_schedule if args.schedule else client.inspect)(valid_id(args.id))
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

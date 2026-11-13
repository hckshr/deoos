"""Backfill four days, kill after one commit, resume, then backfill again."""
import os
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

import paths
from deoos import Client
from engine_server import create_bucket, delete_bucket, server_worker_env, start_deoos_server, stop_process, wait_for_rustfs
from warehouse_backfill import (
    DAY_COUNT, DAY_MS, HANDLER, HANDLERS, ROWS_PER_DAY, SCHEDULE_ID, START_MS, occurrence_id,
)

ROOT = paths.ROOT
ENDPOINT = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")
CRASH_DAY = "2024-01-02"


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def expected_at():
    return [START_MS + index * DAY_MS for index in range(DAY_COUNT)]


def day_tasks(client):
    wanted = set(expected_at())
    found = {}
    for task in client.list_tasks()["tasks"]:
        schedule = task.get("schedule") or {}
        if schedule.get("id") == SCHEDULE_ID and schedule.get("scheduled_at") in wanted:
            found[schedule["scheduled_at"]] = task
    return found


def drain(client):
    deadline = time.monotonic() + 60
    wanted = expected_at()
    while time.monotonic() < deadline:
        found = day_tasks(client)
        done = [found[at] for at in wanted if at in found and found[at]["status"] == "completed"]
        schedule = client.inspect_schedule(SCHEDULE_ID)
        if schedule.get("backfill") is None and len(done) == DAY_COUNT:
            client.pause_schedule(SCHEDULE_ID)
            return found
        try:
            client.run_once(HANDLERS)
        except Exception:
            time.sleep(0.05)
        else:
            time.sleep(0.02)
    raise TimeoutError(f"backfill did not finish; schedule={client.inspect_schedule(SCHEDULE_ID)}")


def warehouse_rows(database):
    import duckdb
    connection = duckdb.connect(database, read_only=True)
    try:
        grouped = connection.execute(
            """SELECT day, COUNT(*), COUNT(DISTINCT order_id), COALESCE(SUM(cents), 0)
               FROM orders GROUP BY day ORDER BY day"""
        ).fetchall()
        duplicate = connection.execute(
            "SELECT order_id FROM orders GROUP BY order_id HAVING COUNT(*) > 1"
        ).fetchall()
        total = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    finally:
        connection.close()
    return grouped, duplicate, total


def main():
    wait_for_rustfs(ENDPOINT)
    bucket = "deoos-backfill-" + uuid.uuid4().hex[:20]
    prefix = "backfill-" + uuid.uuid4().hex[:8]
    directory = ROOT / "outputs" / "warehouse-backfill"
    directory.mkdir(parents=True, exist_ok=True)
    database = directory / "orders.duckdb"
    execution_log = directory / "executions.log"
    crash_marker = directory / "crashed"
    for path in (database, Path(str(database) + ".wal"), execution_log, crash_marker):
        path.unlink(missing_ok=True)
    s3 = create_bucket(ENDPOINT, bucket)
    server = None
    log = open(directory / "deoos-server.log", "w")
    client = None
    worker = None
    try:
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), bucket, prefix, log, ENDPOINT, lease_ms=2000)
        client = Client.remote(remote["url"], token=remote["token"])
        inputs = {
            "database": str(database),
            "execution_log": str(execution_log),
            "crash_on": CRASH_DAY,
            "crash_marker": str(crash_marker),
        }
        client.schedule(
            SCHEDULE_ID, HANDLER, inputs, DAY_MS, first_due_ms=START_MS,
            missed="latest", overlap="skip", max_attempts=3, retry_ms=0,
        )
        start_ms, end_ms = START_MS, START_MS + DAY_COUNT * DAY_MS
        first = client.backfill(SCHEDULE_ID, start_ms, end_ms, limit=DAY_COUNT)
        again = client.backfill(SCHEDULE_ID, start_ms, end_ms, limit=DAY_COUNT)
        check(first["backfill"]["remaining"] == DAY_COUNT, first["backfill"])
        check(again["backfill"] == first["backfill"], "repeat backfill changed the active cursor")
        env = server_worker_env()
        env.update(DEOOS_MODE="server", ENGINE_URL=remote["url"], ENGINE_TOKEN=remote["token"])
        worker_log = open(directory / "worker.log", "w")
        worker = subprocess.Popen(
            [sys.executable, "python/warehouse-backfill/warehouse_backfill.py", "work"],
            cwd=ROOT, env=env, stdout=worker_log, stderr=subprocess.STDOUT, text=True,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if crash_marker.exists() and worker.poll() is not None:
                break
            if worker.poll() is not None and not crash_marker.exists():
                worker_log.flush()
                raise RuntimeError(
                    f"worker exited {worker.returncode} before the crash marker\n"
                    + (directory / "worker.log").read_text(encoding="utf-8")
                )
            time.sleep(0.05)
        else:
            stop_process(worker)
            raise TimeoutError("worker did not crash mid-backfill")
        crashed_at = START_MS + DAY_MS
        check(occurrence_id(SCHEDULE_ID, crashed_at).startswith("schedule-"), "occurrence id shape")
        found = day_tasks(client)
        check(found[START_MS]["status"] == "completed", found.get(START_MS))
        check("load" not in found[crashed_at]["steps"], found[crashed_at]["steps"])
        drain(client)
        grouped, duplicate, total = warehouse_rows(str(database))
        check(duplicate == [], duplicate)
        check(total == DAY_COUNT * ROWS_PER_DAY, total)
        days = [row[0] for row in grouped]
        check(days == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"], grouped)
        check(all(row[1] == ROWS_PER_DAY and row[2] == ROWS_PER_DAY and row[3] == 600 for row in grouped), grouped)
        executions = Counter(execution_log.read_text(encoding="utf-8").splitlines())
        check(executions == Counter({
            "2024-01-01": 1, "2024-01-02": 2, "2024-01-03": 1, "2024-01-04": 1,
        }), executions)
        snapshot = execution_log.read_text(encoding="utf-8")
        client.backfill(SCHEDULE_ID, start_ms, end_ms, limit=DAY_COUNT)
        client.resume_schedule(SCHEDULE_ID)
        drain(client)
        check(execution_log.read_text(encoding="utf-8") == snapshot, "the second backfill executed a day again")
        grouped, duplicate, total = warehouse_rows(str(database))
        check(duplicate == [] and total == DAY_COUNT * ROWS_PER_DAY, (duplicate, total, grouped))
        for at in expected_at():
            task = client.inspect(occurrence_id(SCHEDULE_ID, at))
            check(task["status"] == "completed" and task["schedule"]["scheduled_at"] == at, task["id"])
        print("warehouse backfill: 12 unique rows, crashed day loaded twice, second backfill wrote nothing", flush=True)
    finally:
        stop_process(worker)
        if "worker_log" in locals():
            worker_log.close()
        if client is not None:
            client.close()
        stop_process(server)
        log.close()
        delete_bucket(s3, bucket)


if __name__ == "__main__":
    main()

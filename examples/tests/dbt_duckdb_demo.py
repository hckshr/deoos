"""dbt seed/run/test on deoos-server: a green project, and a test failure that sticks."""
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import paths
from deoos import Client
from dbt_job import HANDLER, HANDLERS, PASSING_ROWS
from engine_server import create_bucket, delete_bucket, start_deoos_server, stop_process, wait_for_rustfs

ROOT = paths.ROOT
ENDPOINT = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")
PROJECT = ROOT / "python" / "dbt-duckdb" / "project"


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def ensure_dbt():
    probe = subprocess.run([sys.executable, "-c", "import dbt.cli.main"], capture_output=True, text=True)
    if probe.returncode == 0:
        return
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--disable-pip-version-check",
         "-r", str(ROOT / "tests" / "requirements-dbt.txt")],
        check=True, timeout=600,
    )


def runs(counter_dir, name):
    path = Path(counter_dir) / name
    if not path.exists():
        return 0
    return len([line for line in path.read_text(encoding="utf-8").splitlines() if line])


def table_amounts(database):
    import duckdb
    connection = duckdb.connect(database, read_only=True)
    try:
        return connection.execute("SELECT amount FROM stg_orders ORDER BY order_id").fetchall()
    finally:
        connection.close()


def drive(client, task_id):
    deadline = time.monotonic() + 180
    last = None
    while time.monotonic() < deadline:
        last = client.inspect(task_id)
        if last["status"] in ("completed", "failed", "cancelled"):
            return last
        try:
            client.run_once(HANDLERS)
        except Exception:
            time.sleep(0.05)
        else:
            time.sleep(0.02)
    raise TimeoutError(f"{task_id} did not finish: {None if last is None else last.get('status')}")


def submit(client, task_id, project_dir, database, rows, counter_dir, attempts):
    return client.submit(task_id, HANDLER, {
        "project_dir": str(project_dir),
        "database": str(database),
        "rows": rows,
        "counter_dir": str(counter_dir),
    }, max_attempts=attempts, retry_ms=0)


def main():
    ensure_dbt()
    wait_for_rustfs(ENDPOINT)
    bucket = "deoos-dbt-" + uuid.uuid4().hex[:20]
    prefix = "dbt-" + uuid.uuid4().hex[:8]
    directory = ROOT / "outputs" / "dbt-duckdb"
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    passing_project = directory / "passing-project"
    failing_project = directory / "failing-project"
    shutil.copytree(PROJECT, passing_project)
    shutil.copytree(PROJECT, failing_project)
    s3 = create_bucket(ENDPOINT, bucket)
    server = None
    log = open(directory / "deoos-server.log", "w")
    client = None
    try:
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), bucket, prefix, log, ENDPOINT, lease_ms=15000)
        client = Client.remote(remote["url"], token=remote["token"])
        passing_db = directory / "passing.duckdb"
        passing_counts = directory / "passing-counts"
        submit(client, "dbt-orders-ok", passing_project, passing_db, PASSING_ROWS, passing_counts, 1)
        passed = drive(client, "dbt-orders-ok")
        check(passed["status"] == "completed", passed.get("error"))
        check(runs(passing_counts, "dbt-seed") == 1, "dbt seed did not run once")
        check(runs(passing_counts, "dbt-run") == 1, "dbt run did not run once")
        check(runs(passing_counts, "dbt-test") == 1, "dbt test did not run once")
        check(table_amounts(str(passing_db)) == [(100,), (250,)], table_amounts(str(passing_db)))
        failing_db = directory / "failing.duckdb"
        failing_counts = directory / "failing-counts"
        rows = [dict(row) for row in PASSING_ROWS] + [{"order_id": "3", "amount": -5}]
        submit(client, "dbt-orders-bad", failing_project, failing_db, rows, failing_counts, 2)
        failed = drive(client, "dbt-orders-bad")
        check(failed["status"] == "failed", failed["status"])
        check("assert_amount_positive" in (failed.get("error") or ""), failed.get("error"))
        check("dbt-run" in failed["steps"] and "dbt-test" not in failed["steps"], failed["steps"])
        check(runs(failing_counts, "dbt-seed") == 1, runs(failing_counts, "dbt-seed"))
        check(runs(failing_counts, "dbt-run") == 1, "dbt run was repeated after the test failed")
        check(runs(failing_counts, "dbt-test") == 2, "the failing test was not retried")
        check(failed["attempts"] == 2, failed["attempts"])
        amounts = sorted(row[0] for row in table_amounts(str(failing_db)))
        check(amounts == [-5, 100, 250], amounts)
        print("dbt duckdb: passing test completed; failing assert_amount_positive kept dbt-run and failed the task", flush=True)
    finally:
        if client is not None:
            client.close()
        stop_process(server)
        log.close()
        delete_bucket(s3, bucket)


if __name__ == "__main__":
    main()

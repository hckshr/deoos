"""Run the object-record workflow against local RustFS, local Postgres, and deoos-server.

The happy path leaves one final object and one final row. Two runs kill the
worker after a write and before that step's checkpoint, then resume the same
task. Two runs refuse a write and compensate. Each run finishes with no staged
object left behind and no pending row.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.parse
import uuid

import boto3
import psycopg
from psycopg.rows import dict_row
from deoos import Client

from record import (
    FILE_NAME, HANDLER, case_inputs, ensure_schema, placement, read_journal, record_object,
)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests"))
from engine_server import (  # noqa: E402
    ACCESS_KEY, REGION, SECRET_KEY, create_bucket, delete_bucket, server_worker_env,
    start_deoos_server, stop_process, wait_for_rustfs,
)

STORAGE = dict(
    provider="s3", endpoint="http://127.0.0.1:19000", allow_http=True,
    region=REGION, access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, lease_ms=5000,
)
ADMIN_URL = "postgresql://objectrecord:object-record-local-only@127.0.0.1:54329/object_record"
COLUMNS = (
    "idempotency_key", "bucket", "staging_key", "object_key", "name",
    "checksum_sha256", "byte_size", "status",
)


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def source_body(task_id):
    return (
        f"field notes\n"
        f"task: {task_id}\n"
        "An upload and a database row do not share a transaction.\n"
    ).encode()


def write_bytes(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, body)
        os.fsync(fd)
    finally:
        os.close(fd)


def tail(path):
    if not path.exists():
        return ""
    lines = path.read_text(errors="replace").splitlines()
    return "\n".join(lines[-40:])


def database_url_for(admin_url, name):
    if not re.fullmatch(r"objrec_[0-9a-f]{12}", name):
        raise RuntimeError("unexpected database name")
    parts = urllib.parse.urlsplit(admin_url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, "/" + name, parts.query, parts.fragment))


def postgres_connects(url, timeout):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(url, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            return None
        except Exception as error:
            last = error
            time.sleep(0.3)
    return last


def ensure_postgres(admin_url):
    if postgres_connects(admin_url, timeout=2) is None:
        return
    parsed = urllib.parse.urlsplit(admin_url)
    local = parsed.hostname in ("127.0.0.1", "localhost") and parsed.port == 54329
    if local:
        completed = subprocess.run(
            ["docker", "compose", "--profile", "postgres", "up", "-d", "--wait", "postgres"],
            cwd=ROOT, capture_output=True, text=True,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            raise RuntimeError(detail[-2000:] or "docker compose up postgres failed")
    error = postgres_connects(admin_url, timeout=60)
    where = f"{parsed.hostname}:{parsed.port}"
    if error is not None:
        raise RuntimeError(
            f"Postgres did not become ready at {where}: {error}. "
            "Start it with: docker compose --profile postgres up -d postgres"
        )


def create_database(admin_url, name):
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=5) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')


def drop_database(admin_url, name):
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=5) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def read_objects(s3, bucket):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        keys.extend(row["Key"] for row in page.get("Contents", []))
    found = {}
    for key in keys:
        found[key] = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return found


def read_rows(database_url):
    with psycopg.connect(database_url, connect_timeout=5, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT idempotency_key, bucket, staging_key, object_key, name, "
            "checksum_sha256, byte_size, status FROM object_records ORDER BY idempotency_key"
        ).fetchall()
    return [{column: (int(row[column]) if column == "byte_size" else row[column]) for column in COLUMNS}
            for row in rows]


def check_world(s3, bucket, database_url, expected_objects, expected_rows):
    actual_objects = read_objects(s3, bucket)
    extra = sorted(set(actual_objects) - set(expected_objects))
    missing = sorted(set(expected_objects) - set(actual_objects))
    changed = sorted(key for key, body in expected_objects.items()
                     if key in actual_objects and actual_objects[key] != body)
    actual_rows = read_rows(database_url)
    if extra or missing or changed or actual_rows != expected_rows:
        raise RuntimeError(
            "object and row inventory did not match\n"
            f"orphaned objects: {extra}\n"
            f"missing objects: {missing}\n"
            f"changed objects: {changed}\n"
            f"rows: {actual_rows}\n"
            f"expected rows: {expected_rows}"
        )


def row_for(task_id, bucket, body, status):
    place = placement(f"{task_id}/upload")
    return {
        "idempotency_key": place["idempotency_key"],
        "bucket": bucket,
        "staging_key": place["staging_key"],
        "object_key": place["object_key"],
        "name": FILE_NAME,
        "checksum_sha256": hashlib.sha256(body).hexdigest(),
        "byte_size": len(body),
        "status": status,
    }


def sorted_rows(rows):
    return sorted(rows, key=lambda row: row["idempotency_key"])


def serve(config):
    client = Client.remote(config["client"]["url"], token=config["client"]["token"])
    try:
        def handler(ctx, inputs):
            crash_after = config.get("crash_after")
            if not crash_after:
                return record_object(ctx, inputs)
            original = ctx.step

            def step(name, function, revision="1"):
                def wrapped():
                    result = function()
                    if name == crash_after:
                        ready = Path(config["ready"])
                        ready.write_text(name)
                        fd = os.open(ready, os.O_RDONLY)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                        time.sleep(3600)
                    return result
                return original(name, wrapped, revision)

            ctx.step = step
            return record_object(ctx, inputs)

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            state = client.inspect(config["task_id"])
            if state["status"] == "completed":
                return
            if state["status"] in ("failed", "cancelled"):
                raise RuntimeError(state.get("error") or state["status"])
            client.run_once({HANDLER: handler})
            time.sleep(0.05)
        raise RuntimeError(f"{config['task_id']} did not finish")
    finally:
        client.close()


def spawn(directory, client_config, task_id, database_url, endpoint, crash_after=None, label="worker"):
    ready = directory / f"{task_id}-ready"
    config_path = directory / f"{task_id}-{label}.json"
    config_path.write_text(json.dumps({
        "client": client_config, "task_id": task_id, "crash_after": crash_after, "ready": str(ready),
    }))
    config_path.chmod(0o600)
    log_path = directory / f"{task_id}-{label}.log"
    env = server_worker_env()
    env.update(
        OBJECT_ENDPOINT=endpoint,
        OBJECT_REGION=REGION,
        OBJECT_ACCESS_KEY_ID=ACCESS_KEY,
        OBJECT_SECRET_ACCESS_KEY=SECRET_KEY,
        DATABASE_URL=database_url,
    )
    log = open(log_path, "w")
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--worker", str(config_path)],
        env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    return process, log, log_path, ready


def stop(process, log):
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)
    if log is not None and not log.closed:
        log.close()


def wait_until(predicate, process, log_path, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            found = predicate()
            if found:
                return found
            raise RuntimeError(f"worker exited {process.returncode}\n{tail(log_path)}")
        found = predicate()
        if found:
            return found
        time.sleep(0.05)
    raise TimeoutError(f"timed out\n{tail(log_path)}")


def wait_exit(process, log_path, timeout=90):
    try:
        code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        raise TimeoutError(f"worker did not exit\n{tail(log_path)}") from None
    check(code == 0, f"worker exited {code}\n{tail(log_path)}")


def outcome(client, task_id):
    view = client.view(task_id)
    check(view["status"] == "completed", f"{task_id} ended {view['status']}: {view.get('error')}")
    return view["output"]


def journal_event(system, op, key, **fields):
    return {"system": system, "op": op, "key": key, **fields}


def main(args):
    endpoint = args.endpoint
    admin_url = args.database_url
    run_id = uuid.uuid4().hex[:12]
    directory = Path(args.directory).resolve() / run_id
    directory.mkdir(parents=True)
    engine_bucket = "deoos-obj-" + run_id
    object_bucket = "objects-" + run_id
    database_name = "objrec_" + run_id
    report = {
        "engine_bucket": engine_bucket,
        "object_bucket": object_bucket,
        "database": database_name,
        "endpoint": endpoint,
        "engine": "deoos-server",
        "cases": [],
        "cleaned": False,
    }
    evidence = directory / "result.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    s3 = boto3.client(
        "s3", endpoint_url=endpoint, region_name=REGION,
        aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
    )
    created = []
    database_url = None
    client = None
    server = None
    server_log = None
    try:
        wait_for_rustfs(endpoint)
        ensure_postgres(admin_url)
        create_bucket(endpoint, engine_bucket)
        created.append(engine_bucket)
        create_bucket(endpoint, object_bucket)
        created.append(object_bucket)
        create_database(admin_url, database_name)
        database_url = database_url_for(admin_url, database_name)
        ensure_schema(database_url)
        server_log = open(directory / "deoos-server.log", "w")
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), engine_bucket, "object-record", server_log,
            endpoint, lease_ms=STORAGE["lease_ms"],
        )
        report["engine_url"] = remote["url"]
        client = Client.remote(remote["url"], token=remote["token"])
        expected_objects = {}
        expected_rows = []

        def submit(task_id, fail_at=None):
            case = directory / task_id
            case.mkdir()
            body = source_body(task_id)
            write_bytes(case / FILE_NAME, body)
            journal = case / "journal.jsonl"
            client.submit(
                task_id, HANDLER,
                case_inputs(str(case / FILE_NAME), object_bucket, str(journal), fail_at=fail_at),
                max_attempts=5, retry_ms=0,
            )
            return body, journal

        def drive(task_id, crash_after=None):
            process, log, log_path, ready = spawn(
                directory, remote, task_id, database_url, endpoint, crash_after)
            try:
                if crash_after is None:
                    wait_exit(process, log_path)
                    return ready
                def waiting():
                    if not ready.exists():
                        return None
                    return client.inspect(task_id)
                state = wait_until(waiting, process, log_path)
                return state
            finally:
                if crash_after is not None:
                    stop(process, log)
                elif log is not None and not log.closed:
                    log.close()

        def resume(task_id):
            process, log, log_path, _ready = spawn(
                directory, remote, task_id, database_url, endpoint, None, label="resume")
            try:
                wait_exit(process, log_path)
            finally:
                stop(process, log)

        def assert_output(task_id, output, outcome_name, body):
            place = placement(f"{task_id}/upload")
            check(output["outcome"] == outcome_name, f"{task_id} outcome was {output['outcome']}")
            check(output["idempotency_key"] == place["idempotency_key"], f"{task_id} idempotency key changed")
            check(output["checksum_sha256"] == hashlib.sha256(body).hexdigest(), f"{task_id} checksum changed")
            check(output["byte_size"] == len(body), f"{task_id} size changed")
            check(output["name"] == FILE_NAME, f"{task_id} file name changed")
            state = client.inspect(task_id)
            check(state["max_attempts"] == 5 and state["retry_ms"] == 0, f"{task_id} attempt budget changed")
            check(client.view(task_id)["function"] == HANDLER, f"{task_id} handler changed")

        recorded_body, recorded_journal = submit("recorded")
        drive("recorded")
        recorded = outcome(client, "recorded")
        recorded_row = row_for("recorded", object_bucket, recorded_body, "final")
        expected_objects[recorded_row["object_key"]] = recorded_body
        expected_rows.append(recorded_row)
        check_world(s3, object_bucket, database_url, expected_objects, sorted_rows(expected_rows))
        assert_output("recorded", recorded, "recorded", recorded_body)
        check(recorded["inventory"] == {"staging": False, "final": True, "row_status": "final"},
              "recorded inventory was not one final object and one final row")
        check(recorded["promoted"]["status"] == "final", "recorded promote did not finish")
        check(recorded["failed_step"] is None and recorded["compensations"] == {},
              "recorded path compensated")
        check(set(client.inspect("recorded")["steps"]) == {"upload", "record", "promote", "audit"},
              "recorded steps were not upload, record, promote, and audit")
        check(client.inspect("recorded")["attempts"] == 1, "recorded path took more than one attempt")
        check(read_journal(recorded_journal) == [
            journal_event("s3", "put", recorded_row["staging_key"], replay=False),
            journal_event("postgres", "insert", recorded_row["idempotency_key"], replay=False),
            journal_event("s3", "promote", recorded_row["object_key"], replay=False),
            journal_event("postgres", "finalize", recorded_row["idempotency_key"], replay=False),
            journal_event("s3", "delete", recorded_row["staging_key"], existed=True),
        ], "recorded journal was not one put, one insert, one promote, and one delete")
        report["cases"].append("recorded")
        print("recorded: one final object and one final row; the staging key is gone", flush=True)

        upload_body, upload_journal = submit("crash-upload")
        upload_row = row_for("crash-upload", object_bucket, upload_body, "final")
        gap_objects = dict(expected_objects)
        gap_objects[upload_row["staging_key"]] = upload_body
        drive("crash-upload", crash_after="upload")
        check_world(s3, object_bucket, database_url, gap_objects, sorted_rows(expected_rows))
        gap_state = client.inspect("crash-upload")
        check(gap_state["status"] == "running" and "upload" not in gap_state["steps"],
              "crash-upload committed the upload step or left the running state")
        check(gap_state["attempts"] == 1, "crash-upload did not stop on the first attempt")
        check(read_journal(upload_journal) == [
            journal_event("s3", "put", upload_row["staging_key"], replay=False),
        ], "crash-upload journal did not stop after the staged put")
        report.setdefault("gaps", {})["crash-upload"] = {
            "objects": sorted(gap_objects),
            "rows": [row["idempotency_key"] for row in expected_rows],
            "steps": sorted(gap_state["steps"]),
        }
        resume("crash-upload")
        upload_out = outcome(client, "crash-upload")
        expected_objects[upload_row["object_key"]] = upload_body
        expected_rows.append(upload_row)
        check_world(s3, object_bucket, database_url, expected_objects, sorted_rows(expected_rows))
        assert_output("crash-upload", upload_out, "recorded", upload_body)
        check(upload_out["inventory"] == {"staging": False, "final": True, "row_status": "final"},
              "resumed upload left a staged object or a pending row")
        check(set(client.inspect("crash-upload")["steps"]) == {"upload", "record", "promote", "audit"},
              "resumed upload did not finish every step")
        check(client.inspect("crash-upload")["attempts"] == 2, "killed upload was not resumed as a second attempt")
        claims = [event for event in client.history("crash-upload") if event["event"] == "claim"]
        check(len(claims) == 2, "resume did not claim crash-upload a second time")
        check(read_journal(upload_journal) == [
            journal_event("s3", "put", upload_row["staging_key"], replay=False),
            journal_event("s3", "put", upload_row["staging_key"], replay=True),
            journal_event("postgres", "insert", upload_row["idempotency_key"], replay=False),
            journal_event("s3", "promote", upload_row["object_key"], replay=False),
            journal_event("postgres", "finalize", upload_row["idempotency_key"], replay=False),
            journal_event("s3", "delete", upload_row["staging_key"], existed=True),
        ], "resumed upload did not put the same key again and then insert once")
        report["cases"].append("crash-upload")
        print("crash-upload: killed after the staged put, before the row; "
              "resume wrote that row once and promoted it", flush=True)

        record_body, record_journal = submit("crash-record")
        record_row = row_for("crash-record", object_bucket, record_body, "final")
        pending_row = dict(record_row)
        pending_row["status"] = "pending"
        gap_objects = dict(expected_objects)
        gap_objects[record_row["staging_key"]] = record_body
        gap_rows = sorted_rows([*expected_rows, pending_row])
        drive("crash-record", crash_after="record")
        check_world(s3, object_bucket, database_url, gap_objects, gap_rows)
        gap_state = client.inspect("crash-record")
        check(gap_state["status"] == "running" and "upload" in gap_state["steps"]
              and "record" not in gap_state["steps"] and "promote" not in gap_state["steps"],
              "crash-record did not stop between the committed upload and promote")
        check(read_journal(record_journal) == [
            journal_event("s3", "put", record_row["staging_key"], replay=False),
            journal_event("postgres", "insert", record_row["idempotency_key"], replay=False),
        ], "crash-record journal did not stop after the insert")
        report["gaps"]["crash-record"] = {
            "objects": sorted(gap_objects),
            "rows": [row["idempotency_key"] + ":" + row["status"] for row in gap_rows],
            "steps": sorted(gap_state["steps"]),
        }
        resume("crash-record")
        record_out = outcome(client, "crash-record")
        expected_objects[record_row["object_key"]] = record_body
        expected_rows.append(record_row)
        check_world(s3, object_bucket, database_url, expected_objects, sorted_rows(expected_rows))
        assert_output("crash-record", record_out, "recorded", record_body)
        check(record_out["inventory"] == {"staging": False, "final": True, "row_status": "final"},
              "resumed record left a staged object or a pending row")
        check(client.inspect("crash-record")["attempts"] == 2, "killed record was not resumed as a second attempt")
        check(read_journal(record_journal) == [
            journal_event("s3", "put", record_row["staging_key"], replay=False),
            journal_event("postgres", "insert", record_row["idempotency_key"], replay=False),
            journal_event("postgres", "insert", record_row["idempotency_key"], replay=True),
            journal_event("s3", "promote", record_row["object_key"], replay=False),
            journal_event("postgres", "finalize", record_row["idempotency_key"], replay=False),
            journal_event("s3", "delete", record_row["staging_key"], existed=True),
        ], "resumed record did not treat the second insert as the same key")
        report["cases"].append("crash-record")
        print("crash-record: killed after the pending row, before promote; "
              "resume promoted that same row", flush=True)

        refused_body, refused_journal = submit("compensate-record", fail_at="record")
        refused_place = placement("compensate-record/upload")
        drive("compensate-record")
        refused = outcome(client, "compensate-record")
        check_world(s3, object_bucket, database_url, expected_objects, sorted_rows(expected_rows))
        assert_output("compensate-record", refused, "compensated", refused_body)
        check(refused["failed_step"] == "record" and refused["error"].startswith("record: database-down:"),
              "insert refusal did not compensate")
        check(refused["row"] is None and refused["promoted"] is None, "refused insert stored a row in the output")
        check(refused["inventory"] == {"staging": False, "final": False, "row_status": None},
              "refused insert left an object or a row")
        check(refused["compensations"]["object"]["absent"] is True
              and refused["compensations"]["row"]["absent"] is True,
              "refused insert did not record the deletes")
        check(set(client.inspect("compensate-record")["steps"]) == {
            "upload", "compensate-object", "compensate-row", "audit"},
              "refused insert committed the record step or skipped compensation")
        check(client.inspect("compensate-record")["attempts"] == 1, "compensation used another attempt")
        check(read_journal(refused_journal) == [
            journal_event("s3", "put", refused_place["staging_key"], replay=False),
            journal_event("s3", "delete", refused_place["staging_key"], existed=True),
            journal_event("s3", "delete", refused_place["object_key"], existed=False),
            journal_event("postgres", "delete", refused_place["idempotency_key"], deleted=0),
        ], "refused insert did not delete the staged object and find no row")
        report["cases"].append("compensate-record")
        print("compensate-record: the insert was refused; the staged object was deleted and no row was left",
              flush=True)

        promote_body, promote_journal = submit("compensate-promote", fail_at="promote")
        promote_place = placement("compensate-promote/upload")
        drive("compensate-promote")
        promote_out = outcome(client, "compensate-promote")
        check_world(s3, object_bucket, database_url, expected_objects, sorted_rows(expected_rows))
        assert_output("compensate-promote", promote_out, "compensated", promote_body)
        check(promote_out["failed_step"] == "promote"
              and promote_out["error"].startswith("promote: promote-refused:"),
              "promote refusal did not compensate")
        check(promote_out["row"]["status"] == "pending", "promote refusal did not see the pending row")
        check(promote_out["inventory"] == {"staging": False, "final": False, "row_status": None},
              "promote refusal left an object or a row")
        check(set(client.inspect("compensate-promote")["steps"]) == {
            "upload", "record", "compensate-object", "compensate-row", "audit"},
              "promote refusal skipped the row or skipped compensation")
        check(read_journal(promote_journal) == [
            journal_event("s3", "put", promote_place["staging_key"], replay=False),
            journal_event("postgres", "insert", promote_place["idempotency_key"], replay=False),
            journal_event("s3", "delete", promote_place["staging_key"], existed=True),
            journal_event("s3", "delete", promote_place["object_key"], existed=False),
            journal_event("postgres", "delete", promote_place["idempotency_key"], deleted=1),
        ], "promote refusal did not delete the staged object and the pending row")
        report["cases"].append("compensate-promote")
        print("compensate-promote: promote was refused; the staged object and the pending row were deleted",
              flush=True)

        check(not any(key.startswith("pending/") for key in expected_objects), "a staged key was treated as final")
        check(all(row["status"] == "final" for row in expected_rows), "a pending row was treated as done")
        check_world(s3, object_bucket, database_url, expected_objects, sorted_rows(expected_rows))
        report["passed"] = True
    except BaseException as error:
        report["passed"] = False
        report["error"] = str(error)
        raise
    finally:
        stop_process(server)
        if server_log is not None and not server_log.closed:
            server_log.close()
        if client is not None:
            client.close()
        try:
            for bucket in created:
                delete_bucket(s3, bucket)
            if database_url is not None:
                drop_database(admin_url, database_name)
                with psycopg.connect(admin_url, connect_timeout=5) as conn:
                    left = conn.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s", (database_name,)
                    ).fetchone()
                check(left is None, "dropped database still exists")
            report["cleaned"] = True
        except Exception as error:
            report["cleaned"] = False
            report["cleanup_error"] = str(error)
            if report.get("passed"):
                raise
        finally:
            evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Object record evidence: {evidence}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=ROOT / "outputs" / "object-record")
    parser.add_argument("--endpoint", default=os.environ.get("DEOOS_ENDPOINT", STORAGE["endpoint"]))
    parser.add_argument("--database-url", default=os.environ.get("OBJECT_RECORD_DATABASE_URL", ADMIN_URL))
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    parsed = parser.parse_args()
    if parsed.worker:
        serve(json.loads(parsed.worker.read_text()))
    else:
        main(parsed)

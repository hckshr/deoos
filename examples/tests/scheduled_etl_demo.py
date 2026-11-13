"""Scheduled ETL on deoos-server: dependency order, one retry, skip-on-overlap."""
import os
import threading
import time
import uuid

import paths
from deoos import Client
from engine_server import create_bucket, delete_bucket, start_deoos_server, stop_process, wait_for_rustfs
from scheduled_etl import (
    AUDIT, EXTRACT, HANDLER, HANDLERS, LOAD, TRANSFORM, pipeline_inputs,
)

ROOT = paths.ROOT
ENDPOINT = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")
INTERVAL_MS = 400
HOLD_MS = 3000


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    wait_for_rustfs(ENDPOINT)
    bucket = "deoos-etl-" + uuid.uuid4().hex[:20]
    prefix = "etl-" + uuid.uuid4().hex[:8]
    directory = ROOT / "outputs" / "scheduled-etl"
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "events.log"
    log_path.unlink(missing_ok=True)
    s3 = create_bucket(ENDPOINT, bucket)
    server = None
    log = open(directory / "deoos-server.log", "w")
    client = None
    worker_client = None
    stop = threading.Event()
    errors = []

    def poll():
        nonlocal worker_client
        worker_client = Client.remote(remote["url"], token=remote["token"])
        while not stop.is_set():
            try:
                worked = worker_client.run_once(HANDLERS)
            except Exception as error:
                errors.append(str(error))
                if stop.wait(0.05):
                    return
                continue
            if not worked:
                stop.wait(0.05)

    try:
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), bucket, prefix, log, ENDPOINT, lease_ms=5000)
        client = Client.remote(remote["url"], token=remote["token"])
        inputs = pipeline_inputs(
            ROOT / "python" / "scheduled-etl" / "orders.json",
            directory, log_path, directory / "attempts", HOLD_MS, True,
        )
        first_due = int(time.time() * 1000) - 50
        client.schedule(
            "daily-orders", HANDLER, inputs, INTERVAL_MS, first_due_ms=first_due,
            missed="latest", overlap="skip", max_attempts=3, retry_ms=200,
        )
        thread = threading.Thread(target=poll, name="etl-worker")
        thread.start()
        deadline = time.monotonic() + 30
        holding = None
        while time.monotonic() < deadline:
            tasks = [task for task in client.list_tasks()["tasks"] if task["handler"] == HANDLER]
            if len(tasks) == 1 and tasks[0]["status"] == "waiting":
                waiting = tasks[0].get("waiting_on") or {}
                if waiting.get("kind") == "timer" and waiting.get("name") == "hold":
                    holding = tasks[0]
                    break
            time.sleep(0.05)
        check(holding is not None, f"pipeline did not reach the hold timer; errors={errors[-3:]}")
        scheduled_at = holding["schedule"]["scheduled_at"]
        snapshot = log_path.read_text(encoding="utf-8")
        time.sleep(1.5)
        tasks = [task for task in client.list_tasks()["tasks"] if task["handler"] == HANDLER]
        schedule = client.inspect_schedule("daily-orders")
        now_ms = int(time.time() * 1000)
        check(len(tasks) == 1, f"overlap skip created another pipeline task: {len(tasks)}")
        check(log_path.read_text(encoding="utf-8") == snapshot, "the active run kept writing during the next intervals")
        check(now_ms > scheduled_at + 2 * INTERVAL_MS, "the overlap window did not cover two intervals")
        check(schedule["next_due_ms"] >= scheduled_at + 2 * INTERVAL_MS,
              f"next_due_ms did not advance past skipped intervals: {schedule['next_due_ms']} vs {scheduled_at}")
        check(schedule["overlap"] == "skip", schedule["overlap"])
        client.pause_schedule("daily-orders")
        deadline = time.monotonic() + 20
        finished = None
        while time.monotonic() < deadline:
            finished = client.inspect(holding["id"])
            if finished["status"] == "completed":
                break
            time.sleep(0.05)
        check(finished["status"] == "completed", f"pipeline did not finish after the hold: {finished['status']} {finished.get('error')}")
        events = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line]
        check(events[:2] == ["extract", "extract"], events)
        check(events[-1] == "load", events)
        check(sorted(events[2:-1]) == ["audit", "transform"], events)
        by_handler = {}
        for task in client.list_tasks()["tasks"]:
            by_handler.setdefault(task["handler"], []).append(task)
        check(len(by_handler[HANDLER]) == 1, by_handler[HANDLER])
        check(len(by_handler[EXTRACT]) == 1 and by_handler[EXTRACT][0]["attempts"] == 2, by_handler[EXTRACT])
        check("pull" in by_handler[EXTRACT][0]["steps"], by_handler[EXTRACT][0]["steps"])
        for name in (TRANSFORM, AUDIT, LOAD):
            check(len(by_handler[name]) == 1 and by_handler[name][0]["status"] == "completed", by_handler[name])
        view = client.view(holding["id"])
        check(view["output"]["rows"] == 2 and view["output"]["cents"] == 350, view["output"])
        check("extract upstream blip" in "\n".join(errors), "the extract retry did not record the upstream blip")
        print("scheduled etl: attempts 2, order extract/extract then audit+transform then load, overlap skipped", flush=True)
    finally:
        stop.set()
        if "thread" in locals():
            thread.join(timeout=10)
        if worker_client is not None:
            worker_client.close()
        if client is not None:
            client.close()
        stop_process(server)
        log.close()
        delete_bucket(s3, bucket)


if __name__ == "__main__":
    main()

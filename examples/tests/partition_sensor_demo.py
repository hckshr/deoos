"""Sensor blocks on a missing RustFS object, survives a dead worker, then loads it."""
import json
import os
import subprocess
import sys
import time
import uuid

import paths
from deoos import Client
from engine_server import (
    create_bucket, delete_bucket, s3_client, server_worker_env, start_deoos_server, stop_process,
    wait_for_rustfs,
)
from partition_sensor import HANDLER, sensor_inputs

ROOT = paths.ROOT
ENDPOINT = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")
KEY = "warehouse/orders/dt=2024-03-15/part.json"
TASK_ID = "sensor-2024-03-15"


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_for(client, predicate, timeout=30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.inspect(TASK_ID)
        if predicate(last):
            return last
        time.sleep(0.05)
    raise TimeoutError(f"timed out; last state status={None if last is None else last.get('status')} waiting={None if last is None else last.get('waiting_on')}")


def start_worker(env, log_file):
    return subprocess.Popen(
        [sys.executable, "python/partition-sensor/partition_sensor.py", "work"],
        cwd=ROOT, env=env, stdout=log_file, stderr=subprocess.STDOUT, text=True,
    )


def main():
    wait_for_rustfs(ENDPOINT)
    engine_bucket = "deoos-sensor-" + uuid.uuid4().hex[:20]
    data_bucket = "deoos-sensor-data-" + uuid.uuid4().hex[:12]
    prefix = "sensor-" + uuid.uuid4().hex[:8]
    directory = ROOT / "outputs" / "partition-sensor"
    directory.mkdir(parents=True, exist_ok=True)
    engine_s3 = create_bucket(ENDPOINT, engine_bucket)
    data = s3_client(ENDPOINT)
    create_bucket(ENDPOINT, data_bucket)
    server = None
    log = open(directory / "deoos-server.log", "w")
    worker_log = open(directory / "worker.log", "w")
    client = None
    worker = None
    try:
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), engine_bucket, prefix, log, ENDPOINT, lease_ms=5000)
        client = Client.remote(remote["url"], token=remote["token"])
        inputs = sensor_inputs(
            ENDPOINT, data_bucket, KEY, "local-development", "local-development-only-secret",
            interval_ms=400, max_pokes=20,
        )
        client.submit(TASK_ID, HANDLER, inputs, max_attempts=1)
        env = server_worker_env()
        env.update(DEOOS_MODE="server", ENGINE_URL=remote["url"], ENGINE_TOKEN=remote["token"])
        worker = start_worker(env, worker_log)
        waiting = wait_for(
            client,
            lambda state: state.get("status") == "waiting" and (state.get("waiting_on") or {}).get("kind") == "timer",
        )
        check(str((waiting.get("waiting_on") or {}).get("name", "")).startswith("wait-"), waiting["waiting_on"])
        check("load" not in waiting["steps"], waiting["steps"])
        check("poke-0" in waiting["steps"], waiting["steps"])
        stop_process(worker)
        worker = None
        still = client.inspect(TASK_ID)
        check(still["status"] == "waiting" and "load" not in still["steps"], still["status"])
        data.put_object(
            Bucket=data_bucket, Key=KEY,
            Body=json.dumps([{"order_id": "a", "cents": 10}, {"order_id": "b", "cents": 25}]).encode(),
        )
        absent = client.inspect(TASK_ID)
        check(absent["status"] == "waiting" and "load" not in absent["steps"], "load ran before a worker saw the object")
        worker = start_worker(env, worker_log)
        finished = wait_for(client, lambda state: state.get("status") in ("completed", "failed"))
        check(finished["status"] == "completed", finished.get("error"))
        view = client.view(TASK_ID)
        check(view["output"] == {"cents": 35, "key": KEY, "rows": 2}, view["output"])
        check("poke-0" in finished["steps"] and "load" in finished["steps"], finished["steps"])
        print("partition sensor: waited on RustFS, stayed waiting with no worker, then loaded 2 rows", flush=True)
    finally:
        stop_process(worker)
        worker_log.close()
        if client is not None:
            client.close()
        stop_process(server)
        log.close()
        delete_bucket(data, data_bucket)
        delete_bucket(engine_s3, engine_bucket)


if __name__ == "__main__":
    main()

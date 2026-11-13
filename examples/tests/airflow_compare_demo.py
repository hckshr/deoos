"""Run the Airflow DAG and the deoos rewrite for the same day and compare output."""
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import paths

ROOT = paths.ROOT
EXAMPLE = ROOT / "python" / "airflow-compare"
ORDERS = EXAMPLE / "orders.csv"
EXPECTED = {"cents": 3500, "day": "2024-03-15", "paid_orders": 2}
CONSTRAINT = "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt"


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def venv_python():
    return ROOT / ".venv-airflow" / "bin" / "python"


def airflow_ready(python):
    if not Path(python).is_file():
        return False
    probe = subprocess.run([str(python), "-c", "import airflow"], capture_output=True, text=True)
    return probe.returncode == 0


def ensure_venv():
    """Re-exec this file inside the Airflow virtualenv. Does not return."""
    python = venv_python()
    if not airflow_ready(python):
        subprocess.run([sys.executable, "-m", "venv", str(python.parent.parent)], check=True)
        subprocess.run(
            [str(python.parent / "pip"), "install", "--disable-pip-version-check",
             "apache-airflow==2.11.2", "--constraint", CONSTRAINT],
            check=True, timeout=900,
        )
    env = os.environ.copy()
    env["AIRFLOW_COMPARE_VENV"] = "1"
    os.execve(str(python), [str(python), str(Path(__file__).resolve()), *sys.argv[1:]], env)


def run_airflow(directory):
    home = directory / "airflow-home"
    home.mkdir(parents=True, exist_ok=True)
    os.environ["AIRFLOW_HOME"] = str(home)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///" + str(home / "airflow.db")
    os.environ["ORDERS_CSV"] = str(ORDERS)
    os.environ["REVENUE_DIR"] = str(directory)
    sys.path.insert(0, str(EXAMPLE))
    from datetime import datetime, timezone
    from airflow.utils.db import upgradedb
    upgradedb()
    import daily_revenue_dag
    logical = datetime(2024, 3, 15, tzinfo=timezone.utc)
    try:
        daily_revenue_dag.dag.test(execution_date=logical)
    except TypeError:
        daily_revenue_dag.dag.test(logical_date=logical)
    payload = json.loads((directory / "airflow.json").read_text(encoding="utf-8"))
    check(payload == EXPECTED, payload)


def run_deoos(directory):
    from deoos import Client
    from engine_server import create_bucket, delete_bucket, start_deoos_server, stop_process, wait_for_rustfs
    from daily_revenue import DAILY, HANDLERS

    endpoint = os.environ.get("DEOOS_ENDPOINT", "http://127.0.0.1:19000")
    wait_for_rustfs(endpoint)
    bucket = "deoos-compare-" + uuid.uuid4().hex[:20]
    prefix = "compare-" + uuid.uuid4().hex[:8]
    s3 = create_bucket(endpoint, bucket)
    server = None
    log = open(directory / "deoos-server.log", "w")
    client = None
    output = directory / "deoos.json"
    try:
        server, remote = start_deoos_server(
            os.environ.get("ENGINE_BINARY"), bucket, prefix, log, endpoint, lease_ms=5000)
        os.environ["DEOOS_MODE"] = "server"
        os.environ["ENGINE_URL"] = remote["url"]
        os.environ["ENGINE_TOKEN"] = remote["token"]
        client = Client.remote(remote["url"], token=remote["token"])
        task_id = "revenue-2024-03-15"
        client.submit(task_id, DAILY, {
            "day": "2024-03-15",
            "orders": str(ORDERS),
            "output": str(output),
        }, max_attempts=2, retry_ms=0)
        deadline = time.monotonic() + 60
        state = None
        while time.monotonic() < deadline:
            state = client.inspect(task_id)
            if state["status"] in ("completed", "failed", "cancelled"):
                break
            try:
                client.run_once(HANDLERS)
            except Exception:
                time.sleep(0.05)
        check(state is not None and state["status"] == "completed", None if state is None else state.get("error"))
        payload = json.loads(output.read_text(encoding="utf-8"))
        check(payload == EXPECTED, payload)
        check(client.view(task_id)["output"] == EXPECTED, client.view(task_id)["output"])
    finally:
        if client is not None:
            client.close()
        stop_process(server)
        log.close()
        delete_bucket(s3, bucket)


def main():
    directory = ROOT / "outputs" / "airflow-compare"
    directory.mkdir(parents=True, exist_ok=True)
    # deoos and boto3 live in the examples virtualenv. Airflow gets its own.
    if os.environ.get("AIRFLOW_COMPARE_VENV") != "1":
        run_deoos(directory)
        ensure_venv()
    run_airflow(directory)
    airflow_payload = json.loads((directory / "airflow.json").read_text(encoding="utf-8"))
    deoos_payload = json.loads((directory / "deoos.json").read_text(encoding="utf-8"))
    check(airflow_payload == deoos_payload == EXPECTED, (airflow_payload, deoos_payload))
    print("airflow compare: both wrote " + json.dumps(EXPECTED, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()

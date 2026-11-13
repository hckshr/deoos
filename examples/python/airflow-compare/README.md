# Daily order revenue: Airflow and deoos

Both jobs read `orders.csv`, keep the paid rows for one logical date, and write the same JSON: the date, the paid order count, and the sum of `cents`. The date under test is 2024-03-15. Paid rows `a` and `b` total 3500 cents. The refund, the pending row, and the March 14 order are excluded.

The Airflow DAG is `daily_revenue_dag.py` (Airflow 2.11). The deoos workflow is `daily_revenue.py`. They do not import each other. The test runs the DAG with `dag.test` and the deoos workflow on `deoos-server`, then compares the files.

## Run Airflow

From the `examples` directory:

```sh
python3 -m venv .venv-airflow
.venv-airflow/bin/pip install 'apache-airflow==2.11.2' \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
export AIRFLOW_HOME="$PWD/outputs/airflow-compare/airflow-home"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export ORDERS_CSV="$PWD/python/airflow-compare/orders.csv"
export REVENUE_DIR="$PWD/outputs/airflow-compare"
.venv-airflow/bin/airflow db migrate
.venv-airflow/bin/python -c 'import sys; sys.path.insert(0, "python/airflow-compare"); from datetime import datetime, timezone; import daily_revenue_dag as dagmod; dagmod.dag.test(execution_date=datetime(2024, 3, 15, tzinfo=timezone.utc))'
```

## Run deoos

With RustFS and `deoos-server` up (`python/server/README.md`):

```sh
. ./.deoos/env.sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
python python/airflow-compare/daily_revenue.py submit \
  --id revenue-2024-03-15 --day 2024-03-15 \
  --orders "$PWD/python/airflow-compare/orders.csv" \
  --output "$PWD/outputs/airflow-compare/deoos.json"
python python/airflow-compare/daily_revenue.py work
```

Stop `work` after inspect shows `completed`. `deoos.json` and `airflow.json` should match.

## What matches

| | Airflow 2.11 | deoos 0.8.0 |
| --- | --- | --- |
| Shape | `extract >> transform >> load` | spawn extract, join, spawn transform, join, spawn load |
| Data between tasks | XCom return values | joined child outputs |
| One logical date | `ds` from the DAG run | `inputs["day"]`, or the occurrence timestamp when the task was scheduled |
| Retries | `retries=1` | `max_attempts=2` |
| One run at a time | `max_active_runs=1` | `overlap="skip"` on a schedule |
| This date's output | `{"cents": 3500, "day": "2024-03-15", "paid_orders": 2}` | the same object |

## What deoos does not have

- **Cron and timezones.** The Airflow schedule is `0 6 * * *`. deoos schedules are `interval_ms` from an anchor timestamp. `schedule_daily` in `daily_revenue.py` uses 86400000 ms and `overlap="skip"`. That is not 06:00, it does not know `America/New_York`, and a DST transition is not a boundary. There is no cron parser in the engine.
- **Calendar catchup.** Airflow `catchup` walks DAG dates. deoos `missed` is `latest` or `catchup` on the interval grid. Backfill is `backfill(start_ms, end_ms)` with `end_ms` in the past and at most 1000 occurrences, run serially when overlap is skip. See [warehouse backfill](../warehouse-backfill/README.md).
- **Sensors and deferrable operators.** Waiting on data is a poke loop you write. See [partition sensor](../partition-sensor/README.md). There is no triggerer.
- **The operator and provider ecosystem.** This DAG is three Python callables. deoos does not ship Airflow operators, datasets/assets, pools, SLAs, or XCom backends.
- **A DAG UI.** `deoos-server` serves `/ui` for task inspect. It is not a grid or graph view of a DAG.
- **Production HA.** There is no highly available scheduler, no metadata database, and no multi-tenant isolation. Workers poll, and the state lives in object storage. The README at the repository root states the same limit.
- **A separate scheduler process.** Due work is admitted when a worker polls. If no worker polls, the interval does not run.

deoos does keep each step's result, so a worker killed after extract does not extract again. That durability is what the other examples in this directory measure. This pair only shows that the business result matches.

## What the test proves

`python tests/airflow_compare_demo.py` installs the Airflow venv if needed, runs `dag.test` for 2024-03-15, runs the deoos workflow for that same date, and asserts the two JSON objects are equal.

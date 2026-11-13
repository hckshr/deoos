# Warehouse backfill

Four historical days are loaded into a local DuckDB file. Each day is one schedule occurrence. The occurrence id is `schedule-` plus the first 32 hex characters of SHA-256 over `{schedule id}/{timestamp}`. Submitting that id again returns the task that is already there.

The day's rows are deleted and inserted in one transaction. If the worker is killed after that commit and before deoos saves the `load` step, the next claim runs the step again and writes the same rows. A second `backfill` of the same range walks the same ids. Completed tasks are not executed again.

This is an interval backfill, not a calendar backfill. The grid is 86400000 milliseconds from 2024-01-01T00:00:00Z. deoos does not accept a cron expression, a timezone, or a DST rule. `end_ms` has to be in the past, and the engine caps one request at 1000 occurrences. Occurrences run one at a time when `overlap` is `skip`.

After the backfill cursor is empty, the schedule's normal interval can still fire. Pause it when the historical range is all you wanted.

## Run

From the `examples` directory, with RustFS up:

```sh
. ./scripts/prepare_local.sh
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=dev-token "$ENGINE_BINARY"
```

In another shell:

```sh
. ./.deoos/env.sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
mkdir -p outputs/warehouse-backfill
python python/warehouse-backfill/warehouse_backfill.py schedule \
  --database "$PWD/outputs/warehouse-backfill/orders.duckdb" \
  --execution-log "$PWD/outputs/warehouse-backfill/executions.log"
python python/warehouse-backfill/warehouse_backfill.py backfill
python python/warehouse-backfill/warehouse_backfill.py work
```

`work` polls until you stop it. Then pause the schedule with the SDK `pause_schedule("orders-daily")` call, or the run will also pick up the latest live interval.

## What the test proves

`python tests/warehouse_backfill_demo.py` kills the worker after 2024-01-02 has been committed and before its checkpoint exists, starts a worker again, then runs the same backfill a second time.

- The table has 12 rows, 3 per day, and no duplicate `order_id`.
- The execution log shows 2024-01-02 twice and the other three days once. The second backfill does not add a line.
- Calling `backfill` again while that cursor is active returns the same range.

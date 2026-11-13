# Scheduled ETL

A daily orders pipeline with a real dependency shape, a retried extract, and a schedule that skips a new interval while the previous occurrence is still active.

```text
extract --+--> transform --+--> load
          +--> audit ------+
```

`extract` reads `orders.json`. `transform` keeps paid rows and `audit` counts them; both wait for extract, and load waits for both. The extract step raises once when `fail_once` is set. deoos reclaims the task after `retry_ms` and runs the step again. The saved steps before the failure are not repeated, because this failure happens on the first step.

The schedule is `overlap=skip` and `missed=latest`. deoos schedules are fixed intervals. There is no cron expression and no timezone. A 24-hour interval is 86400000 milliseconds, not a calendar day, so a DST change is not a schedule boundary. The comparison is in [Airflow and deoos](../airflow-compare/README.md).

`hold_ms` is how the test keeps the occurrence active across later intervals. Leave it at 0 for a run that should finish and allow the next interval.

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
python python/scheduled-etl/scheduled_etl.py schedule \
  --id daily-orders --interval-ms 86400000 \
  --output-dir "$PWD/outputs/scheduled-etl" \
  --log "$PWD/outputs/scheduled-etl/events.log" \
  --attempt-dir "$PWD/outputs/scheduled-etl/attempts"
python python/scheduled-etl/scheduled_etl.py work
```

`work` polls until you stop it. Workers admit due occurrences; nothing else fires the schedule.

## What the test proves

`python tests/scheduled_etl_demo.py` runs this on `deoos-server` and RustFS.

- The event log is `extract`, `extract`, then `audit` and `transform` in either order, then `load`. The second extract is the retry. The extract task's `attempts` is 2.
- Load writes two paid rows, 350 cents, and the audit count matches.
- While that occurrence is still in `hold`, more than one interval elapses, `next_due_ms` moves forward, and no second pipeline task is created.

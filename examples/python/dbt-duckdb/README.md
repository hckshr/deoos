# dbt run and dbt test

deoos runs a self-contained dbt project on DuckDB. The steps are `dbt-seed`, `dbt-run`, and `dbt-test`. `dbt test` includes `assert_amount_positive`. A failing test raises, the step is not saved, and the task goes to `failed` when the attempt budget is exhausted. `dbt-run` stays completed, so the retry runs the test again and does not rebuild the model.

dbt does not roll the model back when a test fails. The task failure is the signal that the build is not acceptable. The DuckDB file still contains the rows `dbt run` wrote.

Install the pinned adapter into the examples virtualenv before the first run:

```sh
.venv/bin/python -m pip install -r tests/requirements-dbt.txt
```

## Run

From the `examples` directory, with RustFS up and `deoos-server` listening:

```sh
. ./.deoos/env.sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
export DBT_DUCKDB_PATH="$PWD/outputs/dbt-duckdb/orders.duckdb"
mkdir -p outputs/dbt-duckdb
python python/dbt-duckdb/dbt_job.py submit \
  --id dbt-orders-ok \
  --database "$DBT_DUCKDB_PATH"
python python/dbt-duckdb/dbt_job.py work
```

Stop `work` after `inspect --id dbt-orders-ok` shows `completed`. The saved `dbt-test` result has return code 0. Add `--negative` on submit to include an amount of -5. That task ends `failed`, and the error text names `assert_amount_positive`.

The project is `project/`. `profiles.yml` reads `DBT_DUCKDB_PATH`. The step sets that variable from the task input, so the worker does not need it in the environment.

## What the test proves

`python tests/dbt_duckdb_demo.py` runs both tasks on `deoos-server`.

- The passing task completes. `stg_orders` has the two seed rows. Each of seed, run, and test executed once.
- The failing task ends `failed` after two test attempts. `dbt-run` executed once and is still in `completed_steps`. `dbt-test` is not. The error includes `assert_amount_positive`, and the table contains the negative amount.

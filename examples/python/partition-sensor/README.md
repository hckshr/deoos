# Partition sensor

Downstream work waits until an object appears in an S3-compatible bucket. The examples stack uses RustFS on `127.0.0.1:19000` with the local development keys. No cloud credentials are required. MinIO, or any other S3 API at that endpoint, is the same shape.

The task pokes with `head_object`. A miss is saved under `poke-0`, `poke-1`, and so on, then the task sleeps on a timer. The worker is not held during the wait. Killing it leaves the task waiting. The next worker resumes the timer, pokes again, and loads the object only after a poke sees it.

This is not an Airflow sensor operator. There is no deferrable trigger or reschedule mode built in. The poke interval and the give-up count are inputs.

## Run

Start RustFS (`docker compose up -d rustfs` from `examples`) and create a data bucket. From `examples`:

```sh
. ./scripts/prepare_local.sh
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=dev-token "$ENGINE_BINARY"
```

In another shell, point the worker at that server and submit the sensor before the object exists:

```sh
. ./.deoos/env.sh
export DEOOS_MODE=server ENGINE_URL=http://127.0.0.1:7331 ENGINE_TOKEN=dev-token
python python/partition-sensor/partition_sensor.py submit \
  --id sensor-2024-03-15 \
  --bucket deoos-data \
  --key warehouse/orders/dt=2024-03-15/part.json \
  --interval-ms 1000
python python/partition-sensor/partition_sensor.py work
```

`inspect` stays on a `wait-*` timer. Put the JSON array in the bucket, for example a list of `{"order_id", "cents"}` objects. The next poke loads it and the task output is the row count and the sum of `cents`.

The access key and secret in the task inputs are the local RustFS credentials. They are stored with the task. Do not point this example at a bucket whose credentials you cannot store in workflow state.

## What the test proves

`python tests/partition_sensor_demo.py` submits the sensor against a bucket that does not contain the key, waits until the task is on the timer, kills the worker, and checks that `load` has not run. It then writes the object and starts a worker. The task completes with 2 rows and 35 cents. `poke-0` remains the saved miss.

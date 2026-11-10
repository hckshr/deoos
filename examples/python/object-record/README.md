# Object record

Uploading a file and inserting the row that points at it is a common pair of writes. Object storage and Postgres cannot share one transaction. This workflow still finishes with either a final object and a final row, or with neither.

The upload step puts the file at `pending/<idempotency key>`. The key comes from `ctx.idempotency_key("upload")`, so a repeated attempt names the same object. The record step inserts a `pending` row with `ON CONFLICT` on that key. Promote copies the bytes to `objects/<idempotency key>`, marks the row `final`, and deletes the staged object. A put, insert, copy, or delete that already landed is safe to run again.

If the insert or the promote is refused, later steps delete the staged object, the final key if it was written, and the row if it is present. Those deletes are ordinary `ctx.step` calls. deoos does not ship a compensation API. Committed steps are stored in the engine bucket and replay after a crash. The task is submitted with `max_attempts=5` and `retry_ms=0`. Killing the worker leaves the attempt leased; the next claim is another attempt of the same task and starts the handler from the top.

The sample file is a few bytes. Engine state lives in a separate bucket on the same RustFS. The worker reads the object-store and Postgres settings from its environment.

This example is Python only.

## Run

```sh
docker compose --profile postgres run --build --rm object-record
```

Postgres is not part of `docker compose up -d`, which starts only RustFS. The profile starts it. The container installs deoos 0.8.0 from the checked release archive and runs the workflow on RustFS, Postgres, and `deoos-server`.

`python tests/object_record.py` runs the same check from a checkout. It starts the Postgres service when nothing is listening on `127.0.0.1:54329`.

## Expected output

```text
recorded: one final object and one final row; the staging key is gone
crash-upload: killed after the staged put, before the row; resume wrote that row once and promoted it
crash-record: killed after the pending row, before promote; resume promoted that same row
compensate-record: the insert was refused; the staged object was deleted and no row was left
compensate-promote: promote was refused; the staged object and the pending row were deleted
```

## What a crash leaves behind

After the staged put returns and before that step commits, the bucket has the staged object and Postgres has no row for the task. Resume puts the same key again, inserts the row once, and promotes it.

After the insert returns and before that step commits, the staged object and a `pending` row both exist. The final key does not. Resume inserts the same key again, then promotes that row.

Both runs finish with one final object and one `final` row, and no staged object. A refused insert finishes with neither. A refused promote finishes with neither, including the pending row that had already been written.

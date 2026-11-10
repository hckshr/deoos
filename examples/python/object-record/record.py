"""Record one uploaded file in Postgres when the two writes cannot share a transaction.

Object storage and Postgres commit separately. This workflow still finishes
with either a final object and a final row, or with neither.

The upload step puts the bytes at ``pending/<idempotency key>``. The key comes
from ``ctx.idempotency_key("upload")``, so a repeated attempt names the same
object. The record step inserts a ``pending`` row with ``ON CONFLICT`` on that
key. The promote step copies the bytes to ``objects/<idempotency key>``, marks
the row ``final``, and deletes the staged object. Each of those writes is safe
to run again: a worker that dies after the effect and before the checkpoint
restarts the handler from the top, replays committed steps, and repeats the
uncommitted one.

A permanent refusal, from ``fail_at`` in this demo or from the same
``RecordFault`` in a real handler, does not use an engine compensation API.
Later steps delete the staged object, the final key if it was written, and the
row if it is present. ``max_attempts`` and ``retry_ms`` belong to the task.
This handler catches the refusal and finishes the task itself.

The worker reads ``OBJECT_ENDPOINT``, ``OBJECT_REGION``,
``OBJECT_ACCESS_KEY_ID``, ``OBJECT_SECRET_ACCESS_KEY``, and ``DATABASE_URL``
from its environment. Task inputs name the file, the bucket, and a journal
file. The journal is evidence of repeated writes. The bucket and the table
are the records that matter.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.parse

import boto3
from botocore.exceptions import ClientError
import psycopg
from psycopg.rows import dict_row

HANDLER = "object-record.v1"
FILE_NAME = "field-notes.txt"
MAX_BYTES = 1_048_576
FAIL_STEPS = ("record", "promote")
SCHEMA = """
CREATE TABLE IF NOT EXISTS object_records (
    idempotency_key text PRIMARY KEY,
    bucket text NOT NULL,
    staging_key text NOT NULL,
    object_key text NOT NULL,
    name text NOT NULL,
    checksum_sha256 text NOT NULL,
    byte_size bigint NOT NULL,
    status text NOT NULL,
    CONSTRAINT object_records_status_chk CHECK (status IN ('pending', 'final')),
    CONSTRAINT object_records_size_chk CHECK (byte_size >= 0)
)
"""


class RecordFault(Exception):
    """A write was refused. The handler compensates instead of retrying it."""

    def __init__(self, step, code, message):
        self.step = step
        self.code = code
        super().__init__(f"{step}: {code}: {message}")


def placement(idempotency_key):
    """Staging and final keys for one upload. Both are derived from the same key."""
    parts = idempotency_key.split("/") if isinstance(idempotency_key, str) else []
    if (len(parts) != 2 or any(part in ("", ".", "..") for part in parts)
            or any("/" in part or "\\" in part for part in parts)):
        raise ValueError("idempotency key must be a task id and a step name")
    return {
        "idempotency_key": idempotency_key,
        "staging_key": "pending/" + idempotency_key,
        "object_key": "objects/" + idempotency_key,
    }


def connect(database_url):
    return psycopg.connect(
        database_url,
        autocommit=False,
        connect_timeout=5,
        application_name="deoos-object-record",
        options="-c statement_timeout=10000",
        row_factory=dict_row,
    )


def ensure_schema(database_url):
    """Create the table. Safe to call on every attempt."""
    with connect(database_url) as conn:
        conn.execute(SCHEMA)


def append_journal(path, event):
    line = (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)


def read_journal(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _missing(error):
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = str(error.response.get("Error", {}).get("Code", ""))
    return status == 404 or code in {"404", "NoSuchKey", "NotFound"}


def _public(row):
    return {
        "idempotency_key": row["idempotency_key"],
        "bucket": row["bucket"],
        "staging_key": row["staging_key"],
        "object_key": row["object_key"],
        "name": row["name"],
        "checksum_sha256": row["checksum_sha256"],
        "byte_size": int(row["byte_size"]),
        "status": row["status"],
    }


def _expect(row, identity):
    for field in ("bucket", "staging_key", "object_key", "name", "checksum_sha256"):
        if row[field] != identity[field]:
            raise RecordFault("record", "conflict", f"stored {field} does not match this upload")
    if int(row["byte_size"]) != identity["byte_size"]:
        raise RecordFault("record", "conflict", "stored byte_size does not match this upload")
    if row["status"] not in ("pending", "final"):
        raise RecordFault("record", "conflict", "stored status is not pending or final")


class Stores:
    """The application bucket and the Postgres table. Not the engine's bucket."""

    def __init__(self, s3, bucket, database_url, journal_path):
        self.s3 = s3
        self.bucket = bucket
        self.database_url = database_url
        self.journal_path = journal_path

    def _journal(self, event):
        append_journal(self.journal_path, event)

    def _read(self, key):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if _missing(error):
                return None
            raise
        return response["Body"].read()

    def upload(self, source_path, identity):
        body = Path(source_path).read_bytes()
        if len(body) > MAX_BYTES:
            raise RecordFault("upload", "too-large", "source file is larger than 1 MiB")
        checksum = hashlib.sha256(body).hexdigest()
        key = identity["staging_key"]
        existing = self._read(key)
        replay = existing == body
        if not replay:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body,
                               ContentType="application/octet-stream")
            stored = self._read(key)
            if stored != body:
                raise RecordFault("upload", "read-back", "staged object did not match the file")
        self._journal({"system": "s3", "op": "put", "key": key, "replay": replay})
        return {
            "staging_key": key,
            "checksum_sha256": checksum,
            "byte_size": len(body),
            "name": identity["name"],
        }

    def upsert_pending(self, identity):
        ensure_schema(self.database_url)
        with connect(self.database_url) as conn:
            inserted = conn.execute(
                """
                INSERT INTO object_records (
                    idempotency_key, bucket, staging_key, object_key, name,
                    checksum_sha256, byte_size, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING idempotency_key, bucket, staging_key, object_key, name,
                          checksum_sha256, byte_size, status
                """,
                (
                    identity["idempotency_key"], identity["bucket"], identity["staging_key"],
                    identity["object_key"], identity["name"], identity["checksum_sha256"],
                    identity["byte_size"],
                ),
            ).fetchone()
            if inserted is not None:
                replay = False
                row = inserted
            else:
                replay = True
                row = conn.execute(
                    """
                    SELECT idempotency_key, bucket, staging_key, object_key, name,
                           checksum_sha256, byte_size, status
                    FROM object_records WHERE idempotency_key = %s
                    """,
                    (identity["idempotency_key"],),
                ).fetchone()
            if row is None:
                raise RecordFault("record", "missing-row", "insert did not land")
            _expect(row, identity)
            stored = _public(row)
        self._journal({
            "system": "postgres", "op": "insert", "key": identity["idempotency_key"], "replay": replay,
        })
        return stored

    def promote(self, identity):
        """Copy the staged bytes, mark the row final, then delete the staged key.

        A crash can repeat any of the three. The final key is the idempotency
        key, the update matches that same key, and deleting a missing staged
        object is a no-op.
        """
        staging_key = identity["staging_key"]
        object_key = identity["object_key"]
        checksum = identity["checksum_sha256"]
        size = identity["byte_size"]
        staged = self._read(staging_key)
        final = self._read(object_key)
        payload = staged if staged is not None else final
        if payload is None:
            raise RecordFault("promote", "missing-object", "staged object is missing")
        if hashlib.sha256(payload).hexdigest() != checksum or len(payload) != size:
            raise RecordFault("promote", "checksum", "object bytes do not match the upload")
        if staged is not None and final != staged:
            self.s3.put_object(Bucket=self.bucket, Key=object_key, Body=staged,
                               ContentType="application/octet-stream")
            final = self._read(object_key)
            if final != staged:
                raise RecordFault("promote", "read-back", "final object did not match the staged bytes")
            replay = False
        else:
            replay = True
        self._journal({"system": "s3", "op": "promote", "key": object_key, "replay": replay})
        self._finalize(identity)
        self._delete_key(staging_key, "promote")
        if self._read(object_key) != payload:
            raise RecordFault("promote", "read-back", "final object changed")
        return {
            "object_key": object_key,
            "checksum_sha256": checksum,
            "byte_size": size,
            "status": "final",
        }

    def _finalize(self, identity):
        ensure_schema(self.database_url)
        with connect(self.database_url) as conn:
            current = conn.execute(
                """
                SELECT idempotency_key, bucket, staging_key, object_key, name,
                       checksum_sha256, byte_size, status
                FROM object_records WHERE idempotency_key = %s FOR UPDATE
                """,
                (identity["idempotency_key"],),
            ).fetchone()
            if current is None:
                raise RecordFault("promote", "missing-row", "pending row is missing")
            _expect(current, identity)
            replay = current["status"] == "final"
            if not replay:
                current = conn.execute(
                    """
                    UPDATE object_records SET status = 'final'
                    WHERE idempotency_key = %s
                    RETURNING idempotency_key, bucket, staging_key, object_key, name,
                              checksum_sha256, byte_size, status
                    """,
                    (identity["idempotency_key"],),
                ).fetchone()
            if current is None or current["status"] != "final":
                raise RecordFault("promote", "read-back", "row was not marked final")
        self._journal({
            "system": "postgres", "op": "finalize", "key": identity["idempotency_key"], "replay": replay,
        })

    def delete_objects(self, staging_key, object_key):
        self._delete_key(staging_key, "compensate-object")
        self._delete_key(object_key, "compensate-object")
        return {"staging_key": staging_key, "object_key": object_key, "absent": True}

    def _delete_key(self, key, step):
        existed = self._read(key) is not None
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if not _missing(error):
                raise
        if self._read(key) is not None:
            raise RecordFault(step, "read-back", "object is still present")
        self._journal({"system": "s3", "op": "delete", "key": key, "existed": existed})

    def delete_row(self, idempotency_key):
        ensure_schema(self.database_url)
        with connect(self.database_url) as conn:
            deleted = conn.execute(
                "DELETE FROM object_records WHERE idempotency_key = %s",
                (idempotency_key,),
            ).rowcount
            left = conn.execute(
                "SELECT 1 FROM object_records WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
            if left is not None:
                raise RecordFault("compensate-row", "read-back", "row is still present")
        self._journal({
            "system": "postgres", "op": "delete", "key": idempotency_key, "deleted": int(deleted),
        })
        return {"idempotency_key": idempotency_key, "absent": True}

    def inventory(self, staging_key, object_key, idempotency_key):
        ensure_schema(self.database_url)
        with connect(self.database_url) as conn:
            row = conn.execute(
                "SELECT status FROM object_records WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
        return {
            "staging": self._read(staging_key) is not None,
            "final": self._read(object_key) is not None,
            "row_status": None if row is None else row["status"],
        }


def _object_client():
    endpoint = os.environ.get("OBJECT_ENDPOINT", "")
    region = os.environ.get("OBJECT_REGION", "")
    access_key = os.environ.get("OBJECT_ACCESS_KEY_ID", "")
    secret_key = os.environ.get("OBJECT_SECRET_ACCESS_KEY", "")
    if not endpoint or not region or not access_key or not secret_key:
        raise ValueError(
            "OBJECT_ENDPOINT, OBJECT_REGION, OBJECT_ACCESS_KEY_ID, and "
            "OBJECT_SECRET_ACCESS_KEY are required"
        )
    parsed = urllib.parse.urlsplit(endpoint)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise ValueError("OBJECT_ENDPOINT must be an HTTP(S) origin without credentials")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def open_stores(spec):
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url.startswith(("postgresql://", "postgres://")):
        raise ValueError("DATABASE_URL must be a postgresql URL")
    return Stores(_object_client(), spec["bucket"], database_url, spec["journal_path"])


def _absolute(value, name):
    if (not isinstance(value, str) or not Path(value).is_absolute() or "\x00" in value
            or len(value) > 4096):
        raise ValueError(f"{name} must be an absolute path")
    return os.path.normpath(value)


def _bucket(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", value):
        raise ValueError("bucket must be a lowercase DNS name")
    return value


def _file_name(value):
    if (not isinstance(value, str) or not 1 <= len(value) <= 128 or not value.isascii()
            or any(not (char.isalnum() or char in "_.-") for char in value)):
        raise ValueError("name must be 1-128 ASCII letters, digits, dot, underscore, or hyphen")
    return value


def validate_inputs(inputs):
    if not isinstance(inputs, dict):
        raise ValueError("inputs must be an object")
    source = _absolute(inputs.get("source_path"), "source_path")
    journal = _absolute(inputs.get("journal_path"), "journal_path")
    if source == journal:
        raise ValueError("journal_path must be distinct from the source file")
    if not Path(source).is_file():
        raise ValueError("source_path must be an existing file")
    if not Path(journal).parent.is_dir():
        raise ValueError("journal_path directory must exist")
    fail_at = inputs.get("fail_at")
    if fail_at is not None and fail_at not in FAIL_STEPS:
        raise ValueError("fail_at must be record, promote, or null")
    return {
        "source_path": source,
        "journal_path": journal,
        "bucket": _bucket(inputs.get("bucket")),
        "name": _file_name(inputs.get("name")),
        "fail_at": fail_at,
    }


def case_inputs(source_path, bucket, journal_path, *, name=FILE_NAME, fail_at=None):
    return validate_inputs({
        "source_path": source_path,
        "bucket": bucket,
        "journal_path": journal_path,
        "name": name,
        "fail_at": fail_at,
    })


def _finish(ctx, stores, identity, staged, row, promoted, compensations, outcome, failure):
    def save():
        document = {
            "outcome": outcome,
            "idempotency_key": identity["idempotency_key"],
            "bucket": identity["bucket"],
            "staging_key": identity["staging_key"],
            "object_key": identity["object_key"],
            "name": identity["name"],
            "checksum_sha256": identity["checksum_sha256"],
            "byte_size": identity["byte_size"],
            "failed_step": None if failure is None else failure.step,
            "error": None if failure is None else str(failure),
            "staged": staged,
            "row": row,
            "promoted": promoted,
            "compensations": compensations,
            "inventory": stores.inventory(
                identity["staging_key"], identity["object_key"], identity["idempotency_key"]),
        }
        ctx.log(f"outcome {outcome} {ctx.task['id']}")
        return document

    return ctx.step("audit", save)


def record_object(ctx, inputs):
    """Upload, insert, and promote. Restarting a worker resumes committed steps."""
    spec = validate_inputs(inputs)
    stores = open_stores(spec)
    place = placement(ctx.idempotency_key("upload"))
    identity = {
        "idempotency_key": place["idempotency_key"],
        "bucket": spec["bucket"],
        "staging_key": place["staging_key"],
        "object_key": place["object_key"],
        "name": spec["name"],
    }
    staged = ctx.step("upload", lambda: stores.upload(spec["source_path"], identity))
    identity = {
        **identity,
        "checksum_sha256": staged["checksum_sha256"],
        "byte_size": staged["byte_size"],
    }

    row = None
    promoted = None
    try:
        def record():
            if spec["fail_at"] == "record":
                raise RecordFault("record", "database-down", "postgres rejected the insert")
            return stores.upsert_pending(identity)

        def promote():
            if spec["fail_at"] == "promote":
                raise RecordFault(
                    "promote", "promote-refused", "promote refused before the final key was written")
            return stores.promote(identity)

        row = ctx.step("record", record)
        promoted = ctx.step("promote", promote)
    except RecordFault as failure:
        compensations = {
            "object": ctx.step(
                "compensate-object",
                lambda: stores.delete_objects(identity["staging_key"], identity["object_key"]),
            ),
            "row": ctx.step(
                "compensate-row",
                lambda: stores.delete_row(identity["idempotency_key"]),
            ),
        }
        return _finish(
            ctx, stores, identity, staged, row, promoted, compensations, "compensated", failure)
    return _finish(ctx, stores, identity, staged, row, promoted, {}, "recorded", None)


HANDLERS = {HANDLER: record_object}

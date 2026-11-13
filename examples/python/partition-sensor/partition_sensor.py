"""Wait until an object exists in an S3-compatible bucket, then load it.

RustFS in the examples compose file is the stand-in. The workflow does not
need a cloud account. Each poke is its own checkpoint, so a saved "missing"
result is not reused for the next look. Between pokes the task sleeps on a
timer and the worker is free.
"""
import argparse
import json
import os
import sys
import time

from deoos import Client

HANDLER = "warehouse.sensor.v1"


def valid_id(value):
    if (not isinstance(value, str) or not value or len(value) > 128
            or value in (".", "..")
            or any(not (char.isascii() and (char.isalnum() or char in "_.-")) for char in value)):
        raise ValueError("ID must use 1-128 ASCII letters, digits, dot, underscore, or hyphen")
    return value


def create_client():
    mode = os.environ.get("DEOOS_MODE", "library")
    if mode == "server":
        url = os.environ.get("ENGINE_URL")
        if not url:
            raise ValueError("ENGINE_URL is required when DEOOS_MODE=server")
        return Client.remote(url, token=os.environ.get("ENGINE_TOKEN"))
    if mode == "library":
        bucket = os.environ.get("DEOOS_STORAGE_BUCKET")
        if not bucket:
            raise ValueError("DEOOS_STORAGE_BUCKET is required in library mode")
        return Client(provider=os.environ.get("DEOOS_STORAGE_PROVIDER", "s3"), bucket=bucket,
                      prefix=os.environ.get("EXECUTION_PREFIX", "deoos"))
    raise ValueError("DEOOS_MODE must be 'library' or 'server'")


def sensor_inputs(endpoint, bucket, key, access_key_id, secret_access_key,
                  region="us-east-1", interval_ms=1000, max_pokes=30):
    if not endpoint.startswith(("http://", "https://")):
        raise ValueError("endpoint must be an HTTP(S) URL")
    if interval_ms < 0 or max_pokes < 1:
        raise ValueError("interval_ms must be >= 0 and max_pokes must be >= 1")
    return {
        "endpoint": endpoint.rstrip("/"),
        "region": region,
        "bucket": bucket,
        "key": key,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
        "interval_ms": interval_ms,
        "max_pokes": max_pokes,
    }


def s3_client(inputs):
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=inputs["endpoint"],
        region_name=inputs["region"],
        aws_access_key_id=inputs["access_key_id"],
        aws_secret_access_key=inputs["secret_access_key"],
        config=Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=10,
            retries={"max_attempts": 2},
            s3={"addressing_style": "path"},
        ),
    )


def object_exists(inputs):
    from botocore.exceptions import ClientError
    try:
        s3_client(inputs).head_object(Bucket=inputs["bucket"], Key=inputs["key"])
    except ClientError as error:
        status = error.response["ResponseMetadata"]["HTTPStatusCode"]
        code = error.response.get("Error", {}).get("Code", "")
        if status == 404 or code in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}:
            return False
        raise
    return True


def read_partition(inputs):
    body = s3_client(inputs).get_object(Bucket=inputs["bucket"], Key=inputs["key"])["Body"].read(1_048_577)
    if len(body) > 1_048_576:
        raise ValueError("partition object is larger than 1048576 bytes")
    rows = json.loads(body)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("partition object must be a JSON array of objects")
    total = 0
    for row in rows:
        cents = row.get("cents")
        if isinstance(cents, bool) or not isinstance(cents, int):
            raise ValueError("each partition row needs an integer cents")
        total += cents
    return {"key": inputs["key"], "rows": len(rows), "cents": total}


def sensor(ctx, inputs):
    interval_ms = inputs["interval_ms"]
    max_pokes = inputs["max_pokes"]
    for poke in range(max_pokes):
        # A new step name each poke. The saved miss must not count as the next look.
        found = ctx.step(f"poke-{poke}", lambda: object_exists(inputs))
        if found:
            return ctx.step("load", lambda: read_partition(inputs))
        if poke + 1 == max_pokes:
            break
        ctx.sleep(f"wait-{poke}", interval_ms)
    raise TimeoutError(f'partition {inputs["key"]} did not appear after {max_pokes} pokes')


HANDLERS = {HANDLER: sensor}


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    submit = actions.add_parser("submit")
    submit.add_argument("--id", required=True)
    submit.add_argument("--endpoint", default=os.environ.get("AWS_ENDPOINT", "http://127.0.0.1:19000"))
    submit.add_argument("--bucket", required=True)
    submit.add_argument("--key", required=True)
    submit.add_argument("--access-key-id", default=os.environ.get("AWS_ACCESS_KEY_ID", "local-development"))
    submit.add_argument("--secret-access-key",
                        default=os.environ.get("AWS_SECRET_ACCESS_KEY", "local-development-only-secret"))
    submit.add_argument("--interval-ms", type=int, default=1000)
    submit.add_argument("--max-pokes", type=int, default=30)
    actions.add_parser("work")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--id", required=True)
    return parser


def main():
    args = make_parser().parse_args()
    client = None
    try:
        client = create_client()
        if args.action == "submit":
            inputs = sensor_inputs(
                args.endpoint, args.bucket, args.key, args.access_key_id, args.secret_access_key,
                interval_ms=args.interval_ms, max_pokes=args.max_pokes,
            )
            result = client.submit(valid_id(args.id), HANDLER, inputs, max_attempts=1)
        elif args.action == "inspect":
            result = client.inspect(valid_id(args.id))
        else:
            result = None
            try:
                while True:
                    try:
                        worked = client.run_once(HANDLERS)
                    except Exception as error:
                        print(str(error), file=sys.stderr)
                        time.sleep(0.05)
                        continue
                    if not worked:
                        time.sleep(0.05)
            except KeyboardInterrupt:
                pass
        if result is not None:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())

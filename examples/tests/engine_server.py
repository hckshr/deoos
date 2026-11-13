"""Start the published deoos-server against local RustFS.

Workers receive only the server URL and token. Storage credentials stay on the
server process. Checksums for that binary are verified by scripts/install_deoos.sh
before ENGINE_BINARY is set.
"""
import os
import socket
import subprocess
import time
import urllib.request
import uuid

import boto3
from botocore.exceptions import ClientError

ACCESS_KEY = "local-development"
SECRET_KEY = "local-development-only-secret"
REGION = "us-east-1"


def wait_for_url(url, timeout=60, headers=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            request = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(request, timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"{url} did not become ready")


def wait_for_rustfs(endpoint, timeout=60):
    wait_for_url(endpoint.rstrip("/") + "/health", timeout=timeout)


def s3_client(endpoint):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


def create_bucket(endpoint, bucket):
    client = s3_client(endpoint)
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
    return client


def delete_bucket(client, bucket):
    """Remove a test bucket. A late write can land between list and delete."""
    last = None
    for _ in range(8):
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as error:
            status = error.response["ResponseMetadata"]["HTTPStatusCode"]
            if status == 404:
                return
            raise
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            objects = [{"Key": row["Key"]} for row in page.get("Contents", [])]
            if objects:
                errors = client.delete_objects(Bucket=bucket, Delete={"Objects": objects}).get("Errors")
                if errors:
                    raise RuntimeError(f"could not delete objects in {bucket}: {errors}")
        try:
            client.delete_bucket(Bucket=bucket)
            return
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code != "BucketNotEmpty":
                raise
            last = error
            time.sleep(0.4)
    raise last


def start_deoos_server(binary, bucket, prefix, log, endpoint, *, lease_ms=5000, token=None):
    """Bind deoos-server to loopback. Return the process and the worker client config."""
    if not binary:
        raise RuntimeError("ENGINE_BINARY is not set. Install deoos with scripts/install_deoos.sh")
    token = token or uuid.uuid4().hex
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("AWS_", "DEOOS_", "GOOGLE_", "AZURE_"))
        and key not in {"ENGINE_BIND", "ENGINE_TOKEN", "ENGINE_URL", "EXECUTION_PREFIX", "LEASE_MS"}
    }
    env.update(
        DEOOS_STORAGE_PROVIDER="s3",
        DEOOS_STORAGE_BUCKET=bucket,
        AWS_ENDPOINT=endpoint,
        AWS_ALLOW_HTTP="true",
        AWS_ACCESS_KEY_ID=ACCESS_KEY,
        AWS_SECRET_ACCESS_KEY=SECRET_KEY,
        AWS_REGION=REGION,
        AWS_EC2_METADATA_DISABLED="true",
        EXECUTION_PREFIX=prefix,
        LEASE_MS=str(lease_ms),
        ENGINE_BIND=f"127.0.0.1:{port}",
        ENGINE_TOKEN=token,
    )
    process = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log)
    url = f"http://127.0.0.1:{port}"
    try:
        wait_for_url(url + "/health", timeout=30, headers={"Authorization": "Bearer " + token})
    except BaseException:
        stop_process(process)
        raise
    if process.poll() is not None:
        raise RuntimeError("deoos-server exited during startup")
    return process, {"url": url, "token": token}


def stop_process(process):
    if process is not None and process.poll() is None:
        process.kill()
        process.wait(timeout=10)


def server_worker_env(base=None):
    """Environment for a worker that must talk only to deoos-server."""
    source = dict(os.environ if base is None else base)
    env = {
        key: value for key, value in source.items()
        if not key.startswith(("AWS_", "DEOOS_STORAGE_", "GOOGLE_", "AZURE_"))
        and key not in {"DEOOS_STORAGE_BUCKET", "DEOOS_STORAGE_PROVIDER", "DEOOS_STORAGE_DIRECTORY",
                        "EXECUTION_PREFIX", "ENGINE_BIND"}
    }
    env["PYTHONUNBUFFERED"] = "1"
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    return env

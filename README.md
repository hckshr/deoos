# DEOOS

<img src="brand/logo.svg" width="56" height="56" alt="DEOOS">

[![Build and test](https://github.com/hckshr/deoos/actions/workflows/build.yml/badge.svg)](https://github.com/hckshr/deoos/actions/workflows/build.yml)

Durable execution on object storage.

Write ordinary functions that recover from failures. DEOOS saves completed steps and retries unfinished work. State lives in your S3-compatible bucket: S3, Azure Blob, GCS, R2, or self-hosted RustFS. There is no database to run.

- Run it inside your app or as one small server
- Python and TypeScript SDKs on a small Rust core
- Timers, signals, schedules, and idempotency keys built in
- See workflow progress from a CLI or web UI, whether you're a person or an agent

## Quick start

Runnable workflows are in [`examples/`](examples/README.md). Docker is the only prerequisite. From that directory:

```sh
docker compose run --build --rm agent-run-resume
```

The container installs the pinned deoos release, starts a worker against local RustFS, and kills that worker after the draft is saved. The restart keeps those model calls and charges with the same idempotency key. The run prints `credits burned without deoos vs with: 3260 vs 1220`. That compose file publishes RustFS on `127.0.0.1:19000`. Stop `make dev` first if that port is already taken.

### Examples

These are the workflows to try first. Every command below runs from the `examples` directory.

| Example | What it shows | Run |
| --- | --- | --- |
| [Agent run](examples/python/agent-run-resume/README.md) | Saved model calls and one charge key across a timeout (3260 credits when each crash starts over, 1220 with the saved steps). | `docker compose run --build --rm agent-run-resume` |
| [Nightly batch](examples/python/nightly-batch-resume/README.md) | 10,000 emails, one send each, after the worker dies mid-batch and a second trigger joins the same task. | `docker compose run --build --rm nightly-batch-resume` |
| [Offline approval](examples/python/offline-approval-ledger/README.md) | An air-gapped approval tied to one transfer, including a kill between the bank call and its checkpoint. | `./scripts/bundle_offline.sh`, then `docker load -i outputs/offline-bundle.tar` and `docker compose -f compose.offline.yaml run --rm offline-approval-ledger` |
| [Multi-arch handoff](examples/python/multi-arch-handoff/README.md) | The same maintenance audit when the next worker is another CPU and the other SDK. | `docker compose up -d rustfs`, then `python tests/multi_arch_handoff_demo.py` |
| [Document index](examples/python/document-index/README.md) | Documents already read stay read, including a crash after the index is published and before that checkpoint commits. | `docker compose run --build --rm document-index` |

Python and TypeScript both run the agent, the nightly batch, the document index, and the multi-arch handoff. The offline approval ledger is Python. TypeScript commands are in each example README.

Other examples: a greeting, an order, a payment replay, a Hacker News snapshot, a DEV article export, a refund, an uploaded file recorded in Postgres, and HTTP integrations. Commands for those are in [`examples/README.md`](examples/README.md).

For your own application, use the wheel or Node package from [Install](#install). These snippets use library mode with your bucket and cloud credentials.

### Python

```python
from deoos import Client

with Client(bucket="my-workflows", region="us-east-1") as engine:
    def greet(ctx, inputs):
        return ctx.step("greeting", lambda: f"Hello, {inputs['name']}!")

    engine.submit("greeting-001", "greet", {"name": "World"})
    engine.run_once({"greet": greet})
    print(engine.view("greeting-001"))
```

### TypeScript

```typescript
import { Client } from "deoos";

const engine = new Client({bucket: "my-workflows", region: "us-east-1"});
await engine.submit("greeting-002", "greet", {name: "World"});
await engine.runOnce({greet: async (ctx, inputs) =>
  ctx.step("greeting", () => `Hello, ${inputs.name}!`)
});
console.log(await engine.view("greeting-002"));
```

The task ID identifies the work. Reusing it with the same definition returns the same task; use a new ID for new work. `run_once` / `runOnce` polls once. Use `run_worker` / `runWorker` for a continuous worker with application-controlled shutdown.

## Install

Release [0.8.0](https://github.com/hckshr/deoos/releases/tag/0.8.0) is the current published build. Python 3.10+ and Node 22+ are required for the SDKs. A Rust compiler is not required.

`deoos` 0.8.0 is published as GitHub release archives. PyPI has no `deoos` package, and the npm registry has no `deoos` package. `pip install deoos` and `npm install deoos` do not install this project. Install the wheel and the Node tarball from the archive that matches your operating system and CPU.

| Archive | Python wheel | Server | Node package |
| --- | --- | --- | --- |
| `deoos-0.8.0-linux-x64.tar.gz` | `deoos-0.8.0-py3-none-manylinux_2_28_x86_64.whl` | `bin/deoos-server` | `node/deoos-0.8.0.tgz` (`os=linux`, `cpu=x64`) |
| `deoos-0.8.0-linux-arm64.tar.gz` | `deoos-0.8.0-py3-none-manylinux_2_28_aarch64.whl` | `bin/deoos-server` | `node/deoos-0.8.0.tgz` (`os=linux`, `cpu=arm64`) |
| `deoos-0.8.0-macos-arm64.tar.gz` | `deoos-0.8.0-py3-none-macosx_11_0_arm64.whl` | `bin/deoos-server` | `node/deoos-0.8.0.tgz` (`os=darwin`, `cpu=arm64`) |
| `deoos-0.8.0-macos-x64.tar.gz` | `deoos-0.8.0-py3-none-macosx_11_0_x86_64.whl` | `bin/deoos-server` | `node/deoos-0.8.0.tgz` (`os=darwin`, `cpu=x64`) |
| `deoos-0.8.0-windows-x64.tar.gz` | `deoos-0.8.0-py3-none-win_amd64.whl` | `bin/deoos-server.exe` | `node/deoos-0.8.0.tgz` (`os=win32`, `cpu=x64`) |

Linux packages need glibc 2.28 or newer. Release 0.8.0 has no Windows arm64 archive. Each Node tarball is built for one platform, and npm rejects a tarball from another operating system or CPU. The archive also contains `LICENSE`, `README.md`, `compose.yaml`, and `SHA256SUMS`. Checksums for the archives are in [`SHA256SUMS`](https://github.com/hckshr/deoos/releases/download/0.8.0/SHA256SUMS). Runnable workflows are in [`examples/`](examples/README.md).

### Linux and macOS

```sh
set -euo pipefail
VERSION=0.8.0
BASE="https://github.com/hckshr/deoos/releases/download/${VERSION}"
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) target=linux-x64 ;;
  Linux-aarch64|Linux-arm64) target=linux-arm64 ;;
  Darwin-arm64) target=macos-arm64 ;;
  Darwin-x86_64) target=macos-x64 ;;
  *) echo "No ${VERSION} build for $(uname -s) $(uname -m). Shipped: linux-x64, linux-arm64, macos-arm64, macos-x64, windows-x64." >&2; exit 1 ;;
esac
archive="deoos-${VERSION}-${target}.tar.gz"
curl -fL --retry 3 -o "$archive" "$BASE/$archive"
curl -fsSL --retry 3 -o SHA256SUMS "$BASE/SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
  grep "  ${archive}\$" SHA256SUMS | sha256sum -c -
else
  grep "  ${archive}\$" SHA256SUMS | shasum -a 256 -c -
fi
rm -rf "deoos-${VERSION}-${target}"
tar -xzf "$archive"
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --disable-pip-version-check "deoos-${VERSION}-${target}/python/"*.whl
"deoos-${VERSION}-${target}/bin/deoos-server" --version
python -m deoos --help
mkdir -p deoos-node-app
(
  cd deoos-node-app
  npm init -y
  npm install "$PWD/../deoos-${VERSION}-${target}/node/deoos-0.8.0.tgz"
  node --input-type=module -e "import { Client } from 'deoos'; console.log(typeof Client)"
)
```

### Windows x64 (PowerShell)

```powershell
$Version = "0.8.0"
$Target = "windows-x64"
$Archive = "deoos-$Version-$Target.tar.gz"
$Base = "https://github.com/hckshr/deoos/releases/download/$Version"
$ErrorActionPreference = "Stop"
Invoke-WebRequest -Uri "$Base/$Archive" -OutFile $Archive
Invoke-WebRequest -Uri "$Base/SHA256SUMS" -OutFile SHA256SUMS
$line = Select-String -Path SHA256SUMS -Pattern ([regex]::Escape($Archive)) | Select-Object -First 1
if (-not $line) { throw "SHA256SUMS has no entry for $Archive" }
$expected = ($line.Line -split '\s+')[0].ToLower()
$actual = (Get-FileHash -Algorithm SHA256 -Path $Archive).Hash.ToLower()
if ($actual -ne $expected) { throw "checksum mismatch for $Archive" }
if (Test-Path "deoos-$Version-$Target") { Remove-Item -Recurse -Force "deoos-$Version-$Target" }
tar -xzf $Archive
python -m venv .venv
.\.venv\Scripts\python -m pip install --disable-pip-version-check (Get-Item "deoos-$Version-$Target\python\*.whl").FullName
& ".\deoos-$Version-$Target\bin\deoos-server.exe" --version
.\.venv\Scripts\python -m deoos --help
New-Item -ItemType Directory -Force deoos-node-app | Out-Null
Push-Location deoos-node-app
npm init -y
npm install (Resolve-Path "..\deoos-$Version-$Target\node\deoos-0.8.0.tgz")
node --input-type=module -e "import { Client } from 'deoos'; console.log(typeof Client)"
Pop-Location
```

The 0.8.0 wheel's CLI commands are `list`, `info`, `mcp`, `inspect`, `history`, `summary`, `explain`, `retry`, `cancel`, `schedule`, `submit`, `signal`, and `feedback`. `library` uses in-process storage and ignores `ENGINE_URL`. `server` requires `ENGINE_URL`. With `DEOOS_MODE` unset, `ENGINE_URL` selects server mode and its absence selects library mode.

## Library mode

The Rust engine runs inside your Python or Node worker. Each worker holds storage credentials and uses the same bucket and execution prefix. No execution server is needed. `DEOOS_MODE=library` selects library mode.

Run cloud workers near their object store, or self-hosted workers beside RustFS. For a local trial on Mac Apple Silicon, an experimental filesystem adapter accepts `provider="filesystem"` and a private local APFS directory.

## Server mode

Run `deoos-server` with storage credentials. Workers connect over HTTP/HTTPS and need only the server URL and token:

```sh
DEOOS_STORAGE_BUCKET=my-workflows AWS_REGION=us-east-1 \
ENGINE_BIND=127.0.0.1:7331 ENGINE_TOKEN=your-token \
/path/to/release/bin/deoos-server
```

```python
engine = Client.remote("http://127.0.0.1:7331", token="your-token")
```

```typescript
const engine = Client.remote("http://127.0.0.1:7331", "your-token");
```

The workflow APIs are the same in both modes. Use HTTPS through a TLS proxy across machines; non-loopback binding requires a token.

## Key concepts

- **Steps:** named checkpoints return saved results on recovery.
- **Retries and leases:** failed or abandoned work can be claimed again, within its attempt budget. Idempotency keys help destinations deduplicate external effects.
- **Timers and signals:** persist a wait and release the worker; polling resumes ready work.
- **Cancellation:** stops future durable commits, not an already dispatched request or running callback.
- **Schedules:** fixed intervals, pause/resume and bounded backfills. Workers poll to admit due work; no separate scheduler service. No cron expressions or timezone/DST rules.
- **Child workflows:** spawn tasks and join their results across workers.
- **CLI and web UI:** inspect progress, read history, retry or cancel. Open `/ui` on a running server; internal details are separate from the default view.
- **MCP:** `python -m deoos mcp` exposes read-only tools by default. `--allow-actions` adds submit, signal, retry, and cancel. `feedback` is always available. See [Agents](#agents).

```sh
python -m deoos inspect greeting-001
python -m deoos history greeting-001
python -m deoos inspect greeting-001 --internal
python -m deoos info
```

`info` shows engine health. In server mode it reads the running server’s counters; library-mode CLI commands create a fresh engine. Counters belong to each engine instance and reset when it is recreated. This repository's CLI reads `DEOOS_MODE`. `library` uses in-process storage and ignores `ENGINE_URL`. `server` requires `ENGINE_URL`. With `DEOOS_MODE` unset, `ENGINE_URL` selects server mode and its absence selects library mode. Runnable workflows for both SDKs are in [`examples/`](examples/README.md).

## Idempotency across long pauses

Completed steps are remembered in your bucket, so replay returns their saved results without calling the API again.
Inside a step, pass `ctx.idempotency_key("charge")` / `ctx.idempotencyKey("charge")` to the external API.
That key stays the same across retries and resumes and covers the gap between the call and its committed result, while the provider retains the key.
After the provider's key window expires, an unfinished step in that gap needs a reconciliation check before calling again.
Use globally unique execution IDs when sharing an external account; step keys do not include the bucket or prefix.
Keep saved workflow state for the full pause. The payment demo in [`examples/python/payment`](examples/python/payment/README.md) replays a saved charge after that window. From the `examples` directory: `docker compose run --build --rm payment-resume`.

## Guarantees and limits

- Recovery restarts a function from the top; committed named steps return their saved results. Effects between checkpoints may repeat, so destinations must enforce `ctx.idempotency_key(name)` / `ctx.idempotencyKey(name)`. Stable task IDs deduplicate submissions only within an execution prefix.
- Workers need synchronized clocks. Lease expiry cannot stop a running callback or dispatched request; conditional writes prevent an expired owner from committing over a replacement owner.
- Keep checkpoint names and handler behavior compatible with active executions. Checkpoint callbacks run sequentially per task; child tasks can run concurrently. Node CPU work must leave time for renewal timers.
- Object-storage requests and retries are bounded per engine, with separate capacity for lease renewals. Overload can return a retryable error; prolonged storage failures can still cause lease expiry. Keep workflow inputs and outputs small; store large datasets separately and pass references.
- There is no production HA, tenant isolation, garbage collection, or application throughput guarantee. There is no backward compatibility or automatic migration. Keep engine and SDK versions matched.
- Retain task state and checkpoint objects while an execution prefix is writable. Stop all writers before retiring a prefix; do not delete live workflow state by age.

## Develop

From a clone of this repository, with Docker and Rust:

```sh
make dev
```

`make dev` starts RustFS from `compose.yaml`, creates the `deoos-dev` bucket, builds `deoos-server`, and listens on `127.0.0.1:7331`. Stdout is the environment to export. The same lines are written to `outputs/dev.env`. `make dev-down` stops the server and RustFS and keeps the volume.

```sh
export ENGINE_URL=http://127.0.0.1:7331
export PYTHONPATH="$PWD/clients/python"
python -m deoos info
python -m deoos submit TASK_ID HANDLER '{"key":"value"}'
python -m deoos signal TASK_ID SIGNAL_NAME true
```

`submit`, `signal`, and `feedback` are in the 0.8.0 CLI. A refused connection prints `ENGINE_UNREACHABLE` (`cannot reach engine at <url>`) and tells you to run `make dev`. Other failures use the same shape: what happened, the likely cause, a concrete fix, a stable code, and `https://github.com/hckshr/deoos/blob/main/docs/errors.md#CODE`. The catalog is [docs/errors.md](docs/errors.md). `AGENTS.md` is the kill-and-resume check for this server. `make test-dev` runs that check against the real engine.

## Agents

Copy [`agents/SKILL.md`](agents/SKILL.md) into a project so a coding agent can start a workflow, read status, send a signal, and resume a failed task without reading the SDK. Point the agent at that file, or register the MCP server:

```json
{
  "mcpServers": {
    "deoos": {
      "command": "python",
      "args": ["-m", "deoos", "mcp", "--allow-actions"],
      "env": {"ENGINE_URL": "http://127.0.0.1:7331", "PYTHONPATH": "clients/python"}
    }
  }
}
```

From a published install, drop `PYTHONPATH` and run the same module from the virtualenv that has the wheel. `--allow-actions` is required for `task_submit`, `task_signal`, and `task_retry`. Without it the server is read-only, plus `feedback`.

`CURSOR_AGENT`, `CLAUDECODE`, `CODEX`, `DEOOS_AGENT=1`, or `--agent` selects agent mode: one JSON line on stdout, progress on stderr, and a `notice` when a list or history is capped instead of a silent cut. `DEOOS_AGENT=0` keeps the human layout. `explain` in agent mode is the summary JSON.

`task_submit` starts a workflow. `task_summary` is the status. `task_signal` sends a signal. `task_retry` resumes a failed or cancelled task; pass the revision from `task_history`. These tools do not run a worker. A worker still has to poll.

When an agent hits a sharp corner:

```sh
python -m deoos feedback --tried "what you tried" --happened "what happened"
```

With `GITHUB_TOKEN` or `GH_TOKEN` that files a GitHub issue in `hckshr/deoos` labeled `agent-feedback`. Without a token it prints a prefilled issue URL. The report is the text you pass, the deoos version, and whether the engine responded. It does not attach workflow payloads or environment variables. Secret-like text is refused unless you pass `--include-secrets`. Optional `--repro` is sent only when you set it. [docs/agent-feedback.md](docs/agent-feedback.md) is the triage path. [evals/fresh-agent/README.md](evals/fresh-agent/README.md) is the repeatable fresh-agent check.

## Docs

- [Examples](examples/README.md): every runnable workflow, including the agent run, nightly batch, offline approval, multi-arch handoff, and document index
- [Error codes](docs/errors.md): every server, CLI, and SDK code, and the fix
- [Agent skill](agents/SKILL.md): how a coding agent starts, inspects, signals, and retries a workflow
- [Agent feedback](docs/agent-feedback.md): how reports are filed and triaged
- [llms.txt](llms.txt): the short map of those pages for agents

## License

Apache-2.0. See LICENSE.

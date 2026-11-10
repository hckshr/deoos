#!/bin/sh
# Behavioral end-to-end tests against the real engine and local RustFS.
set -eu
cd "$(dirname "$0")/.."
if [ ! -f .deoos/env.sh ]; then
  ./scripts/install_deoos.sh
fi
# shellcheck disable=SC1091
. ./.deoos/env.sh
python - <<'PY'
import sys
import urllib.request
try:
    urllib.request.urlopen("http://127.0.0.1:19000/health", timeout=2)
except OSError:
    sys.exit("Start RustFS first: docker compose up -d rustfs")
PY
echo "library hello (in-process engine, RustFS)"
python tests/library_hello.py
echo "server hello (deoos-server, RustFS)"
python tests/server_hello.py
echo "order (deoos-server, RustFS)"
python tests/order_demo.py
echo "HTTP integrations"
python tests/use_cases.py
python tests/payment_resume.py
python tests/counting_house.py
echo "object record (staged upload and a Postgres row)"
python tests/object_record.py
python tests/document_index_demo.py
python tests/devto_etl_demo.py
python tests/hacker_news_demo.py
python tests/hacker_news.py
python tests/nightly_batch_resume_demo.py
python tests/agent_run_resume_demo.py
python tests/offline_approval_ledger_demo.py
python tests/multi_arch_handoff_demo.py
# Drop vendored release archives so a later image build does not upload them as context.
rm -f docker/offline-tarballs/*.tar.gz docker/offline-tarballs/SHA256SUMS

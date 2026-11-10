#!/bin/sh
set -eu

demo=${1:?Choose hacker-news, devto-etl, payment-resume, document-index, counting-house, object-record, nightly-batch-resume, agent-run-resume, offline-approval-ledger, or multi-arch-handoff.}
shift
case "$demo" in
  hacker-news)
    exec python /app/tests/hacker_news_demo.py \
      --endpoint http://rustfs:9000 --directory /app/outputs/hacker-news "$@"
    ;;
  counting-house)
    exec python /app/python/counting-house/demo.py \
      --endpoint http://rustfs:9000 --directory /app/outputs/counting-house "$@"
    ;;
  object-record)
    exec python /app/python/object-record/demo.py \
      --endpoint http://rustfs:9000 \
      --database-url "postgresql://objectrecord:object-record-local-only@postgres:5432/object_record" \
      --directory /app/outputs/object-record "$@"
    ;;
  devto-etl|payment-resume|document-index|nightly-batch-resume|agent-run-resume)
    case "$demo" in
      devto-etl) script=devto_etl_demo.py ;;
      payment-resume) script=payment_resume_demo.py ;;
      document-index) script=document_index_demo.py ;;
      nightly-batch-resume) script=nightly_batch_resume_demo.py ;;
      agent-run-resume) script=agent_run_resume_demo.py ;;
    esac
    exec python "/app/tests/$script" \
      --endpoint http://rustfs:9000 --directory "/app/outputs/$demo" \
      --server "$ENGINE_BINARY" --node-sdk "$DEOOS_NODE_SDK" "$@"
    ;;
  offline-approval-ledger)
    exec python /app/tests/offline_approval_ledger_demo.py --in-container \
      --endpoint http://rustfs:9000 --directory /app/outputs/offline-approval-ledger "$@"
    ;;
  multi-arch-handoff)
    config=${WORKER_CONFIG:?Set WORKER_CONFIG to the worker JSON file.}
    if [ "${WORKER_SDK:-python}" = "typescript" ]; then
      exec node /app/typescript/multi-arch-handoff/maintain.mjs work --config "$config"
    fi
    exec python /app/python/multi-arch-handoff/maintain.py work --config "$config"
    ;;
  *) echo "Unknown demo: $demo" >&2; exit 1 ;;
esac

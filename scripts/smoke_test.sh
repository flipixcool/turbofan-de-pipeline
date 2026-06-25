#!/usr/bin/env bash
set -euo pipefail

ok() {
  printf 'OK: %s\n' "$1"
}

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

if ! docker compose version >/dev/null 2>&1; then
  fail "docker compose is not available"
fi
ok "docker compose is available"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

CLICKHOUSE_USER="${CLICKHOUSE_USER:-user}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-passwd}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-turbofan}"

clickhouse_query() {
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DATABASE" \
    --query "$1"
}

clickhouse_query "SELECT 1" >/dev/null || fail "ClickHouse is not reachable"
ok "ClickHouse is reachable"

raw_count="$(clickhouse_query "SELECT count() FROM raw_data")"
if [[ "$raw_count" -lt 1 ]]; then
  fail "raw_data is empty; wait for producer and consumer, then retry"
fi
ok "raw_data has ${raw_count} rows"

missing_event_id="$(clickhouse_query "SELECT countIf(event_id = '') FROM raw_data")"
if [[ "$missing_event_id" -ne 0 ]]; then
  fail "raw_data contains ${missing_event_id} rows with empty event_id"
fi
ok "all raw_data rows have event_id"

main_stats_count="$(clickhouse_query "SELECT count() FROM main_stats")"
if [[ "$main_stats_count" -lt 1 ]]; then
  printf 'OK: main_stats is empty for now; Airflow may need one 5-minute interval or a manual DAG run.\n'
else
  ok "main_stats has ${main_stats_count} rows"
fi

ok "smoke test completed"

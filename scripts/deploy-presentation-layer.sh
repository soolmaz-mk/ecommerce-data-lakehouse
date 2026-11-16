#!/usr/bin/env bash
# Deploys the gold + presentation layer on the server. Run FROM the repo root
# on the deploy host. Safe to re-run: every step is idempotent.
set -euo pipefail

cd "$(dirname "$0")/.."

require_env() {
    local key="$1" value="$2"
    if grep -qE "^${key}=" .env 2>/dev/null; then
        echo "  .env already has ${key}"
    else
        echo "${key}=${value}" >> .env
        echo "  .env += ${key}"
    fi
}

echo "== 1. preflight =="
test -f .env || { echo "no .env in $(pwd)"; exit 1; }
git log --oneline -1
docker compose ps --format '{{.Service}}\t{{.Status}}' | sort

echo "== 2. .env keys required by the new services =="
require_env MONITORING_RETENTION_HOURS "48"

# Metabase credentials are NOT generated here. This Metabase already has an
# admin account, and provisioning authenticates as it -- inventing a password
# would just fail the login at step 7. They have to be the real ones.
missing=""
grep -qE '^MB_ADMIN_EMAIL=.+'    .env || missing="$missing MB_ADMIN_EMAIL"
grep -qE '^MB_ADMIN_PASSWORD=.+' .env || missing="$missing MB_ADMIN_PASSWORD"
if [ -n "$missing" ]; then
    echo "  .env is missing:$missing"
    echo "  Add the credentials of an existing Metabase admin, then re-run."
    exit 1
fi
echo "  Metabase admin: $(grep '^MB_ADMIN_EMAIL=' .env | cut -d= -f2)"

echo "== 3. confirm which database dbt wrote the OBT tables to =="
set -a; . ./.env; set +a
DBS=$(docker compose exec -T clickhouse clickhouse-client \
        --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
        -q 'SHOW DATABASES' | tr '\n' ' ')
echo "  databases: $DBS"
if echo "$DBS" | grep -qw gold_gold; then GOLD_DB=gold_gold
elif echo "$DBS" | grep -qw gold;    then GOLD_DB=gold
else
    echo "  neither 'gold' nor 'gold_gold' exists -- run the gold_dbt_load DAG first"; exit 1
fi
require_env GOLD_DB "$GOLD_DB"
echo "  using GOLD_DB=$GOLD_DB"

echo "== 4. clickhouse reachable? =="
# ClickHouse is deliberately NOT recreated. Nothing here needs a restart: the
# gold tables already exist, and the monitoring layer is parked. Recreating a
# container that has been healthy for weeks, on a shared box, buys nothing.
docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q 'SELECT 1' >/dev/null \
    && echo "  ok" || { echo "  clickhouse is not answering"; exit 1; }

echo "== 5. rebuild the gold OBTs (product_id, gross, discount, return_lag_days) =="
docker compose exec -T airflow-scheduler bash -lc \
    'cd /opt/airflow/dbt_project && dbt run --profiles-dir . 2>&1 | tail -12'

echo "== 6. provision Metabase (monitoring dashboard excluded by default) =="
docker compose --profile provision run --rm metabase-init

echo "== 7. verify the OBTs actually hold rows =="
docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "
SELECT 'obt_sales' AS t, count() AS rows FROM ${GOLD_DB}.obt_sales
UNION ALL SELECT 'obt_orders', count() FROM ${GOLD_DB}.obt_orders
UNION ALL SELECT 'obt_clickstream', count() FROM ${GOLD_DB}.obt_clickstream
UNION ALL SELECT 'obt_returns', count() FROM ${GOLD_DB}.obt_returns
FORMAT PrettyCompactMonoBlock"

echo
echo "done. Open Metabase and check the 3 dashboards."
echo "The real-time monitoring layer is PARKED -- see metabase/README.md section 4b."

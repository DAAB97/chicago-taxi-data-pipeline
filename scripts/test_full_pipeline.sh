#!/usr/bin/env bash
set -euo pipefail
D=chicago_taxi_medallion; R="manual_test_$(date -u +%Y%m%dT%H%M%SZ)"; docker compose exec -T airflow-scheduler airflow dags trigger "$D" --run-id "$R"
for i in $(seq 1 120); do S=$(docker compose exec -T airflow-scheduler airflow dags state "$D" "$R" 2>/dev/null|tr -d '\r'|tail -n1||true); echo "$i/120: $S"; [ "$S" = success ] && exit 0; [ "$S" = failed ] && exit 1; sleep 10; done; exit 1

#!/bin/bash
set -euo pipefail

echo "[init] Checking DEMO_MODE to restore warehouse data…"
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 1
done

if [ "${DEMO_MODE:-false}" = "true" ]; then
  echo "[init] DEMO_MODE=true — restoring demo warehouse (idempotent via --clean)"
  pg_restore -v --clean --if-exists --no-owner --no-privileges \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    /docker-entrypoint-initdb.d/demo_warehouse.dump
else
  echo "[init] DEMO_MODE=false — skipping demo warehouse restore."
fi
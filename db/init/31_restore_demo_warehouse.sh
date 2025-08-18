#!/bin/bash
set -e

echo "[init] Checking DEMO_MODE to restore warehouse data…"

until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h 127.0.0.1; do sleep 1; done

if [ "${DEMO_MODE}" = "true" ]; then
  echo "[init] Restoring demo warehouse from dump"
  pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" /docker-entrypoint-initdb.d/demo_warehouse.dump
else
  echo "[init] DEMO_MODE=false — skipping demo warehouse restore."
fi
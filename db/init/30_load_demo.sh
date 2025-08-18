#!/bin/bash
set -e

echo "[init] Checking if DEMO_MODE=true for demo data seed…"

# ждём, пока PostgreSQL станет доступен
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h 127.0.0.1; do sleep 1; done

if [ "${DEMO_MODE}" = "true" ]; then
  echo "[init] Loading DEMO seed data into $POSTGRES_DB…"
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/30_seed_data.sql
else
  echo "[init] DEMO_MODE=false — skipping demo seed."
fi
#!/bin/bash
set -euo pipefail

# Выполняется раньше 02_tables.sql благодаря префиксу 00_
if [ "${DEMO_MODE:-false}" = "true" ]; then
  echo "[init] DEMO_MODE=true — disabling 02_tables.sql to avoid DDL conflicts"
  # Переносим файл из папки init, чтобы entrypoint его не исполнил
  mv /docker-entrypoint-initdb.d/02_tables.sql /docker-entrypoint-initdb.d/.disabled_02_tables.sql || true
fi
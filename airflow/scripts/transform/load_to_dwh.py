# Purpose (demo): run SQL script to merge staging -> dwh.
# No Gmail, no external deps. Just executes sql/insert_to_dwh.sql.

import os
from pathlib import Path

import psycopg2
from airflow.utils.log.logging_mixin import LoggingMixin


class LoadToDWH(LoggingMixin):
    """Executes insert_to_dwh.sql against Postgres."""

    def __init__(self):
        super().__init__()
        self.db = {
            "database": os.getenv("DB_NAME", "rail_db"),
            "user": os.getenv("DB_USER", "roman"),
            "password": os.getenv("DB_PASSWORD", "roman"),
            "host": os.getenv("DB_HOST", "postgres"),
            "port": os.getenv("DB_PORT", "5432"),
        }
        self.sql_file = Path(__file__).resolve().parent.parent.parent / "sql" / "insert_to_dwh.sql"

    def run(self):
        if not self.sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {self.sql_file}")
        with psycopg2.connect(**self.db) as conn, conn.cursor() as cur:
            with open(self.sql_file, "r", encoding="utf-8") as f:
                sql = f.read()
            cur.execute(sql)
            conn.commit()
            self.log.info("[DEMO] insert_to_dwh.sql executed successfully.")


if __name__ == "__main__":
    LoadToDWH().run()
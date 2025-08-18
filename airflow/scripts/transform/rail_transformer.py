# Purpose (demo): read local files from RAW_FILES_DIR, normalize columns,
# and bulk-load them into Postgres staging.temp_table via COPY.
# No Gmail and no external services are used.

import os
import io
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from airflow.utils.log.logging_mixin import LoggingMixin


class RailDataETL(LoggingMixin):
    """ETL for demo: files (xlsb/xlsx/csv) -> staging.temp_table."""

    def __init__(self):
        super().__init__()

        # DB connection (matches docker-compose .env)
        self.db = {
            "database": os.getenv("DB_NAME", "rail_db"),
            "user": os.getenv("DB_USER", "roman"),
            "password": os.getenv("DB_PASSWORD", "roman"),
            "host": os.getenv("DB_HOST", "postgres"),
            "port": os.getenv("DB_PORT", "5432"),
        }

        # File locations
        self.src_dir = Path(os.getenv("RAW_FILES_DIR", "/data/raw_demo"))
        self.loaded_dir = Path(os.getenv("LOADED_FILES_DIR", "/data/loaded"))
        self.loaded_dir.mkdir(parents=True, exist_ok=True)

        # Mapping RU → EN
        self.col_map = {
            "Дата отправления": "shipment_date",
            "Номер вагона": "wagon_number",
            "Номер контейнера": "container_number",
            "Номер документа": "document_number",
            "Категория отправки": "shipment_category",
            "Класс груза": "cargo_class",
            "Прогнозная дата прибытия": "estimated_arrival_date",
            "Тоннажность": "tonnage_type",
            "Дата прибытия": "arrival_date",
            "Дата раскредитования": "release_date",
            "Вид перевозки": "transport_type",
            "Код груза": "cargo_code",
            "Груз": "cargo_name",
            "Государство отправления": "departure_country",
            "Станция отправления СНГ": "departure_station_sng",
            "Код станции отправления СНГ": "departure_station_sng_code",
            "Область отправления": "departure_region",
            "Дорога отправления": "departure_railway",
            "Станция отправления РФ": "departure_station_rus",
            "Код станции отправления РФ": "departure_station_rus_code",
            "Грузоотправитель": "sender",
            "Грузоотправитель (ОКПО)": "sender_okpo",
            "Государство назначения": "destination_country",
            "Область назначения": "destination_region",
            "Дорога назначения": "destination_railway",
            "Станция назначения РФ": "destination_station_rus",
            "Код станции назначения РФ": "destination_station_rus_code",
            "Станция назначения СНГ": "destination_station_sng",
            "Код станции назначения СНГ": "destination_station_sng_code",
            "Грузополучатель": "receiver",
            "Грузополучатель (ОКПО)": "receiver_okpo",
            "Род вагона": "wagon_type_group",
            "Тип вагона": "wagon_type",
            "Плательщик": "payer",
            "Собственник": "wagon_owner",
            "Арендатор": "renter",
            "Оператор": "operator",
            "Вагоно-км": "wagon_km",
            "Объем": "volume",
            "Тариф": "tariff"
            }

        # Order = staging.temp_table columns
        self.staging_cols = [
            "shipment_date",
            "wagon_number",
            "container_number",
            "document_number",
            "shipment_category",
            "cargo_class",
            "estimated_arrival_date",
            "tonnage_type",
            "arrival_date",
            "release_date",
            "transport_type",
            "cargo_code",
            "cargo_name",
            "departure_country",
            "departure_station_sng",
            "departure_station_sng_code",
            "departure_region",
            "departure_railway",
            "departure_station_rus",
            "departure_station_rus_code",
            "sender",
            "sender_okpo",
            "destination_country",
            "destination_region",
            "destination_railway",
            "destination_station_rus",
            "destination_station_rus_code",
            "destination_station_sng",
            "destination_station_sng_code",
            "receiver",
            "receiver_okpo",
            "wagon_type_group",
            "wagon_type",
            "payer",
            "wagon_owner",
            "renter",
            "operator",
            "wagon_km",
            "volume",
            "tariff",
            "load_dttm",
            "source_file_name"
            ]

    @staticmethod
    def _read_any(path: Path) -> pd.DataFrame:
        """Read file by extension."""
        if path.suffix.lower() == ".xlsb":
            return pd.read_excel(path, engine="pyxlsb", na_values=["", " "])
        if path.suffix.lower() in (".xlsx", ".xls"):
            return pd.read_excel(path, na_values=["", " "])
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        raise ValueError(f"Unsupported file type: {path}")

    @staticmethod
    def _parse_dates(series: pd.Series) -> pd.Series:
        """Parse Excel serials or normal dates."""
        ser_num = pd.to_numeric(series, errors="coerce")
        base = pd.to_datetime("1899-12-30")
        as_dates = base + pd.to_timedelta(ser_num, unit="D")
        mask = ser_num.isna() & series.notna()
        if mask.any():
            as_dates.loc[mask] = pd.to_datetime(series.loc[mask], errors="coerce", dayfirst=True)
        return pd.to_datetime(as_dates, errors="coerce").dt.normalize()

    def extract_files(self):
        """Find files to process."""
        files = []
        for pat in ("*.xlsb", "*.xlsx", "*.xls", "*.csv"):
            files.extend(self.src_dir.glob(pat))
        return sorted(files)

    def transform(self, df: pd.DataFrame, filename: str) -> pd.DataFrame:
        """Rename, parse, add metadata."""
        df = df.rename(columns={ru: en for ru, en in self.col_map.items() if ru in df.columns})
        for dcol in ("shipment_date", "estimated_arrival_date", "arrival_date", "release_date"):
            if dcol in df.columns:
                df[dcol] = self._parse_dates(df[dcol])
        df["load_dttm"] = datetime.now()
        df["source_file_name"] = filename
        return df[[c for c in self.staging_cols if c in df.columns]]

    def load(self, df: pd.DataFrame):
        """COPY into staging.temp_table."""
        with psycopg2.connect(**self.db) as conn, conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SET search_path TO staging;")
            buf = io.StringIO()
            df.to_csv(buf, sep="\t", header=False, index=False)
            buf.seek(0)
            cur.copy_from(buf, "temp_table", sep="\t", null="", columns=df.columns)
            conn.commit()

    def move_done(self, path: Path):
        shutil.move(str(path), str(self.loaded_dir / path.name))

    def run(self):
        files = self.extract_files()
        if not files:
            self.log.info("[DEMO] No files found.")
            return
        for f in files:
            try:
                self.log.info("[DEMO] Processing %s", f.name)
                df = self._read_any(f)
                df = self.transform(df, f.name)
                self.load(df)
                self.move_done(f)
            except Exception as e:
                self.log.error("Error in file %s: %s", f.name, e)


if __name__ == "__main__":
    RailDataETL().run()
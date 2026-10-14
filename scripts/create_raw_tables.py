
import csv
import os
from pathlib import Path

import psycopg
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Set DATABASE_URL in your environment.
DATABASE_URL = os.environ["DATABASE_URL"]


def create_raw_table(conn, csv_path: Path):
    table_name = csv_path.stem.removeprefix("olist_")
    table_name = table_name.removesuffix("_dataset")

    with csv_path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as file:
        reader = csv.reader(file)
        columns = next(reader)

    if not columns or len(columns) != len(set(columns)):
        raise ValueError(
            f"Invalid or duplicate CSV headers: {csv_path.name}"
        )

    source_columns = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(column))
        for column in columns
    )

    statement = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            source_row_id BIGINT GENERATED ALWAYS
                AS IDENTITY PRIMARY KEY,
            source_file TEXT NOT NULL,
            source_row_number BIGINT NOT NULL,
            {},
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_file, source_row_number)
        )
    """).format(
        sql.Identifier("olist_raw", table_name),
        source_columns,
    )

    with conn.cursor() as cursor:
        cursor.execute(statement)

    print(f"Created or verified olist_raw.{table_name}")


def main():
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {DATA_DIR}"
        )

    with psycopg.connect(DATABASE_URL) as conn:
        for csv_path in csv_files:
            create_raw_table(conn, csv_path)


if __name__ == "__main__":
    main()


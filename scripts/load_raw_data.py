"""Idempotently load original Olist CSV files into PostgreSQL raw tables."""

import csv
import os
from pathlib import Path

import psycopg
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"


def get_table_name(csv_path: Path) -> str:
    name = csv_path.stem.removeprefix("olist_")
    return name.removesuffix("_dataset")


def load_csv(conn: psycopg.Connection, csv_path: Path) -> None:
    table_name = get_table_name(csv_path)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        columns = next(reader)

        if not columns or len(columns) != len(set(columns)):
            raise ValueError(f"Invalid CSV headers: {csv_path.name}")
        if any(not column or column in {
            "source_row_id", "source_file", "source_row_number", "loaded_at"
        } for column in columns):
            raise ValueError(f"Reserved or empty CSV column: {csv_path.name}")

        staging_columns = ["source_file", "source_row_number", *columns]
        column_names = sql.SQL(", ").join(
            sql.Identifier(column) for column in staging_columns
        )

        staging_definitions = sql.SQL(", ").join([
            sql.SQL("source_file TEXT NOT NULL"),
            sql.SQL("source_row_number BIGINT NOT NULL"),
            *(
                sql.SQL("{} TEXT").format(sql.Identifier(column))
                for column in columns
            ),
        ])

        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE TEMP TABLE staging_raw_import ({}) ON COMMIT DROP")
                .format(staging_definitions)
            )

            copy_statement = sql.SQL(
                "COPY staging_raw_import ({}) FROM STDIN"
            ).format(column_names)

            row_count = 0
            with cursor.copy(copy_statement) as copy:
                for row_number, row in enumerate(reader, start=1):
                    if len(row) != len(columns):
                        raise ValueError(
                            f"{csv_path.name} row {row_number}: "
                            f"expected {len(columns)} values, got {len(row)}"
                        )
                    copy.write_row((
                        csv_path.name,
                        row_number,
                        *(value if value != "" else None for value in row),
                    ))
                    row_count += 1

            cursor.execute(
                sql.SQL("""
                    INSERT INTO {} ({})
                    SELECT {} FROM staging_raw_import
                    WHERE TRUE
                    ON CONFLICT (source_file, source_row_number) DO NOTHING
                """).format(
                    sql.Identifier("olist_raw", table_name),
                    column_names,
                    column_names,
                )
            )
            inserted = cursor.rowcount

        print(
            f"{table_name}: source={row_count:,}, "
            f"inserted={inserted:,}, "
            f"already_present={row_count - inserted:,}"
        )


def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    print(f"Found {len(csv_files)} CSV files.")
    with psycopg.connect(database_url, autocommit=True) as conn:
        for csv_path in csv_files:
            with conn.transaction():
                load_csv(conn, csv_path)


if __name__ == "__main__":
    main()


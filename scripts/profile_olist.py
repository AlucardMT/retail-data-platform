
from pathlib import Path

import pandas as pd


# Resolve paths relative to the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"


def profile_csv(file_path: Path) -> None:
    """Print basic profiling information for one CSV file."""

    print(f"\n{'=' * 60}")
    print(f"File: {file_path.name}")
    print(f"{'=' * 60}")

    df = pd.read_csv(file_path, dtype="string")

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumn names:")
    for column in df.columns:
        print(f"  - {column}")

    print("\nMissing values:")
    missing = df.isna().sum()
    print(missing[missing > 0].to_string())

    print(f"\nExact duplicate rows: {df.duplicated().sum():,}")


def main() -> None:
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {DATA_DIR}"
        )

    print(f"Found {len(csv_files)} CSV files.")

    for file_path in csv_files:
        profile_csv(file_path)


if __name__ == "__main__":
    main()


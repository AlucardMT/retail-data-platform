
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

file_path = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_order_reviews_dataset.csv"
)

reviews = pd.read_csv(file_path, dtype="string")

print("Total review rows:", len(reviews))

print(
    "Unique review IDs:",
    reviews["review_id"].nunique(),
)

print(
    "Duplicate review_id values:",
    reviews["review_id"].duplicated().sum(),
)

print(
    "Duplicate (review_id, order_id) combinations:",
    reviews.duplicated(
        subset=["review_id", "order_id"]
    ).sum(),
)

print(
    "Orders with multiple review rows:",
    (reviews.groupby("order_id").size() > 1).sum(),
)

print("\nExamples of repeated review IDs:")

duplicates = reviews[
    reviews.duplicated(
        subset=["review_id"],
        keep=False,
    )
]

print(
    duplicates[
        [
            "review_id",
            "order_id",
            "review_score",
            "review_creation_date",
        ]
    ]
    .sort_values(["review_id", "order_id"])
    .head(20)
    .to_string(index=False)
)


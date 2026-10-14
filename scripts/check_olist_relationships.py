
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw"


FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "categories": "product_category_name_translation.csv",
}


def load_data():
    return {
        name: pd.read_csv(
            DATA_DIR / filename,
            dtype="string",
        )
        for name, filename in FILES.items()
    }


def check_unique(df, columns, table_name):
    duplicates = df.duplicated(
        subset=columns
    ).sum()

    null_keys = df[columns].isna().any(axis=1).sum()

    print(
        f"{table_name}: "
        f"duplicate keys={duplicates:,}, "
        f"null keys={null_keys:,}"
    )


def check_relationship(
    child,
    parent,
    child_key,
    parent_key,
    description,
):
    missing = (
        child.loc[child[child_key].notna(), child_key]
        .isin(parent[parent_key].dropna())
        .eq(False)
        .sum()
    )

    print(f"{description}: {missing:,} orphan rows")


def main():
    data = load_data()

    print("\n=== PRIMARY KEY CHECKS ===")

    check_unique(
        data["customers"],
        ["customer_id"],
        "customers",
    )

    check_unique(
        data["orders"],
        ["order_id"],
        "orders",
    )

    check_unique(
        data["products"],
        ["product_id"],
        "products",
    )

    check_unique(
        data["sellers"],
        ["seller_id"],
        "sellers",
    )

    check_unique(
        data["order_items"],
        ["order_id", "order_item_id"],
        "order_items",
    )

    check_unique(
        data["payments"],
        ["order_id", "payment_sequential"],
        "payments",
    )

    check_unique(
        data["reviews"],
        ["review_id"],
        "reviews",
    )

    check_unique(
        data["categories"],
        ["product_category_name"],
        "categories",
    )

    print("\n=== FOREIGN KEY CHECKS ===")

    check_relationship(
        data["orders"],
        data["customers"],
        "customer_id",
        "customer_id",
        "orders -> customers",
    )

    check_relationship(
        data["order_items"],
        data["orders"],
        "order_id",
        "order_id",
        "order_items -> orders",
    )

    check_relationship(
        data["order_items"],
        data["products"],
        "product_id",
        "product_id",
        "order_items -> products",
    )

    check_relationship(
        data["order_items"],
        data["sellers"],
        "seller_id",
        "seller_id",
        "order_items -> sellers",
    )

    check_relationship(
        data["payments"],
        data["orders"],
        "order_id",
        "order_id",
        "payments -> orders",
    )

    check_relationship(
        data["reviews"],
        data["orders"],
        "order_id",
        "order_id",
        "reviews -> orders",
    )

    print("\n=== CUSTOMER IDENTITY CHECK ===")

    print(
        "Unique customer_id values:",
        data["customers"]["customer_id"].nunique(),
    )

    print(
        "Unique customer_unique_id values:",
        data["customers"]["customer_unique_id"].nunique(),
    )


if __name__ == "__main__":
    main()


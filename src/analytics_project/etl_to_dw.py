"""
P4 - Create and populate the Data Warehouse (DW) for smart-store-abdellah.

This module implements a star schema for business intelligence (BI)
using SQLite as the data warehouse engine.

Workflow:

1. Create the DW folder and SQLite database file.
2. Create the DW schema (dimension + fact tables).
3. Clear any existing records (so the ETL can be re-run safely).
4. Load cleaned data from CSV files in data/prepared/.
5. Insert the data into the DW tables.

Star schema structure:

- Dimension: customer
- Dimension: product
- Fact:      sale

The DW keeps ALL columns from the prepared CSV files
for customers, products, and sales.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


# -------------------------------------------------------------------
# Paths & constants
# -------------------------------------------------------------------

# PROJECT_ROOT points to the top-level project folder (smart-store-abdellah)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
PREPARED_DATA_DIR = DATA_DIR / "prepared"

# Data warehouse directory and database file
DW_DIR = DATA_DIR / "dw"
DW_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DW_DIR / "smart_store_dw.db"


# -------------------------------------------------------------------
# Schema creation
# -------------------------------------------------------------------


def create_schema(cursor: sqlite3.Cursor) -> None:
    """
    Create the Data Warehouse tables if they do not already exist.

    Tables:

    - customer (dimension)
    - product  (dimension)
    - sale     (fact)

    All relevant columns from the prepared CSV files are included.
    """

    # Customer dimension: includes loyalty and contact preference
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customer (
            customer_id              INTEGER PRIMARY KEY,
            name                     TEXT,
            region                   TEXT,
            join_date                TEXT,
            loyalty_points           INTEGER,
            preferred_contact_method TEXT
        )
        """
    )

    # Product dimension: includes price, stock, and supplier
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product (
            product_id     INTEGER PRIMARY KEY,
            product_name   TEXT,
            category       TEXT,
            unit_price     REAL,
            stock_quantity INTEGER,
            supplier_name  TEXT
        )
        """
    )

    # Sales fact table: one row per transaction
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sale (
            sale_id          INTEGER PRIMARY KEY,
            customer_id      INTEGER NOT NULL,
            product_id       INTEGER NOT NULL,
            store_id         INTEGER,
            campaign_id      INTEGER,
            sale_date        TEXT,
            sale_amount      REAL,
            discount_percent REAL,
            payment_type     TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
            FOREIGN KEY (product_id)  REFERENCES product  (product_id)
        )
        """
    )


def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """
    Delete all existing records from the DW tables.

    This makes the ETL process idempotent:
    we can run it multiple times without duplicating data.
    """
    # Delete from fact table first to avoid foreign key issues
    cursor.execute("DELETE FROM sale")
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")


# -------------------------------------------------------------------
# Insert helpers (column mappings reflect your prepared CSV files)
# -------------------------------------------------------------------


def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """
    Insert customer data into the DW customer dimension table.

    Source file:
        data/prepared/customers_prepared.csv

    Expected columns in the CSV:
        - CustomerID
        - Name
        - Region
        - JoinDate
        - LoyaltyPoints
        - PreferredContactMethod

    Columns loaded into the DW:
        - CustomerID            -> customer_id
        - Name                  -> name
        - Region                -> region
        - JoinDate              -> join_date
        - LoyaltyPoints         -> loyalty_points
        - PreferredContactMethod -> preferred_contact_method

    Duplicate CustomerID rows are removed before insertion
    to avoid primary key constraint violations.
    """
    column_mapping = {
        "customer_id": "CustomerID",
        "name": "Name",
        "region": "Region",
        "join_date": "JoinDate",
        "loyalty_points": "LoyaltyPoints",
        "preferred_contact_method": "PreferredContactMethod",
    }

    # Remove duplicate customers (keep first row for each CustomerID)
    customers_df = customers_df.drop_duplicates(subset=["CustomerID"])

    missing = [src for src in column_mapping.values() if src not in customers_df.columns]
    if missing:
        raise ValueError(
            f"Missing columns in customers_prepared.csv: {missing}\n"
            f"Available columns: {list(customers_df.columns)}"
        )

    ordered_df = customers_df[list(column_mapping.values())].rename(
        columns={src: dst for dst, src in column_mapping.items()}
    )

    cursor.executemany(
        """
        INSERT INTO customer (
            customer_id,
            name,
            region,
            join_date,
            loyalty_points,
            preferred_contact_method
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ordered_df.itertuples(index=False, name=None),
    )


def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """
    Insert product data into the DW product dimension table.

    Source file:
        data/prepared/products_prepared.csv

    Expected columns in the CSV:
        - productid
        - productname
        - category
        - unitprice
        - stockquantity
        - suppliername

    Columns loaded into the DW:
        - productid     -> product_id
        - productname   -> product_name
        - category      -> category
        - unitprice     -> unit_price
        - stockquantity -> stock_quantity
        - suppliername  -> supplier_name

    Duplicate productid rows are removed before insertion.
    """
    column_mapping = {
        "product_id": "productid",
        "product_name": "productname",
        "category": "category",
        "unit_price": "unitprice",
        "stock_quantity": "stockquantity",
        "supplier_name": "suppliername",
    }

    # Remove duplicate products (keep first row for each productid)
    products_df = products_df.drop_duplicates(subset=["productid"])

    missing = [src for src in column_mapping.values() if src not in products_df.columns]
    if missing:
        raise ValueError(
            f"Missing columns in products_prepared.csv: {missing}\n"
            f"Available columns: {list(products_df.columns)}"
        )

    ordered_df = products_df[list(column_mapping.values())].rename(
        columns={src: dst for dst, src in column_mapping.items()}
    )

    cursor.executemany(
        """
        INSERT INTO product (
            product_id,
            product_name,
            category,
            unit_price,
            stock_quantity,
            supplier_name
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ordered_df.itertuples(index=False, name=None),
    )


def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """
    Insert sales data into the DW sale fact table.

    Source file:
        data/prepared/sales_prepared.csv

    Expected columns in the CSV:
        - TransactionID
        - SaleDate
        - CustomerID
        - ProductID
        - StoreID
        - CampaignID
        - SaleAmount
        - DiscountPercent
        - PaymentType

    Columns loaded into the DW:
        - TransactionID   -> sale_id
        - CustomerID      -> customer_id
        - ProductID       -> product_id
        - StoreID         -> store_id
        - CampaignID      -> campaign_id
        - SaleDate        -> sale_date
        - SaleAmount      -> sale_amount
        - DiscountPercent -> discount_percent
        - PaymentType     -> payment_type

    Duplicate TransactionID rows are removed before insertion.
    """
    column_mapping = {
        "sale_id": "TransactionID",
        "customer_id": "CustomerID",
        "product_id": "ProductID",
        "store_id": "StoreID",
        "campaign_id": "CampaignID",
        "sale_date": "SaleDate",
        "sale_amount": "SaleAmount",
        "discount_percent": "DiscountPercent",
        "payment_type": "PaymentType",
    }

    # Remove duplicate sales (keep first row for each TransactionID)
    sales_df = sales_df.drop_duplicates(subset=["TransactionID"])

    missing = [src for src in column_mapping.values() if src not in sales_df.columns]
    if missing:
        raise ValueError(
            f"Missing columns in sales_prepared.csv: {missing}\n"
            f"Available columns: {list(sales_df.columns)}"
        )

    ordered_df = sales_df[list(column_mapping.values())].rename(
        columns={src: dst for dst, src in column_mapping.items()}
    )

    cursor.executemany(
        """
        INSERT INTO sale (
            sale_id,
            customer_id,
            product_id,
            store_id,
            campaign_id,
            sale_date,
            sale_amount,
            discount_percent,
            payment_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ordered_df.itertuples(index=False, name=None),
    )


# -------------------------------------------------------------------
# Main ETL function
# -------------------------------------------------------------------


def load_data_to_db() -> None:
    """
    Run the full ETL process for the Data Warehouse.

    Steps:
    1. Connect to the SQLite DW database (creates the file if needed).
    2. Create the DW schema (dimension + fact tables).
    3. Clear existing records from the DW tables.
    4. Load prepared CSV files into pandas DataFrames.
    5. Insert records into the DW tables.

    This function is designed to be safe to run multiple times:
    each run replaces the existing DW data with a fresh load
    from the prepared CSV files.
    """
    print(f"📁 Using DW database at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()

        print("🧱 Creating schema...")
        create_schema(cursor)

        print("🧹 Clearing existing records...")
        delete_existing_records(cursor)

        print("📥 Loading prepared CSV files...")
        customers_df = pd.read_csv(PREPARED_DATA_DIR / "customers_prepared.csv")
        products_df = pd.read_csv(PREPARED_DATA_DIR / "products_prepared.csv")
        sales_df = pd.read_csv(PREPARED_DATA_DIR / "sales_prepared.csv")

        print("📌 Inserting customers...")
        insert_customers(customers_df, cursor)

        print("📌 Inserting products...")
        insert_products(products_df, cursor)

        print("📌 Inserting sales...")
        insert_sales(sales_df, cursor)

        conn.commit()
        print("✅ ETL complete: Data Warehouse populated successfully.")

    finally:
        conn.close()


# -------------------------------------------------------------------
# Script entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    load_data_to_db()

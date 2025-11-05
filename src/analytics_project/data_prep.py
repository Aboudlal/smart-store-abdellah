"""Module 3: Data Preparation Script using the DataScrubber class.

File: src/analytics_project/data_prep.py.

This script demonstrates the reusable approach to data cleaning by utilizing
the DataScrubber class to process raw customer, product, and sales data
before it is loaded into the Data Warehouse.
"""

import pathlib
import pandas as pd
from .utils_logger import init_logger, logger, project_root

# Import the completed DataScrubber class from the new path src/utils/
from ..utils.data_scrubber import DataScrubber
from typing import Dict, Any

# Set up paths as constants
DATA_DIR: pathlib.Path = project_root.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")


# --- Data Configuration (Based on the provided structure) ---
# These mappings define how we want the data to be cleaned and formatted.

# Target data type mappings for each file
CUSTOMER_TYPE_MAPPING: Dict[str, Any] = {
    'CustomerID': 'Int64',  # Using Int64 to support NaN if needed
    'LoyaltyPoints': 'Int64',
}

PRODUCT_TYPE_MAPPING: Dict[str, Any] = {
    'UnitPrice': 'float',
    'StockQuantity': 'Int64',
}

SALE_TYPE_MAPPING: Dict[str, Any] = {
    'TransactionID': 'Int64',
    'CustomerID': 'Int64',
    'ProductID': 'object',  # Assume object/str for the product code
    'SaleAmount': 'float',
    'DiscountPercent': 'float',
}


def read_data(path: pathlib.Path) -> pd.DataFrame:
    """Read a CSV at the given path into a DataFrame, with error handling."""
    try:
        logger.info(f"Reading raw data from {path}.")
        # Reading with specific parameters if separators are not commas
        df = pd.read_csv(path)
        logger.info(
            f"{path.name}: loaded DataFrame with shape {df.shape[0]} rows x {df.shape[1]} cols"
        )
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, filename: str) -> None:
    """Save the cleaned DataFrame to the prepared data directory."""
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    save_path = PREPARED_DATA_DIR.joinpath(filename)

    try:
        df.to_csv(save_path, index=False)
        logger.info(f"Successfully saved prepared data to {save_path.name}")
    except Exception as e:
        logger.error(f"Error saving data to {save_path.name}: {e}")


def clean_customer_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning logic specific to the customer data."""
    if df_raw.empty:
        return df_raw

    logger.info("  -> Cleaning Customer Data...")
    scrubber = DataScrubber(df_raw)

    # Step 1: Remove duplicates
    scrubber.remove_duplicate_records()

    # Step 2: Handle missing values
    # Fill missing strings with 'N/A' (Not Available)
    scrubber.handle_missing_data(fill_value='N/A')

    # Step 3: Normalize strings (lowercase and trim whitespace)
    for col in ['Name', 'Region', 'PreferredContactMethod']:
        scrubber.format_column_strings_to_lower_and_trim(col)

    # Step 4: Convert types (dates and numeric)
    # FIX: Use direct pd.to_datetime with errors='coerce' to handle bad date strings (like month 13)
    scrubber.df['JoinDate'] = pd.to_datetime(scrubber.df['JoinDate'], errors='coerce')
    for col, dtype in CUSTOMER_TYPE_MAPPING.items():
        # Use convert_column_to_new_data_type for other types
        scrubber.convert_column_to_new_data_type(col, dtype)

    logger.info(f"  -> Customer data cleaned. Final shape: {scrubber.df.shape}")
    return scrubber.df


def clean_product_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning logic specific to the product data."""
    if df_raw.empty:
        return df_raw

    logger.info("  -> Cleaning Product Data...")
    scrubber = DataScrubber(df_raw)

    # Step 1: Remove duplicates
    scrubber.remove_duplicate_records()

    # Step 2: Handle missing values
    # For missing prices and quantities, we replace them with 0
    scrubber.df['UnitPrice'] = scrubber.df['UnitPrice'].fillna(0.0)
    scrubber.df['StockQuantity'] = scrubber.df['StockQuantity'].fillna(0)

    # Fill remaining missing strings with 'N/A'
    scrubber.handle_missing_data(fill_value='N/A')

    # Step 3: Normalize strings
    for col in ['ProductName', 'Category', 'SupplierName']:
        scrubber.format_column_strings_to_lower_and_trim(col)

    # Step 4: Convert types
    for col, dtype in PRODUCT_TYPE_MAPPING.items():
        scrubber.convert_column_to_new_data_type(col, dtype)

    logger.info(f"  -> Product data cleaned. Final shape: {scrubber.df.shape}")
    return scrubber.df


def clean_sales_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning logic specific to the sales data."""
    if df_raw.empty:
        return df_raw

    logger.info("  -> Cleaning Sales Data...")
    scrubber = DataScrubber(df_raw)

    # Step 1: Remove duplicates
    scrubber.remove_duplicate_records()

    # --- FIX for ValueError: 'could not convert string to float: '?' ---
    # We must explicitly replace any non-numeric garbage in the float columns with NaN
    # before we attempt the float conversion. This handles '?' or other bad strings.
    numeric_cols_to_clean = ['SaleAmount', 'DiscountPercent']
    for col in numeric_cols_to_clean:
        # Use regex to replace anything that is NOT a number, dot, or minus sign with NaN
        scrubber.df[col] = scrubber.df[col].replace(r'[^0-9\.\-]', pd.NA, regex=True)

    # Step 2: Handle missing values
    # Drop rows where SaleAmount is missing (critical data)
    scrubber.df.dropna(subset=['SaleAmount'], inplace=True)
    # Fill the rest with 0 for numerics and 'N/A' for strings
    # This step now works safely because garbage strings were converted to NaN above.
    scrubber.df['DiscountPercent'] = scrubber.df['DiscountPercent'].fillna(0.0)
    scrubber.df['CampaignID'] = scrubber.df['CampaignID'].fillna('N/A')

    # Step 3: Normalize strings
    scrubber.format_column_strings_to_lower_and_trim('PaymentType')

    # Step 4: Convert types
    # FIX: Use direct pd.to_datetime with errors='coerce' to handle bad date strings (like month 13)
    scrubber.df['SaleDate'] = pd.to_datetime(scrubber.df['SaleDate'], errors='coerce')
    for col, dtype in SALE_TYPE_MAPPING.items():
        # The conversion to float will now succeed as only numeric strings or NaNs remain
        scrubber.convert_column_to_new_data_type(col, dtype)

    logger.info(f"  -> Sales data cleaned. Final shape: {scrubber.df.shape}")
    return scrubber.df


# --- Main Pipeline Function ---


def main() -> None:
    """Process raw data using the DataScrubber and save the prepared data."""
    logger.info("Starting comprehensive data preparation using DataScrubber...")

    # Define paths and file names
    files_to_process = [
        ("customers_data.csv", "customers_prepared.csv", clean_customer_data),
        ("products_data.csv", "products_prepared.csv", clean_product_data),
        ("sales_data.csv", "sales_prepared.csv", clean_sales_data),
    ]

    for raw_filename, prepared_filename, clean_function in files_to_process:
        raw_path = RAW_DATA_DIR.joinpath(raw_filename)

        # 1. Read
        df_raw = read_data(raw_path)

        if not df_raw.empty:
            # 2. Clean
            df_cleaned = clean_function(df_raw)

            # 3. Save
            save_prepared_data(df_cleaned, prepared_filename)

    logger.info("All data preparation complete. Files are ready in data/prepared.")


if __name__ == "__main__":
    # Initialize logger
    init_logger()
    # Call the main function
    main()

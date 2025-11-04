"""
scripts/data_preparation/prepare_sales.py

This script reads data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
from utils.logger import logger

# Optional: Use a data_scrubber module for common data cleaning tasks
from utils.data_scrubber import DataScrubber


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
SRC_DIR: pathlib.Path = SCRIPTS_DIR.parent
PROJECT_ROOT: pathlib.Path = SRC_DIR.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data


# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows. TransactionID should be unique for sales.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial = len(df)
    if 'TransactionID' in df.columns:
        df = df.drop_duplicates(subset=['TransactionID'])
    else:
        df = df.drop_duplicates()
    logger.info(f"Removed {initial - len(df)} duplicate rows; remaining {len(df)}")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values and coerce column types for sales data.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Normalize common placeholders to NaN
    df.replace({"NULL": pd.NA, "null": pd.NA, "": pd.NA, "NaN": pd.NA}, inplace=True)

    # TransactionID must exist
    if 'TransactionID' in df.columns:
        before = len(df)
        df.dropna(subset=['TransactionID'], inplace=True)
        logger.info(f"Dropped {before - len(df)} rows missing TransactionID")

    # Parse dates
    if 'SaleDate' in df.columns:
        df['SaleDate'] = pd.to_datetime(df['SaleDate'], errors='coerce')
        dropped = df['SaleDate'].isna().sum()
        if dropped:
            logger.info(f"Found {dropped} invalid SaleDate values; dropping those rows")
            df = df.dropna(subset=['SaleDate'])

    # Coerce numeric columns
    if 'DiscountPercent' in df.columns:
        df['DiscountPercent'] = pd.to_numeric(
            df['DiscountPercent'].astype(str).str.replace('%', '', regex=False), errors='coerce'
        )
    for col in [
        'SaleAmount',
        'DiscountPercent',
        'CustomerID',
        'ProductID',
        'StoreID',
        'CampaignID',
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill text columns
    if 'PaymentType' in df.columns:
        mode_pay = (
            df['PaymentType'].mode().iloc[0] if not df['PaymentType'].mode().empty else 'Unknown'
        )
        df['PaymentType'].fillna(mode_pay, inplace=True)

    # Fill numeric columns
    if 'SaleAmount' in df.columns:
        df['SaleAmount'].fillna(df['SaleAmount'].median(), inplace=True)
    if 'DiscountPercent' in df.columns:
        df['DiscountPercent'].fillna(0, inplace=True)  # assume no discount when missing

    logger.info("Missing values handled")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers using IQR and simple business limits.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial = len(df)

    # IQR filter for key numeric columns
    for col in ['SaleAmount', 'DiscountPercent']:
        if col in df.columns:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            df = df[(df[col] >= lower) & (df[col] <= upper)]
            logger.info(f"IQR filter on {col}: kept within [{lower}, {upper}]")

    # Business guardrails
    if 'SaleAmount' in df.columns:
        df = df[(df['SaleAmount'] >= 0) & (df['SaleAmount'] <= 10000)]
    if 'DiscountPercent' in df.columns:
        df = df[(df['DiscountPercent'] >= 0) & (df['DiscountPercent'] <= 100)]

    logger.info(f"Removed {initial - len(df)} outlier rows; remaining {len(df)}")
    return df


def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")

    logger.info(f"Column datatypes: \n{df.dtypes}")
    logger.info(f"Number of unique values: \n{df.nunique()}")

    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """
    Main function for processing data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df.shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()

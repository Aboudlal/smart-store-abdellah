"""
scripts/data_preparation/prepare_products.py

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

    # OPTIONAL: basic profiling to understand the dataset
    logger.info(f"Column dtypes:\n{df.dtypes}")
    logger.info(f"Unique values per column:\n{df.nunique()}")
    missing_pct = (df.isna().mean() * 100).round(2)
    logger.info(f"Missing values (%):\n{missing_pct}")
    return df


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)

    df = df.drop_duplicates(subset=['productid'])

    df = df.drop_duplicates()

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Log missing values by column before handling
    # NA means missing or "not a number" - ask your AI for details
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")

    # Handle missing values appropriately for the products dataset

    # Replace placeholder strings with real NaN values
    df.replace({"NULL": pd.NA, "null": pd.NA, "": pd.NA, "NaN": pd.NA}, inplace=True)

    # Drop rows missing the key identifier
    if 'ProductID' in df.columns:
        df.dropna(subset=['ProductID'], inplace=True)

    # Fill missing text columns with defaults or most frequent values
    if 'ProductName' in df.columns:
        df['ProductName'].fillna('Unknown Product', inplace=True)

    if 'Category' in df.columns:
        mode_category = (
            df['Category'].mode().iloc[0] if not df['Category'].mode().empty else 'uncategorized'
        )
        df['Category'].fillna(mode_category, inplace=True)

    if 'SupplierName' in df.columns:
        mode_supplier = (
            df['SupplierName'].mode().iloc[0] if not df['SupplierName'].mode().empty else 'unknown'
        )
        df['SupplierName'].fillna(mode_supplier, inplace=True)

    # Fill numeric columns with median values to maintain reasonable distribution
    for col in ['UnitPrice', 'StockQuantity']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col].fillna(df[col].median(), inplace=True)

    # Log missing values by column after handling
    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds.
    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    # Identify numeric columns to check
    numeric_cols = [c for c in ['unitprice', 'stockquantity'] if c in df.columns]

    # Coerce to numeric and drop rows that cannot be converted
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=numeric_cols)

    # IQR filter per column
    for col in numeric_cols:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df = df[(df[col] >= lower) & (df[col] <= upper)]
        logger.info(f"Applied IQR outlier removal to {col}: bounds [{lower}, {upper}]")

    # Business limits (adjust if your data requires)
    if 'unitprice' in df.columns:
        df = df[(df['unitprice'] >= 0) & (df['unitprice'] <= 2000)]
    if 'stockquantity' in df.columns:
        df = df[(df['stockquantity'] >= 0) & (df['stockquantity'] <= 1000)]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the formatting of various columns.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with standardized formatting.
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    # Trim whitespace in all string columns
    str_cols = df.select_dtypes(include='object').columns.tolist()
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip()

    # Consistent casing
    if 'productname' in df.columns:
        df['productname'] = df['productname'].str.title()
    if 'category' in df.columns:
        df['category'] = df['category'].str.lower()
    if 'suppliername' in df.columns:
        df['suppliername'] = df['suppliername'].str.title()

    # Numeric formatting
    if 'unitprice' in df.columns:
        df['unitprice'] = pd.to_numeric(df['unitprice'], errors='coerce').round(2)
    if 'stockquantity' in df.columns:
        df['stockquantity'] = pd.to_numeric(df['stockquantity'], errors='coerce').astype('Int64')

    # Ensure productid is integer-like when possible
    if 'productid' in df.columns:
        df['productid'] = pd.to_numeric(df['productid'], errors='coerce').astype('Int64')

    logger.info("Completed standardizing formats")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data against business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Validated DataFrame.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")

    # productid must be present, positive, and unique
    if 'productid' in df.columns:
        before = len(df)
        df = df.dropna(subset=['productid'])
        df = df[df['productid'] > 0]
        df = df.drop_duplicates(subset=['productid'])
        logger.info(f"Validated productid: removed {before - len(df)} invalid/duplicate rows")

    # unitprice and stockquantity must be non-negative
    if 'unitprice' in df.columns:
        bad = (df['unitprice'] < 0).sum()
        if bad:
            logger.info(f"Found {bad} rows with negative unitprice; removing")
        df = df[df['unitprice'] >= 0]

    if 'stockquantity' in df.columns:
        bad = (df['stockquantity'] < 0).sum()
        if bad:
            logger.info(f"Found {bad} rows with negative stockquantity; removing")
        df = df[df['stockquantity'] >= 0]

    logger.info("Data validation complete")
    return df


def main() -> None:
    """
    Main function for processing product data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

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

    # Validate data
    df = validate_data(df)

    # Standardize formats
    df = standardize_formats(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("==================================")


# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()

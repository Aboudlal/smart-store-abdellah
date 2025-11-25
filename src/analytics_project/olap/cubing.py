import pandas as pd
import pathlib
import sqlite3
from loguru import logger  # Assurez-vous que loguru est installé

# --- 1. Configuration des Chemins ---
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR: pathlib.Path = THIS_DIR.parent
SRC_DIR: pathlib.Path = PACKAGE_DIR.parent
PROJECT_ROOT_DIR: pathlib.Path = SRC_DIR.parent

DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "dw"
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "smart_store_dw.db"
OLAP_OUTPUT_DIR: pathlib.Path = DATA_DIR / "olap_cubing_outputs"

# Crée le répertoire de sortie s'il n'existe pas
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Configuration de loguru (peut être simplifié si vous avez déjà un fichier utils_logger.py)
logger.remove()
logger.add(
    lambda msg: print(msg, end=''),
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    colorize=True,
)
# --- Fin Configuration ---


def ingest_sales_data_from_dw() -> pd.DataFrame:
    """
    Ingest sale fact table data from SQLite data warehouse.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        sales_df = pd.read_sql_query("SELECT * FROM sale", conn)
        conn.close()
        logger.info("Sales data successfully loaded from SQLite data warehouse.")
        return sales_df
    except sqlite3.OperationalError as e:
        logger.error(f"Erreur de connexion/lecture de la DB : {e}. Vérifiez le chemin {DB_PATH}.")
        raise
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données de vente : {e}")
        raise


def ingest_dim_table(table_name: str) -> pd.DataFrame:
    """
    Ingest a dimension table (e.g., product or customer) from the DW.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        logger.info(f"{table_name} data successfully loaded.")
        return df
    except Exception as e:
        logger.error(f"Error loading {table_name} table data: {e}")
        raise


def generate_column_names(dimensions: list, metrics: dict) -> list:
    """
    Generate explicit column names for OLAP cube based on dimensions and metrics.
    """
    column_names = dimensions.copy()
    for column, agg_funcs in metrics.items():
        if isinstance(agg_funcs, list):
            for func in agg_funcs:
                column_names.append(f"{column}_{func}")
        else:
            column_names.append(f"{column}_{agg_funcs}")

    # Nettoyage des doubles underscores si nécessaire
    column_names = [col.replace('__', '_').rstrip("_") for col in column_names]

    logger.info(f"Generated column names for OLAP cube: {column_names}")
    return column_names


def create_olap_cube(data_df: pd.DataFrame, dimensions: list, metrics: dict) -> pd.DataFrame:
    """
    Create an OLAP cube by aggregating data across multiple dimensions.
    """
    if data_df.empty:
        logger.warning("Input DataFrame is empty, cannot create cube.")
        return pd.DataFrame()

    try:
        grouped = data_df.groupby(dimensions, dropna=True)
        cube = grouped.agg(metrics).reset_index()

        explicit_columns = generate_column_names(dimensions, metrics)
        cube.columns = explicit_columns

        logger.info(f"OLAP cube created with dimensions: {dimensions}")
        return cube
    except KeyError as e:
        logger.error(
            f"KeyError: One or more dimensions/metrics ({dimensions}, {metrics.keys()}) not found in the DataFrame: {e}"
        )
        raise


def write_cube_to_csv(cube: pd.DataFrame, filename: str) -> None:
    """
    Write the OLAP cube to a CSV file.
    """
    output_path = OLAP_OUTPUT_DIR.joinpath(filename)
    cube.to_csv(output_path, index=False)
    logger.info(f"OLAP cube saved to {output_path}.")


def main():
    """
    Execute OLAP cubing process for Category and Region Profitability (P6 Goal).
    """
    logger.info("Starting OLAP Cubing process for P6 Goal (Category & Region Profitability)...")

    # Step 1: Ingest all necessary data (Fact and Dimensions)
    sales_df = ingest_sales_data_from_dw()
    product_df = ingest_dim_table("product")
    customer_df = ingest_dim_table("customer")

    # Step 2: Join tables (Create the Data Mart required for the cube)
    # Goal: Join sale, product (for category), and customer (for region)
    merged_df = pd.merge(
        sales_df, product_df[['product_id', 'category']], on="product_id", how="left"
    )
    final_df = pd.merge(
        merged_df, customer_df[['customer_id', 'region']], on="customer_id", how="left"
    )

    if final_df.isnull().any().any():
        logger.warning("Merged DataFrame contains NaN values, typically due to missing dimensions.")

    # Step 3: Define dimensions and metrics for the cube (P6 Goal)
    dimensions = ["region", "category"]  # Dimensions P6: Rentabilité Catégorie par Région
    metrics = {"sale_amount": ["sum", "mean"], "sale_id": "count"}

    # Step 4: Create the cube
    olap_cube = create_olap_cube(final_df, dimensions, metrics)

    # Step 5: Save the cube to a CSV file (Using the required name)
    write_cube_to_csv(olap_cube, "multidimensional_olap_cube.csv")

    logger.info("OLAP Cubing process completed successfully.")
    logger.info(f"Output saved to {OLAP_OUTPUT_DIR / 'multidimensional_olap_cube.csv'}")


if __name__ == "__main__":
    main()

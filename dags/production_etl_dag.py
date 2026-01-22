
from __future__ import annotations

import pendulum
import logging
from pathlib import Path

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# This DAG requires the 'pandas' library.
# It also requires a Postgres Airflow connection named 'my_postgres_warehouse'.
try:
    import pandas as pd
except ImportError:
    logging.warning("Could not import pandas. The DAG will fail. Please rebuild your docker image.")

# Define the local path for storing intermediate data files
TMP_DATA_DIR = Path("/tmp/data")
RAW_DATA_PATH = TMP_DATA_DIR / "raw_sales.csv"
CLEANED_DATA_PATH = TMP_DATA_DIR / "cleaned_sales.csv"

@task
def extract_raw_sales():
    """
    Generates a mock dataset of raw sales and saves it as a CSV.
    In a real-world scenario, this would pull data from an API or external source.
    """
    logging.info(f"Generating raw sales data at {RAW_DATA_PATH}")
    TMP_DATA_DIR.mkdir(exist_ok=True)

    sales_data = {
        "order_id": [1, 2, 3, 4, 5],
        "product_id": ["P101", "P102", "P101", "P103", "P102"],
        "quantity": [2, 1, 1, 3, 1],
        "unit_price": [10.00, 25.50, None, 5.25, 25.50],
        "order_date": ["2023-01-15", "2023-01-16", "2023-01-16", "2023-01-17", "2023-01-18"],
    }
    df = pd.DataFrame(sales_data)
    df.to_csv(RAW_DATA_PATH, index=False)
    
    return str(RAW_DATA_PATH)

@task
def transform_and_clean(raw_path: str):
    """
    Reads the raw data, cleans it, adds a 'total_price' column,
    and performs a data quality check.
    """
    logging.info(f"Transforming data from {raw_path}")
    df = pd.read_csv(raw_path)

    # 1. Data Cleaning: Fill missing prices with the product's average price
    df['unit_price'] = df.groupby('product_id')['unit_price'].transform(lambda x: x.fillna(x.mean()))
    
    # 2. Transformation: Calculate total price
    df["total_price"] = df["quantity"] * df["unit_price"]

    # 3. Data Quality Check: Ensure all orders have a positive total price
    if not (df["total_price"] > 0).all():
        raise ValueError("Data quality check failed: Found non-positive total prices.")
    
    logging.info(f"Saving cleaned data to {CLEANED_DATA_PATH}")
    df.to_csv(CLEANED_DATA_PATH, index=False)

    return str(CLEANED_DATA_PATH)

@task
def load_to_postgres(cleaned_path: str):
    """
    Loads the cleaned data from a CSV file into the 'daily_sales'
    Postgres table. This task is idempotent; running it multiple
    times with the same data won't create duplicate entries.
    """
    logging.info(f"Loading data from {cleaned_path} to Postgres.")
    
    # Uses an Airflow Hook to interact with the Postgres connection
    hook = PostgresHook(postgres_conn_id="my_postgres_warehouse")
    engine = hook.get_sqlalchemy_engine()
    
    df = pd.read_csv(cleaned_path)
    
    # Logic to prevent duplicate loads for the same date
    # In a real pipeline, you'd typically delete existing data for the execution_date
    # before inserting the new data.
    df.to_sql(
        "daily_sales",
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
    )
    logging.info("Successfully loaded data into daily_sales table.")


with DAG(
    dag_id="production_etl_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md="""
    ### Production ETL DAG

    This DAG simulates a daily sales data pipeline.
    - **Extract**: Generates mock sales data.
    - **Transform**: Cleans data, calculates total price, and runs a quality check.
    - **Load**: Loads the processed data into a Postgres table named `daily_sales`.

    **Requires:**
    1. The `pandas` library to be installed.
    2. A `Postgres` Airflow Connection with `Conn Id` = `my_postgres_warehouse`.
    """,
    tags=["example", "etl", "production"],
) as dag:
    
    # Task to create the target table if it doesn't exist.
    # This makes the DAG robust and idempotent.
    create_sales_table = SQLExecuteQueryOperator(
        task_id="create_sales_table_if_not_exists",
        conn_id="my_postgres_warehouse",
        sql="""
            CREATE TABLE IF NOT EXISTS daily_sales (
                order_id INT,
                product_id VARCHAR(10),
                quantity INT,
                unit_price NUMERIC(10, 2),
                order_date DATE,
                total_price NUMERIC(10, 2)
            );
        """,
    )

    raw_data_filepath = extract_raw_sales()
    cleaned_data_filepath = transform_and_clean(raw_data_filepath)
    
    create_sales_table >> raw_data_filepath
    load_task = load_to_postgres(cleaned_data_filepath)
    
    # Define task dependencies
    raw_data_filepath >> cleaned_data_filepath >> load_task

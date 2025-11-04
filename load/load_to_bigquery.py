from google.cloud import bigquery
import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from config import PROJECT_ID, DATASET_ID, USERS_TABLE, PRODUCTS_TABLE, CARTS_TABLE

def create_dataset_if_not_exists(client):
    """Create dataset if it doesn't exist."""
    dataset_ref = client.dataset(DATASET_ID)
    
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists")
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {DATASET_ID}")

def load_csv_to_bigquery(client, filepath, table_name):
    """Load a CSV file to a BigQuery table."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    # Read CSV file
    df = pd.read_csv(filepath)
    
    # Define table schema
    if table_name == USERS_TABLE:
        schema = [
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
            bigquery.SchemaField("street", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("postal_code", "STRING"),
        ]
    elif table_name == PRODUCTS_TABLE:
        schema = [
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("brand", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
        ]
    elif table_name == CARTS_TABLE:
        schema = [
            bigquery.SchemaField("cart_id", "INTEGER"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("total_cart_value", "FLOAT"),
        ]
    else:
        raise ValueError(f"Unknown table name: {table_name}")
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    try:
        load_job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        
        load_job.result()  # Wait for the job to complete
        print(f"Loaded {len(df)} rows into {table_id}")
        return True
    except GoogleAPIError as e:
        print(f"Error loading data to BigQuery: {e}")
        return False

def load_all_data_to_bigquery(users_csv, products_csv, carts_csv):
    """Load all data to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Ensure dataset exists
    create_dataset_if_not_exists(client)
    
    # Load data
    users_success = load_csv_to_bigquery(client, users_csv, USERS_TABLE)
    products_success = load_csv_to_bigquery(client, products_csv, PRODUCTS_TABLE)
    carts_success = load_csv_to_bigquery(client, carts_csv, CARTS_TABLE)
    
    return users_success and products_success and carts_success

if __name__ == "__main__":
    # Replace with actual file paths when running directly
    load_all_data_to_bigquery(
        "../data/processed/users.csv",
        "../data/processed/products.csv",
        "../data/processed/carts.csv"
    )
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from config import PROJECT_ID, DATASET_ID, USER_SUMMARY_TABLE, CATEGORY_SUMMARY_TABLE, CART_DETAILS_TABLE, USERS_TABLE, PRODUCTS_TABLE, CARTS_TABLE

def execute_query(client, query, destination_table=None):
    """Execute a BigQuery query."""
    job_config = bigquery.QueryJobConfig()
    
    if destination_table:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{destination_table}"
        job_config.destination = table_id
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    
    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        if destination_table:
            print(f"Query results saved to {table_id}")
        else:
            print("Query executed successfully")
        
        return results
    except GoogleAPIError as e:
        print(f"Error executing query: {e}")
        return None

def create_user_summary_table(client):
    """Create the user summary table."""
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{USER_SUMMARY_TABLE}` AS
    SELECT
        u.user_id,
        u.first_name,
        SUM(c.quantity * c.price) AS total_spent,
        SUM(c.quantity) AS total_items,
        u.age,
        u.city
    FROM
        `{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE}` AS u
    JOIN
        `{PROJECT_ID}.{DATASET_ID}.{CARTS_TABLE}` AS c
    ON
        u.user_id = c.user_id
    GROUP BY
        u.user_id, u.first_name, u.age, u.city
    ORDER BY
        total_spent DESC
    """
    
    return execute_query(client, query)

def create_category_summary_table(client):
    """Create the category summary table."""
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{CATEGORY_SUMMARY_TABLE}` AS
    SELECT
        p.category,
        SUM(c.quantity * c.price) AS total_sales,
        SUM(c.quantity) AS total_items_sold
    FROM
        `{PROJECT_ID}.{DATASET_ID}.{PRODUCTS_TABLE}` AS p
    JOIN
        `{PROJECT_ID}.{DATASET_ID}.{CARTS_TABLE}` AS c
    ON
        p.product_id = c.product_id
    GROUP BY
        p.category
    ORDER BY
        total_sales DESC
    """
    
    return execute_query(client, query)

def create_cart_details_table(client):
    """Create the cart details table."""
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{CART_DETAILS_TABLE}` AS
    SELECT
        c.cart_id,
        c.user_id,
        c.product_id,
        c.quantity,
        c.price,
        c.total_cart_value,
        u.first_name,
        u.last_name,
        p.name AS product_name,
        p.category,
        p.brand
    FROM
        `{PROJECT_ID}.{DATASET_ID}.{CARTS_TABLE}` AS c
    JOIN
        `{PROJECT_ID}.{DATASET_ID}.{USERS_TABLE}` AS u
    ON
        c.user_id = u.user_id
    JOIN
        `{PROJECT_ID}.{DATASET_ID}.{PRODUCTS_TABLE}` AS p
    ON
        c.product_id = p.product_id
    ORDER BY
        c.cart_id, c.product_id
    """
    
    return execute_query(client, query)

def run_all_transformations():
    """Run all transformations."""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Create summary tables
    user_summary_success = create_user_summary_table(client) is not None
    category_summary_success = create_category_summary_table(client) is not None
    cart_details_success = create_cart_details_table(client) is not None
    
    return user_summary_success and category_summary_success and cart_details_success

if __name__ == "__main__":
    run_all_transformations()
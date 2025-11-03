import os

# API Endpoints
USERS_API_URL = "https://dummyjson.com/users"
PRODUCTS_API_URL = "https://dummyjson.com/products"
CARTS_API_URL = "https://dummyjson.com/carts"

# Local Data Paths
RAW_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw")
PROCESSED_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "processed")

# BigQuery Configuration
PROJECT_ID = "savannah-etl-project"  # Replace with your project ID
DATASET_ID = "savannah_assessment"

# Table Names
USERS_TABLE = "users_table"
PRODUCTS_TABLE = "products_table"
CARTS_TABLE = "carts_table"
USER_SUMMARY_TABLE = "user_summary"
CATEGORY_SUMMARY_TABLE = "category_summary"
CART_DETAILS_TABLE = "cart_details"
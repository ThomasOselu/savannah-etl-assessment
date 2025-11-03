import requests
import json
import os
from datetime import datetime
from config import USERS_API_URL, PRODUCTS_API_URL, CARTS_API_URL, RAW_DATA_PATH

def ensure_directory_exists(path):
    """Ensure the directory exists."""
    if not os.path.exists(path):
        os.makedirs(path)

def fetch_data_from_api(url):
    """Fetch data from an API."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def save_raw_data(data, filename):
    """Save raw data to a local file."""
    ensure_directory_exists(RAW_DATA_PATH)
    filepath = os.path.join(RAW_DATA_PATH, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f)
    
    print(f"Data saved to {filepath}")
    return filepath

def extract_users_data():
    """Extract users data."""
    data = fetch_data_from_api(USERS_API_URL)
    if data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"users_{timestamp}.json"
        return save_raw_data(data, filename)
    return None

def extract_products_data():
    """Extract products data."""
    data = fetch_data_from_api(PRODUCTS_API_URL)
    if data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"products_{timestamp}.json"
        return save_raw_data(data, filename)
    return None

def extract_carts_data():
    """Extract carts data."""
    data = fetch_data_from_api(CARTS_API_URL)
    if data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"carts_{timestamp}.json"
        return save_raw_data(data, filename)
    return None

def extract_all_data():
    """Extract all data."""
    users_file = extract_users_data()
    products_file = extract_products_data()
    carts_file = extract_carts_data()
    
    return {
        "users": users_file,
        "products": products_file,
        "carts": carts_file
    }

if __name__ == "__main__":
    extract_all_data()
import json
import pandas as pd
import os
from config import PROCESSED_DATA_PATH

def ensure_directory_exists(path):
    """Ensure the directory exists."""
    if not os.path.exists(path):
        os.makedirs(path)

def load_json_data(filepath):
    """Load JSON data."""
    with open(filepath, 'r') as f:
        return json.load(f)

def clean_users_data(users_data):
    """Clean users data."""
    users_list = []
    
    for user in users_data.get('users', []):
        # Extract address information
        address = user.get('address', {})
        street = f"{address.get('address', '')}, {address.get('suite', '')}"
        city = address.get('city', '')
        postal_code = address.get('postalCode', '')
        
        # Create a cleaned user record
        cleaned_user = {
            'user_id': user.get('id'),
            'first_name': user.get('firstName', ''),
            'last_name': user.get('lastName', ''),
            'gender': user.get('gender', ''),
            'age': user.get('age', 0),
            'street': street,
            'city': city,
            'postal_code': postal_code
        }
        
        users_list.append(cleaned_user)
    
    return pd.DataFrame(users_list)

def clean_products_data(products_data):
    """Clean products data."""
    products_list = []
    
    for product in products_data.get('products', []):
        # Only keep products with price > 50
        if product.get('price', 0) > 50:
            cleaned_product = {
                'product_id': product.get('id'),
                'name': product.get('title', ''),
                'category': product.get('category', ''),
                'brand': product.get('brand', ''),
                'price': product.get('price', 0)
            }
            
            products_list.append(cleaned_product)
    
    return pd.DataFrame(products_list)

def clean_carts_data(carts_data):
    """Clean carts data."""
    carts_list = []
    
    for cart in carts_data.get('carts', []):
        cart_id = cart.get('id')
        user_id = cart.get('userId')
        
        # Calculate total cart value
        total_cart_value = sum(item.get('quantity', 0) * item.get('price', 0) 
                              for item in cart.get('products', []))
        
        # Flatten the products array, one row per product
        for item in cart.get('products', []):
            cleaned_cart_item = {
                'cart_id': cart_id,
                'user_id': user_id,
                'product_id': item.get('id'),
                'quantity': item.get('quantity', 0),
                'price': item.get('price', 0),
                'total_cart_value': total_cart_value
            }
            
            carts_list.append(cleaned_cart_item)
    
    return pd.DataFrame(carts_list)

def save_processed_data(df, filename):
    """Save processed data."""
    ensure_directory_exists(PROCESSED_DATA_PATH)
    filepath = os.path.join(PROCESSED_DATA_PATH, filename)
    
    # Save as CSV for easy loading to BigQuery
    df.to_csv(filepath, index=False)
    
    print(f"Processed data saved to {filepath}")
    return filepath

def process_all_data(users_file, products_file, carts_file):
    """Process all data."""
    # Load raw data
    users_data = load_json_data(users_file)
    products_data = load_json_data(products_file)
    carts_data = load_json_data(carts_file)
    
    # Clean data
    users_df = clean_users_data(users_data)
    products_df = clean_products_data(products_data)
    carts_df = clean_carts_data(carts_data)
    
    # Save processed data
    users_csv = save_processed_data(users_df, "users.csv")
    products_csv = save_processed_data(products_df, "products.csv")
    carts_csv = save_processed_data(carts_df, "carts.csv")
    
    return {
        "users": users_csv,
        "products": products_csv,
        "carts": carts_csv
    }

if __name__ == "__main__":
    # Replace with actual file paths when running directly
    process_all_data(
        "../data/raw/users_20230101_120000.json",
        "../data/raw/products_20230101_120000.json",
        "../data/raw/carts_20230101_120000.json"
    )
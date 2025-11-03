import os
import sys
from extract.extract_data import extract_all_data
from transform.clean_data import process_all_data
from load.load_to_bigquery import load_all_data_to_bigquery
from transform.transform_data import run_all_transformations

def main():
    print("Starting ETL pipeline...")
    
    # 1. Extract data
    print("\nStep 1: Extracting data from APIs...")
    raw_files = extract_all_data()
    
    if not all(raw_files.values()):
        print("Error: Failed to extract data from APIs")
        sys.exit(1)
    
    # 2. Clean and transform data
    print("\nStep 2: Cleaning and transforming data...")
    processed_files = process_all_data(
        raw_files["users"],
        raw_files["products"],
        raw_files["carts"]
    )
    
    if not all(processed_files.values()):
        print("Error: Failed to process data")
        sys.exit(1)
    
    # 3. Load data to BigQuery
    print("\nStep 3: Loading data to BigQuery...")
    load_success = load_all_data_to_bigquery(
        processed_files["users"],
        processed_files["products"],
        processed_files["carts"]
    )
    
    if not load_success:
        print("Error: Failed to load data to BigQuery")
        sys.exit(1)
    
    # 4. Run transformations and analysis
    print("\nStep 4: Running transformations and analysis...")
    transform_success = run_all_transformations()
    
    if not transform_success:
        print("Error: Failed to run transformations")
        sys.exit(1)
    
    print("\nETL pipeline completed successfully!")

if __name__ == "__main__":
    main()
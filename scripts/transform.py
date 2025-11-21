import pandas as pd
import glob
import os
from datetime import datetime

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")
        
def transform_data():
    print("Starting transformation phase...")
    
    staging_dir = "data/staging"
    processed_dir = "data/processed"
    ensure_directory(processed_dir)

    # Gather staged files
    staged_files = glob.glob(os.path.join(staging_dir, "*.csv"))
    
    if not staged_files:
        print("No staged files found. Nothing to transform.")
        return

    # Identify each file by columns it contains
    sales_df = None
    products_df = None
    stores_df = None

    for f in staged_files:
        temp = pd.read_csv(f)
        cols = set(temp.columns)

        if {"sale_id", "product_id", "store_id"}.issubset(cols):
            sales_df = temp
            print(f"Detected SALES file: {os.path.basename(f)}")

        elif {"product_id", "product_name"}.issubset(cols):
            products_df = temp
            print(f"Detected PRODUCTS file: {os.path.basename(f)}")

        elif {"store_id", "store_name"}.issubset(cols):
            stores_df = temp
            print(f"Detected STORES file: {os.path.basename(f)}")

    # Sanity checks
    if sales_df is None or products_df is None or stores_df is None:
        print("Missing one of the required files (sales/products/stores).")
        return

    # Standardize column names
    for df in [sales_df, products_df, stores_df]:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

    # Rename columns if needed
    if "total_price" in sales_df.columns:
        sales_df = sales_df.rename(columns={"total_price": "total"})

    if "sale_date" in sales_df.columns:
        sales_df = sales_df.rename(columns={"sale_date": "date"})

    # Convert date to proper datetime
    sales_df["date"] = pd.to_datetime(sales_df["date"], errors="coerce")

    # Join tables
    merged_df = (
        sales_df
        .merge(products_df, how="left", on="product_id")
        .merge(stores_df, how="left", on="store_id")
    )

    # Rename unit price column and remove duplicate column
    merged_df = merged_df.drop("unit_price_x", axis=1)
    merged_df = merged_df.rename(columns = {"unit_price_y": "unit_price"})
    print(merged_df.head())
    
    # Post-processing
    merged_df["date"] = merged_df["date"].dt.strftime("%d/%m/%Y")
    merged_df["total"] = pd.to_numeric(merged_df["total"], errors="coerce")
    merged_df["total"] = merged_df["total"].fillna(merged_df["quantity"] * merged_df["unit_price"])

    # Save output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{processed_dir}/processed_{timestamp}.csv"
    merged_df.to_csv(output_path, index=False)

    print("✅ Transform complete!")
    print(f"Saved: {output_path}")

if __name__ == "__main__":
    transform_data()

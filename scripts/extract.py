import pandas as pd
import shutil
import os
import glob
from datetime import datetime 

# Create staging folder if it doesn't exist 
def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")


def extract_data():
    print("Starting extraction phase... ")
    
    raw_dir = "data/raw"
    staging_dir = "data/staging"
    
    # Make sure both folders exist
    ensure_directory(raw_dir)
    ensure_directory(staging_dir)
    
    # Find all CSV files in raw/
    raw_files = glob.glob("data/raw/*.csv")

    if not raw_files:
        print("No raw CSV files found. Nothing to extract.")
        return 

    # Copy each file to staging/
    for file in raw_files:
        filename = os.path.basename(file)
        
        # timestamp version of the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        staged_filename = f"{timestamp}_{filename}"
        
        dst_path = os.path.join(staging_dir, staged_filename)
        
        shutil.copy(file, dst_path)
        
        print(f"Extracted: {filename} -> staging/{staged_filename}")
        
    print("Extraction complete! Files are ready in data/staging/")
    
if __name__ == "__main__":
    print("Running extract_data manually...")
    extract_data()
import pandas as pd
import glob
import os
from datetime import datetime

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")
        
def load_data():
    print("Starting loading phase...")
    
    processed_dir = "data/processed"
    final_dir = "data/final"
    
    ensure_directory(final_dir)
    
    # Find latest cleaned CSV
    processed_files = glob.glob(os.path.join(processed_dir, "processed_*.csv"))
    
    if not processed_files:
        print("No processed files found. Nothing to load.")
        return
    
    latest_file = max(processed_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)
    
    # Simulate loading to final destination
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{final_dir}/final_{timestamp}.csv"
    
    df.to_csv(output_path, index=False)
    
    print(f"Load complete! Final dataset saved to: {output_path}")
    
if __name__ == "__main__":
    load_data()
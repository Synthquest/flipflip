from flipside import Flipside
import os
import sys
from dotenv import load_dotenv
import pandas as pd
import datetime

def read_sql_from_file(file_path):
    """Read SQL query from a file."""
    with open(file_path, 'r') as file:
        return file.read()

def process_sql_file(flipside, sql_file):
    """Process a single SQL file: run query and save results to CSV."""
    # Read SQL from file
    sql = read_sql_from_file(sql_file)
    
    # Generate output filename based on SQL filename
    base_name = os.path.basename(sql_file)
    file_name_without_ext = os.path.splitext(base_name)[0]
    output_file = f"{file_name_without_ext}.csv"
    
    print(f"Processing {sql_file}...")
    
    # Run query
    query_result_set = flipside.query(sql)
    print(f"Query completed. Converting to DataFrame...")
    
    # Convert to DataFrame
    df = pd.DataFrame(query_result_set.records)
    
    # Clean up data if needed
    if '__row_index' in df.columns:
        df = df.drop('__row_index', axis=1)
    
    # Convert date columns if they exist
    if 'dt' in df.columns:
        df['dt'] = pd.to_datetime(df['dt'])
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
    
    return output_file

def main():
    # Load environment variables
    load_dotenv()
    
    # Initialize Flipside with API Key
    flipside = Flipside(os.getenv("FLIPSIDE_API_KEY"), "https://api-v2.flipsidecrypto.xyz")
    
    # Get SQL files from command line arguments or use a default list
    sql_files = sys.argv[1:] if len(sys.argv) > 1 else ["marinade_mnde_staking.sql", "marinade_lst_staking.sql", "marinade_dex_metrics.sql", "solana_total_sol_staked.sql"]
    
    # Process each SQL file
    processed_files = []
    for sql_file in sql_files:
        try:
            output_file = process_sql_file(flipside, sql_file)
            processed_files.append((sql_file, output_file))
        except Exception as e:
            print(f"Error processing {sql_file}: {e}")
    
    # Summary
    print("\nSummary:")
    for sql_file, output_file in processed_files:
        print(f"{sql_file} -> {output_file}")

if __name__ == "__main__":
    main()
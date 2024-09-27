import os
import pandas as pd

# Directory containing your CSV files
csv_directory = '/Users/chrishuber/src/lbf/data/huber_futures/fx_prices_csv'

# Process each file in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)

        # Load the CSV file
        try:
            df = pd.read_csv(file_path)

            # Ensure 'DATETIME' column exists and is parsed correctly
            if 'DATETIME' in df.columns:
                # Convert 'DATETIME' column to the required format
                df['DATETIME'] = pd.to_datetime(df['DATETIME'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

                # Save the modified CSV file back to its location
                df.to_csv(file_path, index=False)
                print(f"Processed and reformatted dates in {filename}")
            else:
                print(f"DATETIME column not found in {filename}")

        except Exception as e:
            print(f"Error processing {filename}: {e}")

print("All CSV files have been processed.")
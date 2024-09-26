import pandas as pd

# Specify the path to one of your CSV files
csv_file_path = '/Users/chrishuber/src/lbf/data/futures/fx_prices_csv/AUDUSD.csv'

# Load the CSV file without any date parsing to see the raw data
df = pd.read_csv(csv_file_path)

# Print the first few entries in the 'DATETIME' column
print(df['DATETIME'].head())
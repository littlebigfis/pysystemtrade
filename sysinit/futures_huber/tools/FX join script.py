import os
from sysdata.parquet.parquet_spotfx_prices import parquetFxPricesData
from sysdata.parquet.parquet_access import ParquetAccess
from syslogging.logger import get_logger

# Initialize ParquetAccess (handles access to Parquet storage)
parquet_access = ParquetAccess()  # You may need to specify the path if required

# Initialize parquetFxPricesData class with the required parquet_access
fx_parquet_data = parquetFxPricesData(parquet_access=parquet_access, log=get_logger("FXParquetLogger"))

# Path to the directory containing your FX CSV files
csv_directory = '/Users/chrishuber/src/lbf/data/huber_futures/fx_prices_csv'

# Iterate over each CSV file in the directory and let PST handle the rest
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        # Extract the currency code from the filename (assuming format like 'GBPUSD.csv')
        currency_code = os.path.splitext(filename)[0]

        # Let PST handle reading the CSV and processing the FX data into Parquet
        fx_parquet_data.add_fx_prices_from_csv(currency_code, csv_directory)
        print(f"Processed and saved data for {currency_code}.")

print("All FX data has been processed and saved to Parquet.")
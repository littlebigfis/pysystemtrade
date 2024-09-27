import os
from sysdata.parquet.parquet_spotfx_prices import parquetFxPricesData
from sysdata.parquet.parquet_access import ParquetAccess
from syslogging.logger import get_logger
from sysdata.csv.csv_spot_fx import csvFxPricesData, ConfigCsvFXPrices

# Specify your custom CSV directory path
custom_csv_directory = '/Users/chrishuber/src/lbf/data/huber_futures/fx_prices_csv'

# Specify the Parquet storage path
parquet_store_path = '/Users/chrishuber/src/parquet'

# Initialize ParquetAccess with the required storage path
parquet_access = ParquetAccess(parquet_store_path)

# Initialize the csvFxPricesData class with the custom CSV path
csv_fx_prices_data = csvFxPricesData(datapath=custom_csv_directory, log=get_logger("csvFxPricesData"))

# Initialize the parquetFxPricesData class with the required parquet_access
fx_parquet_data = parquetFxPricesData(parquet_access=parquet_access, log=get_logger("FXParquetLogger"))

# Iterate over each CSV file in the directory and let PST handle the rest
for filename in os.listdir(custom_csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(custom_csv_directory, filename)
        try:
            # Log the file being processed
            print(f"Processing file: {file_path}")

            # Extract the currency code from the filename (assuming format like 'GBPUSD.csv')
            currency_code = os.path.splitext(filename)[0]

            # Use csvFxPricesData to process CSV files and write to Parquet
            fx_prices = csv_fx_prices_data._get_fx_prices_without_checking(currency_code)
            fx_parquet_data._add_fx_prices_without_checking_for_existing_entry(currency_code, fx_prices)

            print(f"Processed and saved data for {currency_code}.")
        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            continue

print("All FX data has been processed and saved to Parquet.")
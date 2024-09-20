import os
from syscore.constants import arg_not_supplied
from sysdata.parquet.parquet_access import ParquetAccess
from syslogging.logger import get_logger
from sysobjects.adjusted_prices import futuresAdjustedPrices
from sysobjects.multiple_prices import futuresMultiplePrices
from parquetFuturesAdjustedPricesData import parquetFuturesAdjustedPricesData
from parquetFuturesMultiplePricesData import parquetFuturesMultiplePricesData

# Initialize ParquetAccess (modify the path as necessary)
parquet_access = ParquetAccess("/path/to/parquet/folder")
log = get_logger("ParquetScript")

def get_instrument_code():
    """ Prompt the user to enter an instrument code. """
    instrument_code = input("Please enter the instrument code (e.g., AEX_mini, SPX): ").strip()
    return instrument_code

def process_adjusted_prices(instrument_code: str):
    # Initialize parquet adjusted prices object
    parquet_adj_data = parquetFuturesAdjustedPricesData(parquet_access)

    # Fetch adjusted prices from Parquet
    try:
        adj_prices = parquet_adj_data.get_adjusted_prices(instrument_code)
        print(f"Adjusted prices data for {instrument_code}: {adj_prices.head()}")
    except Exception as e:
        print(f"Error fetching adjusted prices for {instrument_code}: {e}")
        return

    # Optionally, add new adjusted prices back into Parquet
    try:
        parquet_adj_data.add_adjusted_prices(instrument_code, adj_prices)
        print(f"Successfully added adjusted prices for {instrument_code} to Parquet.")
    except Exception as e:
        print(f"Error adding adjusted prices for {instrument_code} to Parquet: {e}")

def process_multiple_prices(instrument_code: str):
    # Initialize parquet multiple prices object
    parquet_mult_data = parquetFuturesMultiplePricesData(parquet_access)

    # Fetch multiple prices from Parquet
    try:
        mult_prices = parquet_mult_data.get_multiple_prices(instrument_code)
        print(f"Multiple prices data for {instrument_code}: {mult_prices.head()}")
    except Exception as e:
        print(f"Error fetching multiple prices for {instrument_code}: {e}")
        return

    # Optionally, add new multiple prices back into Parquet
    try:
        parquet_mult_data.add_multiple_prices(instrument_code, mult_prices)
        print(f"Successfully added multiple prices for {instrument_code} to Parquet.")
    except Exception as e:
        print(f"Error adding multiple prices for {instrument_code} to Parquet: {e}")

if __name__ == "__main__":
    # Main entry point for processing adjusted and multiple prices
    instrument_code = get_instrument_code()

    # Process both adjusted and multiple prices for the instrument
    process_adjusted_prices(instrument_code)
    process_multiple_prices(instrument_code)
from syscore.constants import arg_not_supplied
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysproduction.data.prices import diagPrices

# Initialize the diagPrices object from PySystemTrade
diag_prices = diagPrices()

def init_data_with_csv_futures_contract_prices(
    multiple_price_datapath=arg_not_supplied, adj_price_datapath=arg_not_supplied
):
    """
    This function initializes the system by reading multiple and adjusted prices from CSV
    and writing them to the configured Parquet database (through diagPrices).
    """
    csv_multiple_prices = csvFuturesMultiplePricesData(multiple_price_datapath)
    csv_adj_prices = csvFuturesAdjustedPricesData(adj_price_datapath)

    input(
        f"WARNING: THIS WILL OVERWRITE EXISTING PRICES WITH DATA FROM {csv_adj_prices.datapath}, {csv_multiple_prices.datapath}. "
        "ARE YOU SURE? CTRL-C TO ABORT."
    )

    # Retrieve list of instruments from the CSV data
    instrument_codes = csv_multiple_prices.get_list_of_instruments()

    for instrument_code in instrument_codes:
        process_csv_prices_for_instrument(
            instrument_code,
            multiple_price_datapath=multiple_price_datapath,
            adj_price_datapath=adj_price_datapath,
        )


def process_csv_prices_for_instrument(
    instrument_code: str, multiple_price_datapath=arg_not_supplied, adj_price_datapath=arg_not_supplied
):
    """
    This function processes the multiple and adjusted prices for a given instrument,
    and writes the data to the storage system (handled by diagPrices).
    """
    print(f"Processing {instrument_code}")

    # Process multiple prices
    csv_mult_data = csvFuturesMultiplePricesData(multiple_price_datapath)
    db_mult_data = diag_prices.db_futures_multiple_prices_data

    mult_prices = csv_mult_data.get_multiple_prices(instrument_code)

    try:
        db_mult_data.add_multiple_prices(instrument_code, mult_prices, ignore_duplication=True)
        print(f"Successfully wrote multiple prices for {instrument_code}.")
    except Exception as e:
        print(f"Error writing multiple prices for {instrument_code}: {e}")

    # Process adjusted prices
    csv_adj_data = csvFuturesAdjustedPricesData(adj_price_datapath)
    db_adj_data = diag_prices.db_futures_adjusted_prices_data

    adj_prices = csv_adj_data.get_adjusted_prices(instrument_code)

    try:
        db_adj_data.add_adjusted_prices(instrument_code, adj_prices, ignore_duplication=True)
        print(f"Successfully wrote adjusted prices for {instrument_code}.")
    except Exception as e:
        print(f"Error writing adjusted prices for {instrument_code}: {e}")


if __name__ == "__main__":
    # Modify datapaths as required
    init_data_with_csv_futures_contract_prices(
        adj_price_datapath="/Users/chrishuber/src/lbf/data/huber_futures/adjusted_prices_csv",
        multiple_price_datapath="/Users/chrishuber/src/lbf/data/huber_futures/multiple_prices_csv"
    )
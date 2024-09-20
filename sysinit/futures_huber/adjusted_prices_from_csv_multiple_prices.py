import os
import pandas as pd
from syscore.constants import arg_not_supplied
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysobjects.adjusted_prices import futuresAdjustedPrices

# Define the paths for the CSV repositories
multiple_prices_csv_path = "/Users/chrishuber/src/lbf/data/huber_futures/multiple_prices_csv"
updated_adjusted_prices_csv_path = ("/Users/chrishuber/src/lbf/data/huber_futures/adjusted_prices_csv")

# Ensure the updated adjusted prices directory exists
os.makedirs(updated_adjusted_prices_csv_path, exist_ok=True)


def _get_data_inputs(csv_adj_data_path):
    # Access multiple prices and adjusted prices from CSV files
    csv_multiple_prices = csvFuturesMultiplePricesData(multiple_prices_csv_path)
    csv_adjusted_prices = csvFuturesAdjustedPricesData(csv_adj_data_path)

    return csv_multiple_prices, csv_adjusted_prices


def process_adjusted_prices_single_instrument(
        instrument_code,
        ADD_TO_CSV=True,
):
    # Fetch multiple prices for the instrument from CSV
    csv_multiple_prices = csvFuturesMultiplePricesData(multiple_prices_csv_path)
    multiple_prices = csv_multiple_prices.get_multiple_prices(instrument_code)

    # Stitch the multiple prices to generate adjusted prices (forward fill)
    adjusted_prices = futuresAdjustedPrices.stitch_multiple_prices(
        multiple_prices, forward_fill=True
    )

    # Ensure the result contains 'DATETIME' and 'price' columns
    adjusted_prices = adjusted_prices.reset_index()  # Ensure DATETIME is in the DataFrame
    adjusted_prices.columns = ['DATETIME', 'price']  # Rename columns appropriately

    print(f"Adjusted prices for {instrument_code}:")
    print(adjusted_prices)

    # Save the adjusted prices to the updated CSV directory
    if ADD_TO_CSV:
        output_path = os.path.join(updated_adjusted_prices_csv_path, f"{instrument_code}.csv")
        adjusted_prices.to_csv(output_path, index=False)
        print(f"Adjusted prices for {instrument_code} saved to {output_path}")

    return adjusted_prices


if __name__ == "__main__":
    input("This will create a new back-adjusted price series. Press Enter to continue or CTL-C to abort.")

    # Prompt the user for the instrument code
    instrument_code = input("Enter the instrument code (e.g., 'ED'): ").strip()

    if not instrument_code:
        print("No instrument code provided. Exiting.")
        exit()

    # Process the selected instrument and save adjusted prices to CSV
    process_adjusted_prices_single_instrument(
        instrument_code=instrument_code,
        ADD_TO_CSV=True
    )
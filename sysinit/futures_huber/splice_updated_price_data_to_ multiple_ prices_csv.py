import os
import pandas as pd
from syscore.constants import arg_not_supplied
from sysproduction.data.prices import diagPrices
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysobjects.multiple_prices import futuresMultiplePrices
from syslogging.logger import get_logger

# Initialize the logger
log = get_logger(__name__)

# Initialize diagPrices to access price data stored in Parquet
diag_prices = diagPrices()

def process_multiple_prices_single_instrument(
    instrument_code,
    target_instrument_code=arg_not_supplied,
    csv_multiple_data_path=arg_not_supplied,
    csv_roll_data_path=arg_not_supplied,
    ADD_TO_CSV=True,
    save_with_new_suffix=False
):
    if target_instrument_code is arg_not_supplied:
        target_instrument_code = instrument_code

    # Get price data and roll calendar from the existing CSV paths
    csv_roll_calendars = csvRollCalendarData(csv_roll_data_path)
    db_individual_futures_prices = diag_prices.db_futures_contract_price_data
    csv_multiple_prices = csvFuturesMultiplePricesData(csv_multiple_data_path)

    # Retrieve merged prices for the instrument using PySystemTrade function
    dict_of_futures_contract_prices = db_individual_futures_prices.get_merged_prices_for_instrument(instrument_code)
    dict_of_futures_contract_closing_prices = dict_of_futures_contract_prices.final_prices()

    # Retrieve roll calendar for the instrument from CSV
    roll_calendar = csv_roll_calendars.get_roll_calendar(instrument_code)

    # Generate multiple prices using PySystemTrade's futuresMultiplePrices
    multiple_prices = futuresMultiplePrices.create_from_raw_data(
        roll_calendar,  # Pass the roll calendar
        dict_of_futures_contract_closing_prices  # Pass the contract closing prices
    )

    log.info(f"Generated multiple prices for {instrument_code}")

    if ADD_TO_CSV:
        if save_with_new_suffix:
            # Save the new multiple prices with a '_new' suffix temporarily
            original_instrument_code = target_instrument_code
            target_instrument_code = f"{original_instrument_code}_new"
            csv_multiple_prices.add_multiple_prices(
                target_instrument_code, multiple_prices, ignore_duplication=True
            )
            target_instrument_code = original_instrument_code
        else:
            # Save the multiple prices normally
            csv_multiple_prices.add_multiple_prices(
                target_instrument_code, multiple_prices, ignore_duplication=True
            )

    return multiple_prices

def splice_multiple_prices(
    instrument_code,
    csv_multiple_data_path,  # Path for the stale data
    temp_multiple_data_path,  # Path for the new temp file
    updated_multiple_data_path  # Final output directory
):
    existing_file = os.path.join(csv_multiple_data_path, f'{instrument_code}.csv')
    new_file = os.path.join(temp_multiple_data_path, f'{instrument_code}_new.csv')  # Temporary file for new prices

    # Check if existing multiple prices file exists
    if not os.path.exists(existing_file):
        log.warning(f"No existing multiple prices file for {instrument_code}. Skipping splicing.")
        return

    # Read existing and new multiple prices data
    existing_data = pd.read_csv(existing_file, index_col=0, parse_dates=True)
    new_data = pd.read_csv(new_file, index_col=0, parse_dates=True)

    # Identify the last date in existing data
    last_existing_date = existing_data.index[-1]
    # Truncate new data to dates after the last existing date
    new_data_truncated = new_data.loc[last_existing_date:]

    # Remove overlapping date if necessary
    if not new_data_truncated.empty and new_data_truncated.index[0] == last_existing_date:
        new_data_truncated = new_data_truncated.iloc[1:]

    # Verify contract continuity at the splice point
    if not new_data_truncated.empty:
        if not existing_data.iloc[-1]['PRICE_CONTRACT'] == new_data_truncated.iloc[0]['PRICE_CONTRACT']:
            log.warning(f"PRICE_CONTRACT mismatch at splice point for {instrument_code}. Proceeding with caution.")

        if not existing_data.iloc[-1]['FORWARD_CONTRACT'] == new_data_truncated.iloc[0]['FORWARD_CONTRACT']:
            log.warning(f"FORWARD_CONTRACT mismatch at splice point for {instrument_code}. Proceeding with caution.")

        # Combine the existing and new data
        spliced_data = pd.concat([existing_data, new_data_truncated])
    else:
        log.warning(f"No new data to splice for {instrument_code}.")
        spliced_data = existing_data

    # Save the spliced data to the final updated directory
    updated_file = os.path.join(updated_multiple_data_path, f'{instrument_code}.csv')
    os.makedirs(updated_multiple_data_path, exist_ok=True)  # Ensure the directory exists
    spliced_data.to_csv(updated_file)  # Save the spliced data to the updated path

    log.info(f"Spliced multiple prices for {instrument_code} saved to {updated_file}")

    # Clean up the temporary '_new' data file after splicing
    if os.path.exists(new_file):
        os.remove(new_file)
        log.info(f"Deleted temporary file: {new_file}")

if __name__ == "__main__":
    input("This script will generate new multiple prices and may overwrite existing data. Press Enter to continue or CTRL-C to abort.")

    # Paths to your data directories
    csv_multiple_data_path = '/Users/chrishuber/src/lbf/data/futures/multiple_prices_csv'  # Source stale data
    csv_roll_data_path = '/Users/chrishuber/src/lbf/data/huber_futures/roll_calendars_csv'
    temp_multiple_data_path = '/Users/chrishuber/src/lbf/data/huber_futures/multiple_prices_csv'  # Temporary path
    updated_multiple_data_path = '/Users/chrishuber/src/lbf/data/huber_futures/multiple_prices_csv'  # Final output

    # Prompt the user for the instrument code
    instrument_code = input("Enter the PySystemTrade instrument code (e.g., 'ED'): ").strip()
    if not instrument_code:
        print("No instrument code provided. Exiting.")
        exit()

    # Generate new multiple prices for the specified instrument
    process_multiple_prices_single_instrument(
        instrument_code=instrument_code,
        csv_multiple_data_path=csv_multiple_data_path,
        csv_roll_data_path=csv_roll_data_path,
        ADD_TO_CSV=True,
        save_with_new_suffix=True  # Save with '_new' suffix to avoid overwriting
    )

    # Splice the new multiple prices onto existing data and save to updated path
    log.info(f"Splicing multiple prices for {instrument_code}")
    splice_multiple_prices(
        instrument_code,
        csv_multiple_data_path,  # Source stale data
        temp_multiple_data_path,  # Temp location for '_new' files
        updated_multiple_data_path  # Final output after splicing
    )
# Run this after Prices Seeding from IBKR and Generating Roll Calendars to Multiple Prices last Price Date
# This script prepares a current roll calendar based on recent seed pricing stored in parquet.
# Next it appends that current roll calendar to the stale roll calendar in the roll calendar repo
# It saves the updated Roll Calendar in the roll calendar path roll calendar repo


# THIS SCRIPT HAS A PROBLEM WRITING A ROLL CALENDAR TO THE ROLL CALENDAR CSV REPO IF THERE ARE NO NEW ROLL DATES I.E. WHEAT SINCE THE LAST UPDATE. HAVE TO MANUALLY SAVE THE ROLL UPDATE TO THE REPO
# SEE IF ROB'S SCRIPT CAN REPLACE THIS ONE

import os
import pandas as pd
from sysobjects.rolls import rollParameters
from sysobjects.roll_calendars import rollCalendar
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysdata.csv.csv_roll_parameters import csvRollParametersData
from sysdata.futures.rolls_parameters import rollParametersData
from sysproduction.data.prices import get_valid_instrument_code_from_user, diagPrices
from syscore.constants import arg_not_supplied
from syscore.interactive.input import true_if_answer_is_yes

diag_prices = diagPrices()
parquet_futures_contract_price_data = diag_prices.db_futures_contract_price_data


# Function to build and write the current roll calendar based on Parquet data
def build_and_write_current_roll_calendar(
        instrument_code,
        output_datapath="/Users/chrishuber/src/lbf/data/huber_futures/current_roll_calendars_from_parquet_seed_prices_csv",
        write=True,
        check_before_writing=True,
        input_prices=arg_not_supplied,
        roll_parameters_data: rollParametersData = arg_not_supplied,
        roll_parameters: rollParameters = arg_not_supplied,
):
    print(f"Writing to {output_datapath}")

    # Get the price data
    if input_prices is arg_not_supplied:
        prices = parquet_futures_contract_price_data
    else:
        prices = input_prices

    # Get roll parameters
    if roll_parameters is arg_not_supplied:
        if roll_parameters_data is arg_not_supplied:
            roll_parameters_data = csvRollParametersData()
        roll_parameters = roll_parameters_data.get_roll_parameters(instrument_code)

    csv_roll_calendars = csvRollCalendarData(output_datapath)

    # Fetch contract prices
    try:
        dict_of_all_futures_contract_prices = prices.get_merged_prices_for_instrument(
            instrument_code
        )
        dict_of_futures_contract_prices = dict_of_all_futures_contract_prices.final_prices()

        if len(dict_of_futures_contract_prices) == 0:
            print(f"No prices found for instrument {instrument_code}. Skipping.")
            return

    except Exception as e:
        print(f"Error fetching prices for {instrument_code}: {str(e)}")
        return

    # Preparing roll calendar
    print(f"Prepping current roll calendar for {instrument_code}... might take a few seconds")

    try:
        roll_calendar = rollCalendar.create_from_prices(
            dict_of_futures_contract_prices, roll_parameters
        )
    except Exception as e:
        print(f"Failed to create roll calendar for {instrument_code}: {str(e)}")
        return

    # Checks on the roll calendar
    try:
        roll_calendar.check_if_date_index_monotonic()
        roll_calendar.check_dates_are_valid_for_prices(dict_of_futures_contract_prices)
    except Exception as e:
        print(f"Failed during roll calendar checks for {instrument_code}: {str(e)}")
        return

    # Write roll calendar
    if write:
        if check_before_writing:
            check_happy_to_write = true_if_answer_is_yes(
                "Are you ok to write this csv to path %s/%s.csv?" % (csv_roll_calendars.datapath, instrument_code)
            )
        else:
            check_happy_to_write = True

        if check_happy_to_write:
            print(f"Adding current roll calendar for {instrument_code}")
            csv_roll_calendars.add_roll_calendar(
                instrument_code, roll_calendar, ignore_duplication=True
            )
        else:
            print("Not writing - not happy")


# Function to append the current roll calendar to the updated roll calendar
def append_roll_calendar(updated_roll_calendars_from_multiple_prices_path, current_roll_calendars_from_parquet_seed_prices_path, final_output_path):
    # Load both roll calendars
    updated_roll_calendars_from_multiple_prices = pd.read_csv(updated_roll_calendars_from_multiple_prices_path)
    current_roll_calendars_from_parquet_seed_prices = pd.read_csv(current_roll_calendars_from_parquet_seed_prices_path)

    # Convert 'DATE_TIME' to datetime for proper comparison
    updated_roll_calendars_from_multiple_prices['DATE_TIME'] = pd.to_datetime(updated_roll_calendars_from_multiple_prices['DATE_TIME'])
    current_roll_calendars_from_parquet_seed_prices['DATE_TIME'] = pd.to_datetime(current_roll_calendars_from_parquet_seed_prices['DATE_TIME'])

    # Drop the last row in the updated roll calendar
    updated_roll_calendars_from_multiple_prices = updated_roll_calendars_from_multiple_prices[:-1]

    # Find the last available roll date in the updated roll calendar
    last_roll_date = updated_roll_calendars_from_multiple_prices['DATE_TIME'].max()

    # Find the closest matching roll in the current calendar after the last roll date in the updated calendar
    current_roll_calendars_filtered = current_roll_calendars_from_parquet_seed_prices[current_roll_calendars_from_parquet_seed_prices['DATE_TIME'] > last_roll_date]

    # Check if the next roll date in the current calendar is reasonably close (within 1-3 days)
    if not current_roll_calendars_filtered.empty:
        next_current_roll_date = current_roll_calendars_filtered['DATE_TIME'].min()
        time_difference = (next_current_roll_date - last_roll_date).days

        # Append only if the time difference is reasonable (e.g., between 1 and 3 days)
        if 1 <= time_difference <= 3:
            # Concatenate the updated roll calendar with the filtered current roll calendar
            merged_roll_calendar = pd.concat([updated_roll_calendars_from_multiple_prices, current_roll_calendars_filtered])

            # Save the merged roll calendar to the final output path
            merged_roll_calendar.to_csv(final_output_path, index=False)

            print(f"Roll calendars merged successfully and saved to {final_output_path}")
        else:
            print("No suitable roll date found in the current calendar within the expected range.")
    else:
        print("No new roll dates found in the current calendar after the last roll date in the updated calendar.")


# Main script
if __name__ == "__main__":
    input("Will overwrite existing roll calendar, are you sure?! CTL-C to abort")

    # Prompt the user for the instrument code
    instrument_code = get_valid_instrument_code_from_user(source="single")

    # Step 1: Build and write the new current roll calendar based on Parquet data
    build_and_write_current_roll_calendar(instrument_code)

    # Step 2: Append the current roll calendar to the updated roll calendar
    updated_roll_calendars_from_multiple_prices_path = f"/Users/chrishuber/src/lbf/data/huber_futures/updated_roll_calendars_from_multiple_prices_csv/{instrument_code}.csv"
    current_roll_calendars_from_parquet_seed_prices_path = f"/Users/chrishuber/src/lbf/data/huber_futures/current_roll_calendars_from_parquet_seed_prices_csv/{instrument_code}.csv"
    final_output_path = f"/Users/chrishuber/src/lbf/data/huber_futures/roll_calendars_csv/{instrument_code}.csv"

    append_roll_calendar(updated_roll_calendars_from_multiple_prices_path, current_roll_calendars_from_parquet_seed_prices_path, final_output_path)

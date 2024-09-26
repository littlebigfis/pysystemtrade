import os
from sysinit.futures.rollcalendars_from_arcticprices_to_csv import build_and_write_roll_calendar
from syscore.constants import arg_not_supplied
from sysproduction.data.prices import get_valid_instrument_code_from_user

# Define the output directory for the current roll calendar
current_roll_calendar_output_path = "/Users/chrishuber/src/lbf/data/huber_futures/current_roll_calendars_from_parquet_seed_prices_csv"

# Ensure the output directory exists
if not os.path.exists(current_roll_calendar_output_path):
    os.makedirs(current_roll_calendar_output_path)

if __name__ == "__main__":
    input(
        "This will overwrite existing roll calendar data for the instrument. Press Enter to continue or CTRL-C to abort.")

    # Get the instrument code from the user
    instrument_code = get_valid_instrument_code_from_user(source="single")

    # Call the build_and_write_roll_calendar function to generate the current roll calendar
    build_and_write_roll_calendar(
        instrument_code=instrument_code,
        output_datapath=current_roll_calendar_output_path,
        write=True,  # Write the roll calendar to the CSV file
        check_before_writing=False  # Skip confirmation before writing
    )

    print(f"Roll calendar for {instrument_code} generated and written to {current_roll_calendar_output_path}")
import os
from syscore.constants import arg_not_supplied
from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
from sysobjects.roll_calendars import rollCalendar
from sysdata.csv.csv_roll_calendars import csvRollCalendarData
from sysproduction.data.prices import get_valid_instrument_code_from_user  # Import function for input validation

# Specify the final folder for roll calendars
ROLL_CALENDAR_PATH = "/Users/chrishuber/src/lbf/data/huber_futures/individual_roll_calendars_from_multiple_prices_csv"


def generate_roll_calendar_for_single_instrument(
        instrument_code,
        output_datapath=arg_not_supplied,
):
    if output_datapath is arg_not_supplied:
        # Use the specified folder
        output_datapath = ROLL_CALENDAR_PATH
        os.makedirs(output_datapath, exist_ok=True)
        print(f"Writing to specified directory: {output_datapath}")
    else:
        print(f"Writing to: {output_datapath}")

    input(
        f"This will overwrite any existing roll calendar for {instrument_code} in {output_datapath}. CRTL-C if you aren't sure!")

    csv_roll_calendars = csvRollCalendarData(datapath=output_datapath)
    sim_futures_data = csvFuturesSimData()

    # Check if the instrument exists in the sim futures data
    instrument_list = sim_futures_data.get_instrument_list()
    if instrument_code not in instrument_list:
        print(f"Instrument {instrument_code} not found in the dataset.")
        return

    # Fetch multiple prices for the instrument
    multiple_prices = sim_futures_data.get_multiple_prices(instrument_code)

    # Back out the roll calendar from multiple prices
    roll_calendar = rollCalendar.back_out_from_multiple_prices(multiple_prices)
    print("Calendar:")
    print(roll_calendar)

    # Save the roll calendar to the specified folder
    csv_roll_calendars.add_roll_calendar(
        instrument_code, roll_calendar, ignore_duplication=True
    )


if __name__ == "__main__":
    # Prompt the user for the instrument code
    instrument_code = get_valid_instrument_code_from_user(source="single").strip()

    # Generate roll calendar for the specified instrument
    generate_roll_calendar_for_single_instrument(instrument_code)
# Use this script to bring the roll calendar up to the multiple prices Date_Time
# Run this script before Seeding Prices from IB or at least before Update Roll Calendars from Seeded Prices and Append to Stale Roll Calendars"


import os
from syscore.constants import arg_not_supplied
from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
from sysobjects.roll_calendars import rollCalendar
from sysdata.csv.csv_roll_calendars import csvRollCalendarData

# Specify the final folder for roll calendars
ROLL_CALENDAR_PATH = "/Users/chrishuber/src/lbf/data/huber_futures/updated_roll_calendars_from_multiple_prices_csv"


def generate_roll_calendars_from_provided_multiple_csv_prices(
        output_datapath=arg_not_supplied,
):
    if output_datapath is arg_not_supplied:
        # Use the specified folder
        output_datapath = ROLL_CALENDAR_PATH
        os.makedirs(output_datapath, exist_ok=True)
        print(f"Writing to specified directory: {output_datapath}")
    else:
        print(f"Writing to: {output_datapath}")

    input(f"This will overwrite any existing roll calendars in {output_datapath}. CRTL-C if you aren't sure!")

    csv_roll_calendars = csvRollCalendarData(datapath=output_datapath)
    sim_futures_data = csvFuturesSimData()

    instrument_list = sim_futures_data.get_instrument_list()

    for instrument_code in instrument_list:
        print(instrument_code)
        multiple_prices = sim_futures_data.get_multiple_prices(instrument_code)

        roll_calendar = rollCalendar.back_out_from_multiple_prices(multiple_prices)
        print("Calendar:")
        print(roll_calendar)

        # Save the roll calendar to the specified folder
        csv_roll_calendars.add_roll_calendar(
            instrument_code, roll_calendar, ignore_duplication=True
        )


if __name__ == "__main__":
    generate_roll_calendars_from_provided_multiple_csv_prices()
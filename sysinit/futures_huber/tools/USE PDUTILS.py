import pandas as pd
from syscore.pandas.pdutils import pd_readcsv, add_datetime_index

# Path to your CSV file
csv_file = "/Users/chrishuber/src/lbf/data/huber_futures/roll_calendars_csv/COFFEE.csv"  # Replace this with the actual path
date_column = "DATE_TIME"  # Assuming your date column is named 'DATE_TIME'

# Read the CSV and inspect the date
try:
    # Attempt to read the CSV and parse the DATE_TIME column
    df = pd.read_csv(csv_file)

    # Inspect the raw DATE_TIME column first
    print("Last 5 rows of the DATE_TIME column (raw):")
    print(df[date_column].tail())  # Inspect last 5 rows

    # Now try to convert DATE_TIME to datetime format
    print("\nAttempting to convert DATE_TIME column to datetime format...")
    df = add_datetime_index(df=df, date_index_name=date_column, date_format="%Y-%m-%d %H:%M:%S")

    print("\nLast 5 rows of the DATE_TIME column (converted):")
    print(df.index[-5:])  # Print the last 5 datetime index values

except Exception as e:
    print(f"\nError encountered: {e}")
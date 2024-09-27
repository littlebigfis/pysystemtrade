import pandas as pd


def clean_dates_in_csv(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)

    # The date column is named 'DATE_TIME'
    date_column = 'DATE_TIME'

    # Strip any leading/trailing whitespace from the date strings
    df[date_column] = df[date_column].str.strip()

    # Convert the date strings to datetime objects to ensure format consistency
    df[date_column] = pd.to_datetime(df[date_column], format='%m/%d/%y %H:%M', errors='coerce')

    # Optional: Drop any rows where the date conversion failed (if any)
    df = df.dropna(subset=[date_column])

    # Write the cleaned data back to the same CSV file, overwriting the original
    df.to_csv(file_path, index=False)

    print(f"Data cleaned and saved back to {file_path}")


# Specify the path to your CSV file
file_path = '/Users/chrishuber/src/lbf/data/huber_futures/fx_prices_csv/AUDUSD.csv'

# Call the function to clean the CSV
clean_dates_in_csv(file_path)
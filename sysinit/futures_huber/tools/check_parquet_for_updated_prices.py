import pandas as pd


def check_parquet_file(file_path):
    """
    Check and print all the data in a specific Parquet file along with the index.

    :param file_path: The path to the Parquet file
    """
    try:
        # Read the Parquet file
        df = pd.read_parquet(file_path)

        # Print all the data in the file
        print("\nFull data in the Parquet file:")
        print(df.to_string())  # Prints the entire DataFrame to the console

        # Print the index (date or timestamp info)
        print("\nIndex (date or timestamp info):")
        print(df.index)

        # Print columns info
        print("\nColumns available in the data:")
        print(df.columns)

    except Exception as e:
        print(f"Error reading the Parquet file: {e}")


file_path = '/Users/chrishuber/src/parquet/futures_contract_prices/AEX#20241100.parquet'  # Update with the path to your specific Parquet file
check_parquet_file(file_path)


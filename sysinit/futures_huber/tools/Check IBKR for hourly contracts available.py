from ib_insync import *
import datetime

# Variables you can modify
IBKR_HOST = '127.0.0.1'  # Host for IBKR connection
IBKR_PORT = 4001  # Port for IB Gateway or TWS
CLIENT_ID = 1  # Unique client ID for the session
INSTRUMENT_CODE = 'EOE'  # Futures symbol
EXPIRY = '202410'  # Expiry date in yyyymm format
EXCHANGE = 'FTA'  # Exchange for the futures contract
CURRENCY = 'EUR'  # Currency for the futures contract
MULTIPLIER = '200'  # Multiplier for the contract (AEX futures is typically 200)
TRADING_CLASS = ''  # Trading class, if known (optional)

# Connect to IBKR Gateway or TWS
ib = IB()
ib.connect(IBKR_HOST, IBKR_PORT, clientId=CLIENT_ID)

def check_hourly_data(instrument_code, expiry):
    # Define the futures contract, adding the exchange, currency, multiplier, and trading class
    contract = Future(
        symbol=instrument_code,
        lastTradeDateOrContractMonth=expiry,
        exchange=EXCHANGE,
        currency=CURRENCY,
        multiplier=MULTIPLIER,
        tradingClass=TRADING_CLASS if TRADING_CLASS else None  # Include trading class only if provided
    )

    # Query for historical hourly data (for a limited date range to avoid too large requests)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=30)  # Fetch data for the past 30 days

    print(f"Fetching hourly data for {instrument_code} with expiry {expiry}")
    try:
        # Request hourly historical data
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_date,
            durationStr='30 D',
            barSizeSetting='1 hour',
            whatToShow='TRADES',
            useRTH=True,  # Set to False if you want outside regular trading hours
            formatDate=1
        )

        # If data is available, print a summary
        if bars:
            print(f"Found {len(bars)} hours of data for {instrument_code} (expiry: {expiry})")
            for bar in bars:
                print(bar.date, bar.open, bar.high, bar.low, bar.close)
        else:
            print(f"No hourly data available for {instrument_code} (expiry: {expiry})")

    except Exception as e:
        print(f"Error fetching data: {e}")

# Example usage: check hourly data for the EOE contract expiring in October 2024
check_hourly_data(INSTRUMENT_CODE, EXPIRY)

# Disconnect from IBKR after fetching the data
ib.disconnect()
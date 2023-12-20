import asyncio
import aiohttp
import pandas as pd
import websocket
import ssl
import json
import sqlite3
from datetime import datetime


# Store 20 popular pairs, available on both Binance and Kucoin
trading_pairs = [
    "BTC-USDT",
    # "ETH-USDT",
    # "SOL-USDT",
    # "INJ-USDT",
    # "BTC-USDC",
    # "AVAX-USDT",
    # "ADA-USDT",
    # "BONK-USDT",
    # "TIA-USDT",
    # "XRP-USDT",
    # "ETH-USDC",
    # "FET-USDT",
    # "JTO-USDT",
    # "MATIC-USDT",
    # "DOT-USDT",
    # "RUNE-USDT",
    # "ATOM-USDT",
    # "LINK-USDT",
    # "ETH-BTC",
    # "DOGE-USDT",
]

# Binance websocket URL
# ticker refreshes every second, bookTicker updates in real time

binance_url = "wss://stream.binance.com:9443/ws/{}@ticker"


binance_results_table = []

ws = None  # Single websocket connection object


# Establish connection with database
pool = sqlite3.connect("market_data.db")

# Create the table if it doesn't exist (with appropriate data types)
with pool:
    cursor = pool.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_data (
            date DATETIME,
            source TEXT,
            trading_pair TEXT,
            spread REAL,
            buy_order_slippage REAL,
            sell_order_slippage REAL
        )
    """
    )


# Function to append data efficiently using batch inserts
def append_data(entries):
    with pool:
        cursor = pool.cursor()
        print(f"Inserting data into the database: {entries}")

        cursor.executemany(
            """
            INSERT INTO market_data (date, source, trading_pair, spread, buy_order_slippage, sell_order_slippage)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            entries,
        )
        print("Data inserted successfully.")


def on_open(ws):
    print("### Connection Opened ###")


def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")


def on_error(ws, error):
    print(f"The error message is {error}")

    ws.close()


db_entries = []


def on_message(ws, message):
    global db_entries
    data = json.loads(message)  # Parse JSON message

    print(f"Processed data for: {data}")

    # Get precise time of the data in unix time then covert to datetime object for the database. We divide unix_date by 1000 since it's provided in milliseconds and we need it in seconds
    unix_date = data["E"]
    date = datetime.fromtimestamp(unix_date / 1000)

    # Get trading pair
    trading_pair = data["s"]

    # Extract last price
    last_price = float(data["c"])
    print("Last price is:", last_price)

    # Extract best ask
    best_ask = float(data["a"])
    print("Best ask is:", best_ask)

    # Extract best bid
    best_bid = float(data["b"])
    print("Best bid is:", best_bid)

    # Calculate spread
    # Spread is the difference between the best ask and bid
    spread = ((best_ask - best_bid) / best_ask) * 100

    # Slippage for buy orders
    slippage_percentage_buy = ((last_price - best_ask) / best_ask) * 100

    # Slippage for sell orders
    slippage_percentage_sell = ((last_price - best_bid) / best_bid) * 100

    # Store the entry in a tuple
    spread_entry = (
        date,
        "KuCoin",
        trading_pair,
        spread,
        slippage_percentage_buy,
        slippage_percentage_sell,
    )

    # Add results to database
    # Buffer entries for batch insertion
    db_entries.append(spread_entry)

    # Insert every 5 seconds
    if len(db_entries) >= 5:
        append_data(db_entries)
        db_entries = []  # Clear the buffer

    # Do this just once for testing
    # ws.close()


def get_binance_tasks():
    # Start the websocket connection with handlers
    global ws
    for pair in trading_pairs:
        binance_pair = pair.replace("-", "").lower()
        ws = websocket.WebSocketApp(
            url=binance_url.format(binance_pair),
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            on_message=on_message,
        )
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


def main():
    get_binance_tasks()


if __name__ == "__main__":
    main()

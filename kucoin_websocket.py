import asyncio
import websockets
import ssl
import json
import sqlite3
from datetime import datetime
import requests


# Store 20 popular pairs, available on both Kucoin and Kucoin
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

# Kucoin websocket URL
# ticker refreshes every second, bookTicker updates in real time
# Using the ticker in this case to avoid frying my hardware

kucoin_url = "wss://ws-api-spot.kucoin.com/market/ticker:{}?token=2neAiuYvAU61ZDXANAGAsiL4-iAExhsBXZxftpOeh_55i3Ysy2q2LEsEWU64mdzUOPusi34M_wGoSf7iNyEWJ9gDkELGuNf9D28R6CUFDAEkXnY7BeaM69iYB9J6i9GjsxUuhPw3BlrzazF6ghq4L5cGbq4apP9OyMXVCWwjhsg=.WGxzfigRuE8puTaCMk6gdA=="

# Establish connection with database
pool = sqlite3.connect("kucoin_market_data.db")

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

        cursor.executemany(
            """
            INSERT INTO market_data (date, source, trading_pair, spread, buy_order_slippage, sell_order_slippage)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            entries,
        )


def on_open(ws):
    print("### Connection Opened ###")


def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")


def on_error(ws, error):
    print(f"The error message is {error}")

    ws.close()


db_entries = []


async def on_message(ws, message):
    global db_entries
    data = json.loads(message)  # Parse JSON message
    print(f"the data is {data}")

    # Get precise time of the data in unix time then covert to datetime object for the database. We divide unix_date by 1000 since it's provided in milliseconds and we need it in seconds
    date = datetime.now()

    # Get trading pair
    # The topic contains the trading pair after a colon
    topic = data["topic"]
    trading_pair = topic.split(":")[1]

    # Extract last price
    last_price = float(data["data"]["price"])

    # Extract best ask
    best_ask = float(data["data"]["bestAsk"])

    # Extract best bid
    best_bid = float(data["data"]["bestBid"])

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
        "Kucoin",
        trading_pair,
        spread,
        slippage_percentage_buy,
        slippage_percentage_sell,
    )

    # Buffer entries for batch insertion
    db_entries.append(spread_entry)


async def connect_to_kucoin(pair):
    uri = kucoin_url.format(pair)
    print(uri)
    async with websockets.connect(uri, ssl=ssl.SSLContext()) as ws:
        await asyncio.gather(listen_kucoin(ws), write_to_database(ws))


async def listen_kucoin(ws):
    while True:
        try:
            message = await ws.recv()
            await on_message(ws, message)
        except websockets.exceptions.ConnectionClosed:
            break


async def write_to_database(ws):
    while True:
        await asyncio.sleep(5)  # Interestingly, the app doesn't work without this line
        global db_entries
        if db_entries:
            append_data(db_entries)
            db_entries = []


async def get_kucoin_tasks():
    tasks = [connect_to_kucoin(pair) for pair in trading_pairs]
    await asyncio.gather(*tasks)


async def main():
    # Open connection with KuCoin
    connect = requests.post("https://api.kucoin.com/api/v1/bullet-public")
    connect_response = connect.json()
    token = connect_response["data"]["token"]
    print(f"Token is", token)

    await get_kucoin_tasks()


if __name__ == "__main__":
    asyncio.run(main())


"welcome message: nOoY1IK608"

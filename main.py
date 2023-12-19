import asyncio
import aiohttp
import pandas as pd
import websocket
import ssl
import json

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

binance_url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"


binance_results_table = []
ws = None  # Single websocket connection object


def on_open(ws):
    print("### Connection Opened ###")
    # Subscribe to channels for all trading pairs
    for pair in trading_pairs:
        binance_pair = pair.replace("-", "").lower()
        print(binance_pair)


def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")


def on_error(ws, error):
    print(f"The error message is {error}")

    ws.close()


def on_message(ws, message):
    data = json.loads(message)  # Parse JSON message

    print(f"Processed data for: {data}")

    # Extract last price
    last_price = data["c"]
    print("Last price is: {}", last_price)

    # Extract best ask
    best_ask = data["a"]
    print("Best ask is: {}", best_ask)

    # extract best bid
    best_bid = data["b"]
    print("Best bid is: {}", best_bid)

    # Calculate spread

    # Calculate slippage

    # Add results to database

    # Do this just once for testing
    ws.close()


def get_binance_tasks():
    # Start the websocket connection with handlers
    global ws
    ws = websocket.WebSocketApp(
        url=binance_url,
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

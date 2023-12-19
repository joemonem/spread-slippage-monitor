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
binance_url = "wss://stream.binance.com:9443/ws/{}@ticker"

binance_results_table = []
ws = None  # Single websocket connection object


def on_open(ws):
    print("### Connection Opened ###")
    # Subscribe to channels for all trading pairs
    for pair in trading_pairs:
        binance_pair = pair.replace("-", "").lower()
        print(binance_pair)
        ws.subscribe(binance_url.format(binance_pair))


def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")


def on_error(ws, error):
    print(f"The error message is {error}")


def on_message(ws, message):
    data = json.loads(message)  # Parse JSON message
    # Extract relevant data and perform analysis/store in results table
    # ... Implement data processing logic
    print(f"Processed data for: {data['symbol']}")


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

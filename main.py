import asyncio
import aiohttp
import csv

# Store 20 popular pairs, available on Binance and Kucoin
kucoin_trading_pairs = [
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "INJ-USDT",
    "BTC-USDC",
    "AVAX-USDT",
    "ADA-USDT",
    "BONK-USDT",
    "TIA-USDT",
    "XRP-USDT",
    "ETH-USDC",
    "FET-USDT",
    "JTO-USDT",
    "MATIC-USDT",
    "DOT-USDT",
    "RUNE-USDT",
    "ATOM-USDT",
    "LINK-USDT",
    "ETH-BTC",
    "DOGE-USDT",
]

# Kucoin ticker GET request
kucoin_url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}"

# Binance
binance_url = "https://api.binance.com/api/v3/ticker/24hr?symbol={}"


kucoin_results = []
kucoin_results_table = []
binance_results_table = []


def get_kucoin_tasks(session):
    task_kucoin = []

    for trading_pair in kucoin_trading_pairs:
        task_kucoin.append(
            asyncio.create_task(
                fetch_kucoin(session, kucoin_url.format(trading_pair), trading_pair)
            )
        )

    return task_kucoin


def get_binance_tasks(session):
    task_binance = []

    for trading_pair in kucoin_trading_pairs:
        # Binance's format is BTCUSDT instead of BTC-USDT
        binance_pair = trading_pair.replace("-", "")

        task_binance.append(
            asyncio.create_task(
                fetch_binance(session, binance_url.format(binance_pair), binance_pair)
            )
        )

    return task_binance


async def fetch_kucoin(session, url, trading_pair):
    async with session.get(url, ssl=False) as response:
        result = await response.json()

        best_bid = float(result["data"]["bestBid"])
        best_ask = float(result["data"]["bestAsk"])

        spread = ((best_ask - best_bid) / best_ask) * 100

        execution_price = float(result["data"]["price"])

        # Slippage for buy orders
        slippage_percentage_buy = ((execution_price - best_ask) / best_ask) * 100

        # Slippage for sell orders
        slippage_percentage_sell = ((execution_price - best_bid) / best_bid) * 100

        spread_entry = {
            "source": "KuCoin",
            "trading_pair": trading_pair,
            "spread": spread,
            "buy_order_slippage": slippage_percentage_buy,
            "sell_order_slippage": slippage_percentage_sell,
        }

        # kucoin_results.append(result)
        kucoin_results_table.append(spread_entry)


async def fetch_binance(session, url, trading_pair):
    async with session.get(url, ssl=False) as response:
        result = await response.json()

        best_bid = float(result["bidPrice"])
        best_ask = float(result["askPrice"])

        spread = ((best_ask - best_bid) / best_ask) * 100

        execution_price = float(result["lastPrice"])

        # Slippage for buy orders
        slippage_percentage_buy = ((execution_price - best_ask) / best_ask) * 100

        # Slippage for sell orders
        slippage_percentage_sell = ((execution_price - best_bid) / best_bid) * 100

        spread_entry = {
            "source": "Binance",
            "trading_pair": trading_pair,
            "spread": spread,
            "buy_order_slippage": slippage_percentage_buy,
            "sell_order_slippage": slippage_percentage_sell,
        }

        # kucoin_results.append(result)
        binance_results_table.append(spread_entry)


async def get_kucoin():
    async with aiohttp.ClientSession() as session:
        # Kucoin result's index is 0

        tasks = get_kucoin_tasks(session)
        await asyncio.gather(*tasks)


async def get_binance():
    async with aiohttp.ClientSession() as session:
        # Binance result's index is 1
        tasks = get_binance_tasks(session)
        await asyncio.gather(*tasks)


async def main():
    await get_kucoin()
    await get_binance()

    with open("kucoin_output.csv", "w", newline="") as csvfile:
        fieldnames = kucoin_results_table[
            0
        ].keys()  # Assuming all dictionaries in the list have the same keys
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in kucoin_results_table:
            writer.writerow(row)

    with open("binance_output.csv", "w", newline="") as csvfile:
        fieldnames = binance_results_table[
            0
        ].keys()  # Assuming all dictionaries in the list have the same keys
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in binance_results_table:
            writer.writerow(row)


asyncio.run(main())

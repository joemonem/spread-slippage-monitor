import asyncio
import aiohttp
import os
import csv


# Store the top 20 pairs on KuCoin
kucoin_pairs = [
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
    "SATS-USDT",
    "RATS-USDT",
    "RUNE-USDT",
    "ATOM-USDT",
    "LINK-USDT",
    "ETH-BTC",
    "DOGE-USDT",
]

# Kucoin ticker GET request
kucoin_url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}"

kucoin_results = []
results_table = []


def get_tasks(session):
    task_kucoin = []

    for trading_pair in kucoin_pairs:
        task_kucoin.append(
            asyncio.create_task(
                fetch_kucoin(session, kucoin_url.format(trading_pair), trading_pair)
            )
        )

    return task_kucoin


async def fetch_kucoin(session, url, trading_pair):
    async with session.get(url, ssl=False) as response:
        result = await response.json()

        best_bid = float(result["data"]["bestBid"])
        best_ask = float(result["data"]["bestAsk"])
        spread = ((best_ask - best_bid) / best_ask) * 100

        spread_entry = {
            "source": "KuCoin",
            "trading_pair": trading_pair,
            "spread": spread,
        }

        kucoin_results.append(result)
        results_table.append(spread_entry)


async def get_kucoin():
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session)
        await asyncio.gather(*tasks)


async def main():
    kucoin_info = await get_kucoin()
    best_bid = float(kucoin_results[0]["data"]["bestBid"])
    best_ask = float(kucoin_results[0]["data"]["bestAsk"])

    spread = ((best_ask - best_bid) / best_ask) * 100

    # spread_entry = {
    #     "source": source,
    #     "trading_pair": trading_pair,
    #     "spread": spread,
    # }

    # print(
    #     f"The best bid is {best_bid}, the best ask is {best_ask}, and the spread is {spread}"
    # )
    # print(f"Here are the results in table form: {results_table}")
    with open("output.csv", "w", newline="") as csvfile:
        fieldnames = results_table[
            0
        ].keys()  # Assuming all dictionaries in the list have the same keys
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in results_table:
            writer.writerow(row)


asyncio.run(main())

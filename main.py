import asyncio
import aiohttp
import os


# Store the top 20 pairs on KuCoin
kucoin_pairs = [
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
    # "SATS-USDT",
    # "RATS-USDT",
    # "RUNE-USDT",
    # "ATOM-USDT",
    # "LINK-USDT",
    # "ETH-BTC",
    # "DOGE-USDT",
]

# Kucoin ticker GET request
kucoin_url = "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={}"

kucoin_results = []


def get_tasks(session):
    task_kucoin = []

    for symbol in kucoin_pairs:
        task_kucoin.append(
            asyncio.create_task(session.get(kucoin_url.format(symbol), ssl=False))
        )

    return task_kucoin


async def get_kucoin():
    async with aiohttp.ClientSession() as session:
        tasks = get_tasks(session)
        responses = await asyncio.gather(*tasks)
        for response in responses:
            kucoin_results.append(await response.json())


async def main():
    kucoin_info = await get_kucoin()
    best_bid = float(kucoin_results[0]["data"]["bestBid"])
    best_ask = float(kucoin_results[0]["data"]["bestAsk"])

    spread = best_ask - best_bid

    print(
        f"The best bid is {best_bid}, the best ask is {best_ask}, and the spread is {spread}"
    )


asyncio.run(main())

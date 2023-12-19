# spread-slippage-monitor

# Async
This is an application where async programming shines. The api calls we're making aren't interdependant, so we can run them asynchronously. Time is an extremely valuable resource in financial markets, and running those calls asynchronously saves a lot it.

# General Structure 
I've collected 20 popular markets that are available on both KuCoin and Binance, and each platform's api request URL.
Then I loop through each market, adding a task during each iteration.
The tasks are then executed and their results are stored in a list of dictionaries.
That list is then turned into a Pandas DataFrame which will facilitates future data analysis. 

# Data Analysis
The spread and slippage were generally low which is expected for popular and established markets. Those figures would've most likely been higher if we were dealing with more volatile and less known markets. 
Keeping track of the source of the result is important, so each entry has: source (Binance or KuCoin), trading_pair (market), spread, slippage from the buy order POV, and slippage from the sell order POV.
The source and trading_pair are set as indexes for the DataFrame since they'll be the most useful for filtering the data.

# Difficulties 
Had to get familiar with Asyncio, since using regular requests was much slower. Also had to get familiar with Pandas since simply outputing the data in csv format isn't optimal for future data analysis.



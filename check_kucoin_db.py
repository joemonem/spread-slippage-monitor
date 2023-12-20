import sqlite3

# Check on the results of the database

with sqlite3.connect("kucoin_market_data.db") as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM market_data")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

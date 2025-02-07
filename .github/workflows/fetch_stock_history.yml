import os
import yfinance as yf
from supabase import create_client
from datetime import datetime

# Supabase credentials (stored in GitHub Secrets or local .env)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials missing")

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# List of stock symbols to fetch
STOCKS = ["AAPL", "GOOGL", "AMZN", "TSLA", "MSFT"]

def fetch_and_store():
    for symbol in STOCKS:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1d")  # Fetch latest daily data

        if not hist.empty:
            latest = hist.iloc[-1]  # Get the latest row
            data = {
                "date": latest.name.strftime("%Y-%m-%d"),
                "open": float(latest["Open"]),
                "close": float(latest["Close"]),
                "high": float(latest["High"]),
                "low": float(latest["Low"]),
                "volume": int(latest["Volume"]),
                "created_at": datetime.utcnow().isoformat()
            }

            # Insert into Supabase
            response = supabase.table("stocks_history").insert(data).execute()
            print(f"Inserted {symbol} OHLC data:", response.data)

if __name__ == "__main__":
    fetch_and_store()


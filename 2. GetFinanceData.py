import yfinance as yf
import pandas as pd
from datetime import datetime

# --- 1. Define Your List of Valid Tickers ---
valid_tickers = [
    "NVDA", "TSLA", "AAPL", "TGT", "DJT", "AMD", "AMZN", "MSFT", "CVI", "MSTR",
    "LUNR", "RKLB", "ACHR", "SMCI", "ASTS", "NDAQ", "SGHC", "PLTR", "BA", "SCI", "INTC", "BYON",
    "BBY", "META", "AMC", "QTWO", "LTH", "GOOG", "CRWD", "GOOGL", "IAUX", "NFLX", "HOOD", "AVGO",
    "CVNA", "WMT", "BPOP", "HIMS", "SBUX", "BILL", "NKE", "MU", "DOW", "SOFI", "RSI", "XYZ", "RIVN",
    "MARA", "GPI", "HNST", "MTCH", "LLY", "MCD", "COIN", "MS", "PYPL", "ORCL", "BFC", "FRBA", "SOUN",
    "GS", "CELH", "BAC", "LCID", "COF", "FISI", "GM", "SNOW", "WEN", "BLK", "JPM", "GRND", "GME", "RKT",
    "CRM", "RGTI", "BKE", "UNH", "MMM", "AFG", "QCOM", "ADBE", "LULU", "CAVA", "IONQ", "DTE", "PPL",
    "BYD", "PL", "FCF", "PRO"
]

# --- 2. Set Date Range ---
start_date = "2024-04-01"
end_date = "2025-03-31"

# --- 3. Download Helper ---
def download_prices(ticker_list):
    data = yf.download(
        tickers=ticker_list,
        start=start_date,
        end=end_date,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True,
        progress=True
    )
    return data

# --- 4. Chunk Helper ---
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# --- 5. Download and Collect Closing Prices and Volume ---
close_data = []
volume_data = []

for chunk in chunk_list(valid_tickers, 50):  # Smaller chunk to reduce risk of throttling
    chunk_data = download_prices(chunk)
    for ticker in chunk:
        try:
            df = chunk_data[ticker][['Close', 'Volume']].copy()
            df_close = df[['Close']].rename(columns={"Close": ticker})
            df_volume = df[['Volume']].rename(columns={"Volume": ticker})
            close_data.append(df_close)
            volume_data.append(df_volume)
        except KeyError:
            print(f"⚠️ Missing data for {ticker}")

# --- 6. Merge All Into Two Separate DataFrames ---
closing_prices_df = pd.concat(close_data, axis=1)
volumes_df = pd.concat(volume_data, axis=1)

# --- 7. Save to CSV ---
closing_prices_df.to_csv("valid_tickers_closing_prices.csv")
volumes_df.to_csv("valid_tickers_volumes.csv")
print("✅ Done! Saved 'valid_tickers_closing_prices.csv' and 'valid_tickers_volumes.csv'")

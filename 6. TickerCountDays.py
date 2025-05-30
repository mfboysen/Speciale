import pandas as pd
import ast

# --- Load Data ---
submissions = pd.read_csv("wsb_arcticshift_submissions2023.csv")
comments = pd.read_csv("wsb_arcticshift_comments2023.csv")

# --- Define Valid Tickers ---
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

# --- Process Comments ---
comments["comment_created_utc"] = pd.to_datetime(comments["comment_created_utc"], unit="s", utc=True)
comments["comment_created_est"] = comments["comment_created_utc"].dt.tz_convert("US/Eastern")
comments["comment_date"] = comments["comment_created_est"].dt.date

# Ensure ticker list is parsed correctly
comments["tickers_mentioned"] = comments["tickers_mentioned"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)

# Explode tickers into individual rows
comments_exploded = comments.explode("tickers_mentioned")
comments_exploded = comments_exploded.dropna(subset=["tickers_mentioned"])
comments_exploded["tickers_mentioned"] = comments_exploded["tickers_mentioned"].str.upper().str.strip()

# Filter for valid tickers
comments_exploded = comments_exploded[comments_exploded["tickers_mentioned"].isin(valid_tickers)]

# Group by date and ticker
comment_mentions_daily = (
    comments_exploded.groupby(["comment_date", "tickers_mentioned"])
    .size()
    .reset_index(name="comment_mentions")
)

# --- Process Submissions ---
submissions["companies_mentioned"] = submissions["companies_mentioned"].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
)

# Explode tickers into individual rows
submissions_exploded = submissions.explode("companies_mentioned")
submissions_exploded = submissions_exploded.dropna(subset=["companies_mentioned"])
submissions_exploded["companies_mentioned"] = submissions_exploded["companies_mentioned"].str.upper().str.strip()

# Filter for valid tickers
submissions_exploded = submissions_exploded[submissions_exploded["companies_mentioned"].isin(valid_tickers)]

# Group by date and ticker
post_mentions_daily = (
    submissions_exploded.groupby(["date_est", "companies_mentioned"])
    .size()
    .reset_index(name="post_mentions")
    .rename(columns={"date_est": "date", "companies_mentioned": "ticker"})
)

# --- Format comment_mentions_daily to match ---
comment_mentions_daily = comment_mentions_daily.rename(columns={
    "comment_date": "date",
    "tickers_mentioned": "ticker"
})

# --- Fix date formatting ---
# Ensure 'date_est' from submissions is in datetime.date format
submissions_exploded["date_est"] = pd.to_datetime(submissions_exploded["date_est"]).dt.date
submissions_exploded = submissions_exploded[submissions_exploded["companies_mentioned"].isin(valid_tickers)]

# Re-group post mentions with cleaned date format
post_mentions_daily = (
    submissions_exploded.groupby(["date_est", "companies_mentioned"])
    .size()
    .reset_index(name="post_mentions")
    .rename(columns={"date_est": "date", "companies_mentioned": "ticker"})
)

# Make sure comment dates are also datetime.date type
comment_mentions_daily["date"] = pd.to_datetime(comment_mentions_daily["date"]).dt.date

# --- Merge on cleaned 'date' and 'ticker' columns ---
combined_mentions_daily = pd.merge(
    post_mentions_daily,
    comment_mentions_daily,
    on=["date", "ticker"],
    how="outer"
).fillna(0)

# Convert counts to integers
combined_mentions_daily["post_mentions"] = combined_mentions_daily["post_mentions"].astype(int)
combined_mentions_daily["comment_mentions"] = combined_mentions_daily["comment_mentions"].astype(int)
combined_mentions_daily["total_mentions"] = (
    combined_mentions_daily["post_mentions"] + combined_mentions_daily["comment_mentions"]
)

# --- Save Outputs ---
comment_mentions_daily.to_csv("ticker_mentions_from_comments_daily.csv", index=False)
post_mentions_daily.to_csv("ticker_mentions_from_posts_daily.csv", index=False)
combined_mentions_daily.to_csv("ticker_mentions_combined_daily.csv", index=False)

print("✅ Saved ticker_mentions_from_comments_daily.csv")
print("✅ Saved ticker_mentions_from_posts_daily.csv")
print("✅ Saved: ticker_mentions_combined_daily.csv")
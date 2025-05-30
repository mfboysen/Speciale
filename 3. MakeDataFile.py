import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import ast
import pandas_market_calendars as mcal

# Load data
comments = pd.read_csv("comments_with_consensus.csv")
submissions = pd.read_csv("submissions_with_consensus.csv")
closing_price = pd.read_csv("valid_tickers_closing_prices.csv", parse_dates=["Date"])
volume = pd.read_csv("valid_tickers_volumes.csv", parse_dates=["Date"])

# Preprocess mentions
comments["tickers_mentioned"] = comments["tickers_mentioned"].apply(
    lambda x: ast.literal_eval(x) if pd.notnull(x) and isinstance(x, str) else []
)
submissions["companies_mentioned"] = submissions["companies_mentioned"].apply(
    lambda x: ast.literal_eval(x) if pd.notnull(x) and isinstance(x, str) else []
)

# Convert UTC to EST and extract date
eastern = pytz.timezone('US/Eastern')
comments['post_created_utc'] = pd.to_datetime(comments['post_created_utc'], unit='s', utc=True)
comments['date_est'] = comments['post_created_utc'].dt.tz_convert(eastern).dt.date
comments["date"] = comments["date_est"]
submissions["date"] = pd.to_datetime(submissions["datetime_est"]).dt.date

# Explode ticker mentions
comment_ticker = comments.explode("tickers_mentioned")
comment_ticker = comment_ticker[comment_ticker["tickers_mentioned"].notnull()]
submission_ticker = submissions.explode("companies_mentioned")
submission_ticker = submission_ticker[submission_ticker["companies_mentioned"].notnull()]

# Rename
comment_ticker = comment_ticker.rename(columns={"tickers_mentioned": "ticker", "consensus_score": "consensus"})
submission_ticker = submission_ticker.rename(columns={"companies_mentioned": "ticker", "consensus_score": "consensus"})

# Sentiment mapping
sentiment_mapping = {"positive": 1, "neutral": 0, "negative": -1}
comment_ticker["consensus_numeric"] = comment_ticker["consensus"].map(sentiment_mapping)
submission_ticker["consensus_numeric"] = submission_ticker["consensus"].map(sentiment_mapping)
comment_ticker["score"] = comment_ticker["comment_score"]

# Combine and enrich
combined = pd.concat([
    comment_ticker[["date", "ticker", "consensus", "consensus_numeric", "score"]],
    submission_ticker[["date", "ticker", "consensus", "consensus_numeric", "score", "num_comments", "link_flair_text"]]
])

combined["like_score_positive"] = combined.apply(
    lambda row: row["score"] if row["consensus"] == "positive" else 0, axis=1
)
combined["like_score_negative"] = combined.apply(
    lambda row: row["score"] if row["consensus"] == "negative" else 0, axis=1
)

# Aggregation
def calc_sentiment_metrics(df):
    grouped = df.groupby(["date", "ticker"])
    result = grouped.agg(
        no_positive_consensus=("consensus", lambda x: (x == "positive").sum()),
        no_neutral_consensus=("consensus", lambda x: (x == "neutral").sum()),
        no_negative_consensus=("consensus", lambda x: (x == "negative").sum()),
        like_score_positive=("like_score_positive", "sum"),
        like_score_negative=("like_score_negative", "sum"),
        avg_num_comments=("num_comments", "mean"),
        most_mentioned_link_flair_text=("link_flair_text", lambda x: x.mode().iloc[0] if not x.mode().empty else None),
        number_of_mentions=("ticker", "count")
    ).reset_index()

    result["ticker_consensus_label"] = result.apply(
        lambda row: "positive" if row["no_positive_consensus"] > row["no_negative_consensus"]
        else "negative" if row["no_negative_consensus"] > row["no_positive_consensus"]
        else "equal",
        axis=1
    )
    return result

ticker_sentiment = calc_sentiment_metrics(combined)

# General sentiment
comments_general = comments[comments["tickers_mentioned"].apply(lambda x: isinstance(x, list) and len(x) == 0)]
submissions_general = submissions[submissions["companies_mentioned"].apply(lambda x: isinstance(x, list) and len(x) == 0)]
combined_general = pd.concat([
    comments_general[["date", "consensus_score"]],
    submissions_general[["date", "consensus_score"]]
]).rename(columns={"consensus_score": "consensus"})

general_sentiment = combined_general.groupby("date").agg(
    no_positive_consensus_general=("consensus", lambda x: (x == "positive").sum()),
    no_neutral_consensus_general=("consensus", lambda x: (x == "neutral").sum()),
    no_negative_consensus_general=("consensus", lambda x: (x == "negative").sum())
).reset_index()

general_sentiment["general_consensus_label"] = general_sentiment.apply(
    lambda row: "positive" if row["no_positive_consensus_general"] > row["no_negative_consensus_general"]
    else "negative" if row["no_negative_consensus_general"] > row["no_positive_consensus_general"]
    else "equal",
    axis=1
)

# Merge ticker + general sentiment
features = ticker_sentiment.merge(general_sentiment, on="date", how="left")

# Reshape market data to long format
volume = volume.melt(id_vars=["Date"], var_name="ticker", value_name="volume")
closing_price = closing_price.melt(id_vars=["Date"], var_name="ticker", value_name="closing_price")

# Normalize dates for safe merging
closing_price["Date"] = pd.to_datetime(closing_price["Date"]).dt.normalize()
volume["Date"] = pd.to_datetime(volume["Date"]).dt.normalize()

# Merge price and volume
market = closing_price.merge(volume, on=["Date", "ticker"], how="outer")

# Get valid trading days (timezone-aware → timezone-naive)
nyse = mcal.get_calendar('NYSE')
valid_trading_days = nyse.valid_days(
    start_date=market["Date"].min().strftime('%Y-%m-%d'),  # FIXED here
    end_date=market["Date"].max().strftime('%Y-%m-%d')     # FIXED here
)
valid_trading_days = valid_trading_days.tz_convert(None).normalize()

# Normalize and rename market date column
market["Date"] = pd.to_datetime(market["Date"]).dt.normalize()
market = market[market["Date"].isin(valid_trading_days)]
market = market.rename(columns={"Date": "date"})

# Create full date × ticker panel
all_dates = valid_trading_days
all_tickers = market["ticker"].unique()
full_index = pd.MultiIndex.from_product([all_dates, all_tickers], names=["date", "ticker"])
market = market.set_index(["date", "ticker"]).reindex(full_index).reset_index()

# Forward-fill price and volume
market = market.sort_values(by=["ticker", "date"])
market[["closing_price", "volume"]] = market.groupby("ticker")[["closing_price", "volume"]].ffill()

# Compute target
market["closing_price_next_day"] = market.groupby("ticker")["closing_price"].shift(-1)
market["target"] = (market["closing_price_next_day"] > market["closing_price"]).astype(int)

# Merge market data with sentiment features
features["date"] = pd.to_datetime(features["date"])
final = market.merge(features, on=["date", "ticker"], how="left")

# Fill missing sentiment data with 0 or neutral
sentiment_cols = [
    "no_positive_consensus", "no_neutral_consensus", "no_negative_consensus",
    "like_score_positive", "like_score_negative", "avg_num_comments",
    "number_of_mentions", "no_positive_consensus_general", "no_neutral_consensus_general",
    "no_negative_consensus_general"
]
final[sentiment_cols] = final[sentiment_cols].fillna(0)
final["ticker_consensus_label"] = final["ticker_consensus_label"].fillna("equal")
final["general_consensus_label"] = final["general_consensus_label"].fillna("equal")

# Export
final.to_csv("final_stock_sentiment_dataset2.csv", index=False)
print("✅ Final dataset saved as 'final_stock_sentiment_dataset2.csv'.")
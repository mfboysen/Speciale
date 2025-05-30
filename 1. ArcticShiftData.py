import requests
import pandas as pd
import time
import datetime as dt
import re
from pytz import timezone
from collections import defaultdict

# --- Config ---
start_date = "2024-04-01"
end_date = "2025-03-31"
subreddit = "wallstreetbets"
eastern = timezone("US/Eastern")

# --- Clean company names ---
remove_words = ["INC", "CORP", "CO", "COM", "LTD", "PLC", "COMPANY", "INCORPORATED", "HOLDINGS", "GROUP", "CLASS A", "CLASS B", "CLASS C", "CVR"]

def clean_name(name):
    if not isinstance(name, str):
        return []
    pattern = r'\b(?:' + '|'.join(remove_words) + r')\b'
    name = re.sub(pattern, '', name.upper())
    name = re.sub(r'[^A-Z0-9 ]', '', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()
    return name, name.split()

# --- Load Russell 3000 ---
companies_df = pd.read_csv("russel_3000.csv", sep=";")
companies_df["CleanNameTuple"] = companies_df["Name"].apply(clean_name)
companies_df["CleanKeywords"] = companies_df["CleanNameTuple"].apply(lambda x: x[1])

# Only include tickers with 2+ characters
company_dict = {
    row["Ticker"]: (row["CleanKeywords"], row["Ticker"])
    for _, row in companies_df.iterrows()
    if isinstance(row["Ticker"], str) and len(row["Ticker"]) >= 2
}

def find_companies(text):
    found = []
    if not isinstance(text, str):
        return found
    text_upper = text.upper()
    for ticker, (keywords, ticker_str) in company_dict.items():
        name_match = all(re.search(rf'\b{re.escape(word)}\b', text_upper) for word in keywords)
        if len(ticker_str) <= 2:
            ticker_match = re.search(rf'\b{re.escape(ticker_str)}\b', text)
        else:
            ticker_match = re.search(rf'\b{re.escape(ticker_str)}\b', text)
        if name_match or ticker_match:
            found.append(ticker)
    return found

# --- Fetch function with pagination ---
def fetch_all_posts_incrementally(url, start_date, end_date):
    all_posts = []
    eastern = timezone("US/Eastern")  # already declared in your script

    # Convert start_date string ("YYYY-MM-DD") to datetime in NY time, then to UTC string
    after_timestamp = (
        eastern.localize(dt.datetime.strptime(start_date, "%Y-%m-%d"))
        .astimezone(dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    before_timestamp = (
        eastern.localize(dt.datetime.strptime(end_date, "%Y-%m-%d"))
        .astimezone(dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")  
    )

    print("🔄 Fetching posts incrementally...")

    while True:
        params = {
            "subreddit": subreddit,
            "after": after_timestamp,
            "before": before_timestamp,
            "limit": "auto",  # Max allowed
            "sort": "asc",
            "sort_type": "created_utc"
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print("Error:", response.text)
            break

        data = response.json().get("data", [])
        if not data:
            break

        all_posts.extend(data)
        print(f"  → Total posts collected: {len(all_posts)}")

        # Get timestamp of the last post
        last_utc = data[-1]["created_utc"]
        last_dt = dt.datetime.fromtimestamp(last_utc, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if last_dt >= end_date:
            break

        after_timestamp = last_dt
        time.sleep(2)

    return all_posts

def get_daily_thread_post_id(submission_url, date):
    # Localize and convert both after and before to UTC full timestamps
    after_timestamp = (
        eastern.localize(dt.datetime.strptime(date, "%Y-%m-%d"))
        .astimezone(dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    before_timestamp = (
        eastern.localize(dt.datetime.strptime(date, "%Y-%m-%d") + dt.timedelta(days=1))
        .astimezone(dt.timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    params = {
        "subreddit": subreddit,
        "after": after_timestamp,
        "before": before_timestamp,
        "limit": 100,
        "sort": "asc",
        "author": "wsbapp",
    }

    response = requests.get(submission_url, params=params)
    if response.status_code == 200:
        for post in response.json().get("data", []):
            if "daily discussion thread" in post.get("title", "").lower():
                return post["id"]
    else:
        print("Error in get_daily_thread_post_id:", response.text)
    return None


def fetch_comments_for_post(comment_url, post_id):
    all_comments = []
    after = None
    MAX_TOTAL_COMMENTS = 5000 
    print(f"🔄 Fetching comments for post: {post_id}")

    while True:
        params = {
            "link_id": f"t3_{post_id}",
            "limit": "auto",  # Max allowed per page
            "sort": "asc",
            "sort_type": "created_utc"  # ✅ FIXED: Use valid value
        }
        if after:
            params["after"] = after

        response = requests.get(comment_url, params=params)
        if response.status_code != 200:
            print("Error:", response.text)
            break

        data = response.json().get("data", [])
        if not data:
            break

        all_comments.extend(data)
        print(f"  → Total comments collected: {len(all_comments)}")

        if len(all_comments) >= MAX_TOTAL_COMMENTS:
            print(f"🚫 Reached max of {MAX_TOTAL_COMMENTS} comments for post {post_id}")
            break

        if len(data) < 100:
            break

        after = data[-1]["created_utc"]
        time.sleep(2)

    return all_comments


# --- Fetch submissions and comments ---
submission_url = "https://arctic-shift.photon-reddit.com/api/posts/search"
comment_url = "https://arctic-shift.photon-reddit.com/api/comments/search"

all_submissions = fetch_all_posts_incrementally(submission_url, start_date, end_date)

from datetime import timedelta

def daterange(start_date_str, end_date_str):
    start_dt = dt.datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = dt.datetime.strptime(end_date_str, "%Y-%m-%d")
    for n in range(int((end_dt - start_dt).days)):
        yield (start_dt + timedelta(n)).strftime("%Y-%m-%d")

all_comments = []

daily_thread_ids = {}
for single_date in daterange(start_date, end_date):
    if single_date not in daily_thread_ids:
        post_id = get_daily_thread_post_id(submission_url, single_date)
        daily_thread_ids[single_date] = post_id
    else:
        post_id = daily_thread_ids[single_date]
    
    if post_id:
        comments = fetch_comments_for_post(comment_url, post_id)
        all_comments.extend(comments)
        time.sleep(2)
    else:
        print(f"❌ No Daily Thread found for {single_date}")


# ✅ Remove duplicate comments (based on comment ID)
all_comments = list({c["id"]: c for c in all_comments}.values())

# group comments by post ID
comments_by_post = defaultdict(list)
for comment in all_comments:
    post_id = comment.get("link_id", "").replace("t3_", "")
    if post_id:
        comments_by_post[post_id].append(comment)

skip_starts = ("Thanks for your submission!",)
skip_contains = ("**User Report**", "I am bot")

# Keep top 200 comments PER post (e.g. per daily discussion thread)
filtered_comments = []

for post_id, comments in comments_by_post.items():
    # Skip low-quality / automated content before scoring
    cleaned_comments = [
        c for c in comments
        if not any(c.get("body", "").startswith(s) for s in skip_starts)
        and not any(s in c.get("body", "") for s in skip_contains)
    ]

    # Sort by score and limit to top 1000
    top_comments = sorted(
        cleaned_comments,
        key=lambda c: c.get("score", 0),
        reverse=True
    )[:200]

    filtered_comments.extend(top_comments)

# Replace all_comments with the reduced filtered list
all_comments = filtered_comments

print(f"✅ Filtered down to {len(all_comments)} top-scoring comments across all posts.")

# --- Process submissions ---
submission_df = pd.DataFrame(all_submissions)
if "removed_by_category" in submission_df.columns:
    submission_df = submission_df[submission_df["removed_by_category"].isnull()]

columns_to_keep = [
    "id", "created_utc", "author", "link_flair_text", "media",
    "no_follow", "num_comments", "permalink", "score", "selftext",
    "title", "upvote_ratio", "url"
]
submission_df = submission_df[[col for col in columns_to_keep if col in submission_df.columns]]

# Convert time
submission_df["created_utc"] = pd.to_datetime(submission_df["created_utc"], unit="s", utc=True)
submission_df["datetime_est"] = submission_df["created_utc"].dt.tz_convert(eastern).dt.strftime("%Y-%m-%d %H:%M:%S")
submission_df["date_est"] = submission_df["created_utc"].dt.tz_convert(eastern).dt.date

# Tickers
submission_df["companies_mentioned"] = submission_df.apply(
    lambda row: list(set(
        find_companies(row.get("title", "")) + find_companies(row.get("selftext", ""))
    )),
    axis=1
)

print(f"✅ Processed {len(submission_df)} submissions.")

# --- Process comments ---
submission_lookup = {
    s["id"]: {
        "title": s.get("title", ""),
        "created_utc": s.get("created_utc")
    }
    for s in all_submissions if "id" in s and "title" in s
}

comment_rows = []
for i, comment in enumerate(all_comments):
    if i % 1000 == 0 and i > 0:
        print(f"Processed {i} comments...")
    author = comment.get("author", "")
    text = comment.get("body", "")
    mentioned = list(set(find_companies(text)))
    # if not mentioned:
    #     continue

    parent_id = comment.get("link_id", "").replace("t3_", "")
    post = submission_lookup.get(parent_id, {})
    if not post:
        continue  # skip if we can't match comment to a post
    post_title = post.get("title", "")
    post_created = post.get("created_utc")
    post_date = (
        dt.datetime.fromtimestamp(post_created, tz=dt.timezone.utc).astimezone(eastern).date()
        if post_created else None
    )

    comment_created = comment.get("created_utc")
    comment_date = (
        dt.datetime.fromtimestamp(comment_created, tz=dt.timezone.utc).astimezone(eastern).strftime("%Y-%m-%d %H:%M:%S")
        if comment_created else None
    )

    comment_rows.append({
    "post_id": parent_id,
    "post_created_utc": post_created,
    "comment_created_utc": comment.get("created_utc"),
    "post_title": post.get("title", ""),
    "comment_score": comment.get("score"),
    "tickers_mentioned": mentioned,
    "body": text
    })

comment_mentions_df = pd.DataFrame(comment_rows)

comment_mentions_df["comment_created_utc"] = pd.to_datetime(comment_mentions_df["comment_created_utc"], unit="s", utc=True)
comment_mentions_df["comment_date"] = comment_mentions_df["comment_created_utc"].dt.tz_convert(eastern).dt.strftime("%Y-%m-%d %H:%M:%S")

comment_mentions_df["post_created_utc"] = pd.to_datetime(comment_mentions_df["post_created_utc"], unit="s", utc=True)
comment_mentions_df["post_date"] = comment_mentions_df["post_created_utc"].dt.tz_convert(eastern).dt.date

comment_mentions_df = pd.DataFrame(comment_rows)

print(f"✅ Processed {len(comment_mentions_df)} comments.")

# --- Save outputs ---
submission_df.to_csv("wsb_arcticshift_submissions2023.csv", index=False)
comment_mentions_df.to_csv("wsb_arcticshift_comments2023.csv", index=False)

print("✅ Saved submissions to: wsb_arcticshift_submissions.csv")
print("✅ Saved comments to: wsb_arcticshift_comments.csv")

# --- Summary of ticker mentions ---
post_mentions = (
    submission_df["companies_mentioned"]
    .explode()
    .dropna()
    .loc[lambda x: x.str.len() >= 2]  # filter tickers with 2+ chars
    .value_counts()
    .reset_index()
)
post_mentions.columns = ["ticker", "post_mentions"]

if not comment_mentions_df.empty and "tickers_mentioned" in comment_mentions_df.columns:
    comment_mentions_exploded = (
        comment_mentions_df[["tickers_mentioned"]]
        .explode("tickers_mentioned")
        .dropna(subset=["tickers_mentioned"])
        .loc[lambda x: x["tickers_mentioned"].str.len() >= 2]  # filter tickers with 2+ chars
    )
    comment_counts = (
        comment_mentions_exploded["tickers_mentioned"]
        .str.upper()
        .value_counts()
        .reset_index()
    )
    comment_counts.columns = ["ticker", "comment_mentions"]
else:
    comment_counts = pd.DataFrame(columns=["ticker", "comment_mentions"])

mention_summary = pd.merge(post_mentions, comment_counts, on="ticker", how="outer")
mention_summary[["post_mentions", "comment_mentions"]] = mention_summary[["post_mentions", "comment_mentions"]].fillna(0)
mention_summary["post_mentions"] = mention_summary["post_mentions"].astype(int)
mention_summary["comment_mentions"] = mention_summary["comment_mentions"].astype(int)
mention_summary["total_mentions"] = mention_summary["post_mentions"] + mention_summary["comment_mentions"]
mention_summary = mention_summary.sort_values("total_mentions", ascending=False)

mention_summary.to_csv("ticker_mentions_summary2023.csv", index=False)
print("✅ Saved summary to: ticker_mentions_summary.csv")


import requests
import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime

# -----------------------------
# Step 1: Setup & API Key
# -----------------------------
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")  # stored in .env file
BASE_URL = "https://www.googleapis.com/youtube/v3/videos"

# -----------------------------
# Step 2: Data Ingestion
# -----------------------------
def get_trending_videos(region="IN", max_results=20):
    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "regionCode": region,
        "maxResults": max_results,
        "key": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    videos = []
    for item in data.get("items", []):
        videos.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "publish_time": item["snippet"]["publishedAt"],
            "views": item["statistics"].get("viewCount", 0),
            "likes": item["statistics"].get("likeCount", 0),
            "comments": item["statistics"].get("commentCount", 0),
            "category": item["snippet"].get("categoryId")
        })
    return pd.DataFrame(videos)

# -----------------------------
# Step 3: Data Cleaning
# -----------------------------
def clean_data(df):
    if df.empty:
        return df

    df["publish_time"] = pd.to_datetime(df["publish_time"])
    df.drop_duplicates(subset="video_id", inplace=True)
    df.fillna({"likes": 0, "comments": 0}, inplace=True)

    # Convert to int safely
    df["views"] = df["views"].astype(int)
    df["likes"] = df["likes"].astype(int)
    df["comments"] = df["comments"].astype(int)

    return df

# -----------------------------
# Step 4: Save Data
# -----------------------------
def save_to_csv(df, folder="data"):
    os.makedirs(folder, exist_ok=True)
    filename = f"{folder}/trending_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"[INFO] Data saved to {filename}")

def save_to_sqlite(df, db_name="youtube.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql("trending_videos", conn, if_exists="append", index=False)
    conn.close()
    print(f"[INFO] Data saved to {db_name} (table: trending_videos)")

# -----------------------------
# Step 5: Main Pipeline
# -----------------------------
def run_pipeline():
    print("[INFO] Fetching trending videos...")
    df = get_trending_videos(region="IN", max_results=1000)  # India, 50 videos
    print(f"[INFO] Retrieved {len(df)} videos")

    print("[INFO] Cleaning data...")
    df_clean = clean_data(df)

    if df_clean.empty:
        print("[WARN] No data to save.")
        return

    print("[INFO] Saving data to CSV and SQLite...")
    save_to_csv(df_clean)
    save_to_sqlite(df_clean)

    print("[SUCCESS] Pipeline execution completed.")

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    run_pipeline()

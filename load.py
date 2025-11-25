import sys
import os

# Add project root to PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from config import DB_CONFIG



import sys
import os
import psycopg2
import logging
import json
from psycopg2.extras import execute_values

def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

def insert_records(query, records):
    """Reusable insert function with proper transaction handling."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        execute_values(cur, query, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Insert failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def load_raw(df, raw_json):
    logging.info("Loading RAW data into PostgreSQL...")

    # Store raw JSON once, not per row
    raw_json_str = json.dumps(raw_json)

    records = [
        (row["article"], row["rank"], row["views"], row["project"], row["date"], raw_json_str)
        for _, row in df.iterrows()
    ]

    query = """
        INSERT INTO wikiviews_raw(article, rank, views, project, date, raw_json)
        VALUES %s
        ON CONFLICT (article, date) DO NOTHING;
    """

    insert_records(query, records)

def load_cleaned(df_clean):
    logging.info("Loading CLEANED data into PostgreSQL...")

    records = [
        (row['article'], row['views'], row['rank'], row['date'], row['url'], row['category'])
        for _, row in df_clean.iterrows()
    ]

    query = """
        INSERT INTO wikiviews_cleaned(article, views, rank, date, url, category)
        VALUES %s
        ON CONFLICT (article, date) DO NOTHING;
    """

    insert_records(query, records)

def load_top_daily(df_clean):
    logging.info("Loading TOP 20 pages into PostgreSQL...")

    df_top = df_clean.sort_values("rank").head(20)

    records = [
        (row['article'], row['views'], row['rank'], row['date'], row['url'])
        for _, row in df_top.iterrows()
    ]

    query = """
        INSERT INTO top_pages_daily(article, views, rank, date, url)
        VALUES %s
        ON CONFLICT (article, date) DO NOTHING;
    """

    insert_records(query, records)


if __name__ == "__main__":
    import pandas as pd
    import json

    # Load raw JSON and CSV
    raw_df_path = "data/raw/raw_wikiviews.csv"
    clean_df_path = "data/processed/clean_wikiviews.csv"

    if not os.path.exists(raw_df_path) or not os.path.exists(clean_df_path):
        raise FileNotFoundError("Run extract and transform first.")

    df_raw = pd.read_csv(raw_df_path)
    df_clean = pd.read_csv(clean_df_path)

    # Fake raw_json for now
    raw_json = {"items": []}

    load_raw(df_raw, raw_json)
    load_cleaned(df_clean)
    load_top_daily(df_clean)

    print("Load complete, database updated")


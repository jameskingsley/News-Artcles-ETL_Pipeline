import sys
import os

# Add project root to PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from config import DB_CONFIG

import requests
import datetime
import logging
import pandas as pd
from requests.exceptions import RequestException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_wikipedia_pageviews():
    """
    Fetches top English Wikipedia pageviews for yesterday.
    
    Returns:
        df (pd.DataFrame): Cleaned article data
        raw_json (dict): Raw API response
    """

    # Use yesterday's date
    today = datetime.date.today() - datetime.timedelta(days=1)

    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
        f"en.wikipedia/all-access/{today.year}/{today.month:02d}/{today.day:02d}"
    )

    logging.info(f"Fetching data from: {url}")

    # Add User-Agent to avoid 403 Forbidden
    headers = {
        "User-Agent": "Wikiviews ETL Pipeline (bestmankingsley001@gmail.com)"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except RequestException as e:
        logging.error(f"Error fetching Wikipedia data: {e}")
        raise

    raw_json = response.json()

    # Extract articles safely
    try:
        articles = raw_json["items"][0]["articles"]
        project = raw_json["items"][0]["project"]
    except (KeyError, IndexError) as e:
        logging.error(f"Unexpected JSON format: {e}")
        raise

    df = pd.DataFrame(articles)

    # Additional metadata
    df["project"] = project
    df["date"] = today

    logging.info(f"Fetched {len(df)} records.")

    return df, raw_json 

# Main execution block
if __name__ == "__main__":
    df, raw_json = fetch_wikipedia_pageviews()

    os.makedirs("data/raw", exist_ok=True)
    df.to_csv("data/raw/raw_wikiviews.csv", index=False)

    print("Extract complete → data/raw/raw_wikiviews.csv")

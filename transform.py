import sys
import os
import requests
import pandas as pd
import logging
from time import sleep
from requests.adapters import HTTPAdapter, Retry

# Add project root to PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

UNWANTED_PREFIXES = [
    "Special:", "User:", "Template:", "File:", "Wikipedia:", "Category:"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))


def fetch_categories(title: str) -> str:
    """
    Fetch categories for a Wikipedia article. Returns the first meaningful category.
    Falls back to 'General' on failure or if no categories exist.
    """
    if any(title.startswith(prefix) for prefix in UNWANTED_PREFIXES):
        return "General"

    url = f"https://en.wikipedia.org/w/api.php?action=query&prop=categories&titles={title}&format=json&cllimit=max"

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            cats = page.get("categories", [])
            if cats:
                # Return first category without 'Wikipedia:' prefix
                return cats[0]["title"].replace("Category:", "")
        return "General"
    except requests.exceptions.RequestException as e:
        logging.warning(f"Could not fetch categories for '{title}': {e}")
        return "General"


def clean_wikipedia_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting data transformation...")

    df = df.copy()

    # Validate expected columns
    required_cols = {"article", "views", "rank"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Remove system pages
    before_count = len(df)
    df = df[~df["article"].fillna("").str.startswith(tuple(UNWANTED_PREFIXES))]
    after_count = len(df)
    logging.info(f"Removed {before_count - after_count} unwanted system articles.")

    # Generate Wikipedia URLs
    df["url"] = df["article"].apply(lambda x: f"https://en.wikipedia.org/wiki/{x.replace(' ', '_')}")

    # Fetch categories for each article
    categories = []
    for title in df["article"]:
        cat = fetch_categories(title)
        categories.append(cat)
        sleep(0.1)  # slight delay to avoid hitting API too fast
    df["category"] = categories

    logging.info("Transformation complete with real categories.")
    return df


if __name__ == "__main__":
    raw_path = "data/raw/raw_wikiviews.csv"
    if not os.path.exists(raw_path):
        raise FileNotFoundError("Run extract step first: raw_wikiviews.csv not found")

    df_raw = pd.read_csv(raw_path)
    df_clean = clean_wikipedia_data(df_raw)

    os.makedirs("data/processed", exist_ok=True)
    df_clean.to_csv("data/processed/clean_wikiviews.csv", index=False)
    print("Transform complete, data/processed/clean_wikiviews.csv")

import os
import datetime as dt
from typing import List, Optional

import duckdb
import pandas as pd
from pathlib import Path


def parse_date(date_str):
    if not date_str:
        return None
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',  # 2024-02-15 14:30:00
        '%Y-%m-%d',           # 2024-02-15
        '%m/%d/%Y',           # 2/15/2024
        '%m/%d/%y'            # 2/15/24
    ]
    
    for date_format in formats_to_try:
        try:
            return dt.datetime.strptime(date_str, date_format)
        except ValueError:
            continue
    
    return None


def load_stories_from_csv(csv_path: Path):
    """Load stories from a CSV file instead of the API."""
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} stories from {csv_path.resolve()}")
        
        # Convert DataFrame to list of dictionaries (same format as API response)
        stories = df.to_dict('records')
        return stories
    
    except FileNotFoundError:
        print(f"Error: Could not find {csv_path.resolve()}")
        print("Please make sure 'stories.csv' is in the same directory as this script.")
        return []
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return []


def save_stories(stories, db_path="db/data.duckdb"):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Delete existing file if it exists
    if Path(db_path).exists():
        Path(db_path).unlink()

    df = pd.DataFrame(stories)
    conn = duckdb.connect(db_path)

    conn.execute("CREATE TABLE stories AS SELECT * FROM df")
    conn.close()
    print(f"Saved {len(df)} stories into {db_path}")


def load_csv_data(csv_path: Path):
    """Main function to load stories from CSV and save to database."""
    stories = load_stories_from_csv(csv_path)
    if stories:
        save_stories(stories)
    return stories


if __name__ == "__main__":
    # Always resolve relative to the script’s directory
    script_dir = Path(__file__).parent
    csv_file_path = script_dir / "stories.csv"

    stories = load_csv_data(csv_file_path)
    print(f"Processed {len(stories)} stories")

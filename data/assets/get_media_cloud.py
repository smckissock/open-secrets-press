# projects/usaid-media/get_media_cloud.py
import os
import datetime as dt
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
from pathlib import Path
import mediacloud.api

from .db_connection import connect

load_dotenv()


def parse_date(date_str: Optional[str]) -> Optional[dt.datetime]:
    if not date_str:
        return None
    formats_to_try: List[str] = [
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


def get_media_cloud_stories() -> List[Dict[str, Any]]:
    search_api: mediacloud.api.SearchApi = mediacloud.api.SearchApi(os.environ['MEDIACLOUD_API_KEY'])

    collection_ids: List[int] = [
        34412234,   # United States - National
        262985232,  # US College Papers
        262985236,  # US Most Visited New Online (Mar 2025)
        186572435,  # U.S. Top Newspapers 2018
        186572516   # U.S. Top Sources 2018
    ]

    terms: List[str] = ["opensecrets"]
    ored_terms: str = " OR ".join(f'"{term}"' for term in terms)

    start_date: dt.date = dt.date(2025, 9, 26)
    end_date: dt.date = dt.date.today()
    
    print(f"Searching for stories from {start_date} to {end_date}")

    all_stories: List[Dict[str, Any]] = []
    more_stories: bool = True
    pagination_token: Optional[str] = None

    while more_stories:
        page: List[Dict[str, Any]]
        page, pagination_token = search_api.story_list(
            ored_terms,
            start_date,
            end_date,
            collection_ids=collection_ids,
            pagination_token=pagination_token
        )
        all_stories += page
        more_stories = pagination_token is not None

    return all_stories


# Excludes those with duplicate IDs 
def save_stories(stories: List[Dict[str, Any]]) -> None:
    df: pd.DataFrame = pd.DataFrame(stories)
    conn: duckdb.DuckDBPyConnection = connect()

    # Get existing IDs as a set for O(1) lookup
    existing_ids: set = set(
        conn.execute("SELECT id FROM stage_story").fetchdf()['id'].tolist()
    )
    
    # Filter out stories with existing IDs
    df_filtered: pd.DataFrame = df[~df['id'].isin(existing_ids)]
    
    to_insert = len(df_filtered)
    if to_insert > 0:
        conn.register("df_filtered", df_filtered)
        conn.execute("INSERT INTO stage_story SELECT * FROM df_filtered")
        print(f"Saved {to_insert} new stories (skipped {len(df) - to_insert} duplicates)")
    else:
        print(f"No new stories to save ({len(df)} were duplicates)")
        
    conn.close()
    return to_insert


def get_media_cloud() -> List[Dict[str, Any]]:
    stories: List[Dict[str, Any]] = get_media_cloud_stories()
    inserted = save_stories(stories)
    return inserted


if __name__ == "__main__":
    stories: List[Dict[str, Any]] = get_media_cloud()
    print(f"Processed {len(stories)} stories")
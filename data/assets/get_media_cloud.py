# projects/usaid-media/get_media_cloud.py
import os
import datetime as dt
from dotenv import load_dotenv
from typing import Iterable, List, Optional

import duckdb
import pandas as pd
from pathlib import Path
import mediacloud.api

load_dotenv()


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


def get_media_cloud_stories():
    search_api = mediacloud.api.SearchApi(os.environ['MEDIACLOUD_API_KEY'])

    collection_ids = [
        34412234,   # United States - National
        262985232,  # US College Papers
        262985236,  # US Most Visited New Online (Mar 2025)
        186572435,  # U.S. Top Newspapers 2018
        186572516   # U.S. Top Sources 2018
    ]

    terms = ["opensecrets"]
    ored_terms = " OR ".join(f'"{term}"' for term in terms)

    start_date = dt.date(2023, 1, 1)
    end_date =  dt.date.today()
    
    print(f"Searching for stories from {start_date} to {end_date}")

    all_stories = []
    more_stories = True
    pagination_token = None

    while more_stories:
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


def save_stories(stories, db_path="db/data.duckdb"):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(stories)
    conn = duckdb.connect(db_path)

    conn.execute("CREATE TABLE IF NOT EXISTS stories AS SELECT * FROM df LIMIT 0")
    conn.register("df", df)
    conn.execute("INSERT INTO stories SELECT * FROM df")

    conn.close()
    print(f"Saved {len(df)} stories into {db_path}")


def get_media_cloud():
    stories = get_media_cloud_stories()
    save_stories(stories)
    return stories


if __name__ == "__main__":
    stories = get_media_cloud()
    print(f"Processed {len(stories)} stories")
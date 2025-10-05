# data/assets/get_newspaper.py
import time
import os
import random
from datetime import datetime, date
from typing import Any, Dict, Iterable, Tuple, Optional
from dotenv import load_dotenv

import duckdb

from .db_connection import connect


def normalize_timestamp(value) -> Optional[datetime]:
    """Return a Python datetime or None. Empty/invalid values -> None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return None


def get_stories_without_newspaper(conn: duckdb.DuckDBPyConnection) -> Iterable[Tuple[str, str]]:
    sql = """
        SELECT s.id, s.url
        FROM stage_story s
        WHERE s.id NOT IN (SELECT n.media_cloud_id FROM stage_newspaper n)
    """
    return conn.execute(sql).fetchall()


def insert_stage_row(
    conn: duckdb.DuckDBPyConnection,
    media_cloud_id: str,
    article: Dict[str, Any],
    imported_at: datetime,
) -> None:
    sql = """
    INSERT INTO stage_newspaper (
        media_cloud_id,
        import_date,
        title,
        text,
        publish_date,
        authors,
        top_image,
        summary,
        success,
        error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        str(media_cloud_id),
        imported_at,
        (article.get("title") or "").strip(),
        article.get("text") or "",
        normalize_timestamp(article.get("publish_date")),
        article.get("authors") or "",
        article.get("top_image") or "",
        article.get("summary") or "",
        bool(article.get("success")),
        article.get("error"),
    ]
    conn.execute(sql, params)

def process_url(url: str) -> Dict[str, Any]:
    try:
        from newspaper import Article, Config
        import requests.adapters
        import urllib3

        # Disable urllib3 warnings about unverified HTTPS
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        cfg = Config()
        cfg.browser_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"
        cfg.request_timeout = 5
        cfg.number_threads = 1
        cfg.memoize_articles = False
        cfg.fetch_images = False

        art = Article(url, config=cfg)
        try:
            art.download()
            art.parse()

            try:
                art.build()
            except Exception as e:
                print(f"    [warn] Could not build article metadata for {url}: {e}")

            pub = normalize_timestamp(getattr(art, "publish_date", None))
            return {
                "title": getattr(art, "title", "") or "",
                "text": getattr(art, "text", "") or "",
                "publish_date": pub,
                "authors": ", ".join(getattr(art, "authors", []) or []),
                "top_image": getattr(art, "top_image", "") or "",
                "summary": getattr(art, "summary", "") or "",
                "success": True,
                "error": None,
            }
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            return {
                "title": "",
                "text": "",
                "publish_date": None,
                "authors": "",
                "top_image": "",
                "summary": "",
                "success": False,
                "error": error_msg,
            }

    except Exception as e:
        return {
            "title": "",
            "text": "",
            "publish_date": None,
            "authors": "",
            "top_image": "",
            "summary": "",
            "success": False,
            "error": f"ImportError: {type(e).__name__}: {e}",
        }


def get_newspaper():
    conn = connect()
    
    stories = get_stories_without_newspaper(conn)
    total = len(stories)
    print(f"Found {total} stories to without newspaper data.")

    processed_ok = 0
    processed_fail = 0
    now = datetime.now()
    start_time = time.time()

    for idx, (story_id, url) in enumerate(stories, start=1):
        print(f"[{idx}/{total}] Processing ID={story_id} :: {url}")
        
        try:
            article = process_url(url)
            insert_stage_row(conn, story_id, article, now)
            
            if article.get("success"):
                processed_ok += 1
                print(f"  ✓ Success: {(article.get('title') or '')[:50]}...")
            else:
                processed_fail += 1
                print(f"  ✗ Failed: {article.get('error', 'Unknown error')}")
                
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            print(f"  [error] ID={story_id} :: {err}")
            
            fallback = {
                "title": "",
                "text": "",
                "publish_date": None,
                "authors": "",
                "top_image": "",
                "summary": "",
                "success": False,
                "error": err,
            }
            try:
                insert_stage_row(conn, story_id, fallback, now)
            except Exception as e2:
                print(f"  [fatal-insert] ID={story_id} :: {type(e2).__name__}: {e2}")
            processed_fail += 1
        
        time.sleep(2)        
        if idx % 100 == 0:
            # Force write from wal to data.duckdb 
            conn.execute("CHECKPOINT")
            elapsed = time.time() - start_time
            rate = idx / elapsed * 60
            success_rate = processed_ok / idx * 100
            print(f"  === Progress: Inserted:{processed_ok}  {idx}/{total} ({success_rate:.1f}% success, {rate:.1f} items/min) ===")

    elapsed = time.time() - start_time
    print(f"Done in {elapsed/60:.1f} minutes. Success: {processed_ok}, Failed: {processed_fail}")
    conn.close()
    return {
        "stories_found": processed_ok,
        "stories_not_found": processed_fail
    }


if __name__ == "__main__":
    get_newspaper()
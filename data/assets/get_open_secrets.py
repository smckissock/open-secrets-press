#!/usr/bin/env python3
"""
OpenSecrets.org News Scraper - DuckDB Version
Writes article data directly to DuckDB database
"""

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
import hashlib

import duckdb
import pandas as pd
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin

#from db_connection import connect  ## for dagster
from db_connection import connect ## for F5


load_dotenv()

BASE_URL = "https://www.opensecrets.org/news/page/{}"


def parse_index_page(html_content: str, page_num: int) -> List[Dict[str, Any]]:
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'lxml')
    articles = []
    article_links = soup.find_all('a', href=True)
    
    for link in article_links:
        href = link.get('href', '')
        
        is_article = (
            '/news/' in href and 
            '/news/page/' not in href and 
            '/news/category' not in href and
            '/news/issues' not in href and
            '/news/reports/' not in href and
            '/news/partner' not in href and
            '/20' in href
        )
        
        if is_article:
            full_url = urljoin('https://www.opensecrets.org', href)
            title = link.get_text(strip=True)
            
            if title and len(title) > 10 and full_url not in [a['url'] for a in articles]:
                articles.append({
                    'url': full_url,
                    'title': title,
                    'found_on_page': page_num
                })
    
    return articles


def scrape_article_details(page, url: str) -> Optional[Dict[str, Any]]:
    try:
        response = page.goto(url, wait_until='networkidle', timeout=30000)
        
        if not response or response.status != 200:
            return None
        
        page.wait_for_timeout(1000)
        html = page.content()
        soup = BeautifulSoup(html, 'lxml')
        
        details = {
            'publish_date': None,
            'author': None,
            'photo_url': None
        }
        
        date_elem = soup.find('time')
        if date_elem:
            details['publish_date'] = date_elem.get('datetime') or date_elem.get_text(strip=True)
        else:
            meta_date = soup.find('meta', {'property': 'article:published_time'})
            if not meta_date:
                meta_date = soup.find('meta', {'name': 'publish-date'})
            if meta_date:
                details['publish_date'] = meta_date.get('content')
        
        meta_author = soup.find('meta', {'name': 'author'})
        if meta_author:
            details['author'] = meta_author.get('content')
        else:
            author_elem = soup.find(class_=['author', 'byline', 'author-name'])
            if author_elem:
                details['author'] = author_elem.get_text(strip=True)
            else:
                author_link = soup.find('a', {'rel': 'author'})
                if author_link:
                    details['author'] = author_link.get_text(strip=True)
        
        meta_img = soup.find('meta', {'property': 'og:image'})
        if meta_img:
            details['photo_url'] = meta_img.get('content')
        else:
            img = soup.find('img', class_=['featured-image', 'article-image', 'wp-post-image'])
            if img:
                details['photo_url'] = img.get('src')
        
        return details
        
    except Exception as e:
        print(f"  Error scraping article details: {e}")
        return None


def normalize_timestamp(value) -> Optional[datetime]:
    """Convert various date formats to datetime"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    
    if isinstance(value, str):
        formats_to_try = [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
        ]
        for fmt in formats_to_try:
            try:
                return datetime.strptime(value.split('+')[0], fmt.replace('%z', ''))
            except ValueError:
                continue    
    return None


def get_existing_urls(conn: duckdb.DuckDBPyConnection) -> set:    
    try:
        result = conn.execute("SELECT url FROM stage_story").fetchall()
        return {row[0] for row in result}
    except:
        return set()


# Insert into both stage_story and stage_newpaper
def insert_article(
    conn: duckdb.DuckDBPyConnection,
    article: Dict[str, Any],
    imported_at: datetime
) -> None:
    
    publish_date = datetime.strptime(article['publish_date'], '%Y-%m-%d').date()
    url = article.get('url').rstrip('/')
    url_hash = hashlib.sha256(url.encode()).hexdigest()  
    sql = """
    INSERT INTO stage_story (
        id,
        indexed_date,
        language,
        media_name,
        media_url,
        publish_date,
        title,
        url
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        url_hash,
        normalize_timestamp(article.get('publish_date')),
        'en',
        'OpenSecrets',
        'opensecrets.org',
        publish_date,
        article.get('title') or '',
        article.get('url')
    ]
    conn.execute(sql, params)

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
        url_hash,
        imported_at,  # Use imported_at parameter, not publish_date
        article.get('title') or '',
        '',  # text (empty)
        publish_date,
        article.get('author') or '',
        article.get('photo_url') or '',
        '',     # summary
        True,   # success
        None    # error
    ]
    conn.execute(sql, params)


def scrape_opensecrets_articles(max_articles: int = 1) -> int:
    conn = connect()
    
    print(f"\nOpenSecrets.org News Scraper - Scraping {max_articles} article(s)\n")
    
    existing_urls = get_existing_urls(conn)
    print(f"Found {len(existing_urls)} existing articles in database")
    
    all_articles = []
    imported_at = datetime.now()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
        )
        
        page = context.new_page()        
        try:
            page.goto('https://www.opensecrets.org/', wait_until='networkidle', timeout=30000)
            time.sleep(1)
        except Exception as e:
            print(f"Warning: Could not initialize session: {e}")
        
        for page_num in range(1, 241):  #241
            if len(all_articles) >= max_articles:
                break
                
            url = BASE_URL.format(page_num)
            print(f"Page {page_num}: {url}")            
            try:
                response = page.goto(url, wait_until='networkidle', timeout=20000)
                
                if response and response.status == 200:
                    page.wait_for_timeout(2000)
                    html = page.content()
                    articles = parse_index_page(html, page_num)
                    
                    print(f"  Found {len(articles)} articles") 
                    for article in articles:
                        if len(all_articles) >= max_articles:
                            break

                        if article['url'] in existing_urls:
                            print(f"    Skipping (exists): {article['title'][:50]}...")
                            continue
                        
                        print(f"    Scraping: {article['title'][:50]}...")                        
                        try:
                            details = scrape_article_details(page, article['url'])
                            if details:
                                article.update(details)
                                insert_article(conn, article, imported_at)
                                existing_urls.add(article['url'])  # Track this URL
                                all_articles.append(article)
                                print(f"      Saved to database")
                                time.sleep(1)
                        except Exception as e:
                            print(f"      Error saving: {e}")
                            existing_urls.add(article['url'])
                else:
                    print(f"  Failed (Status: {response.status if response else 'No response'})")
                    
            except Exception as e:
                print(f"  Error: {e}")
            
            if len(all_articles) < max_articles:
                time.sleep(.5)
        
        browser.close()    
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"Articles saved: {len(all_articles)}")
    print(f"{'='*50}\n")
    
    return len(all_articles)


def get_opensecrets() -> int:
    """Main entry point matching the pattern of get_media_cloud and get_newspaper"""
    return scrape_opensecrets_articles(max_articles=100000)


if __name__ == "__main__":
    count = get_opensecrets()
    print(f"Processed {count} article(s)")
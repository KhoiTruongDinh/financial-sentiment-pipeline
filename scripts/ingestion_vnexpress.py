import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import requests
from bs4 import BeautifulSoup
from mongo_utils import get_raw_collection, get_cutoff_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Fetching macro events: World News, Business, and Breaking News
VNEXPRESS_RSS_FEEDS = [
    "https://vnexpress.net/rss/tin-moi-nhat.rss",
    "https://vnexpress.net/rss/the-gioi.rss",
    "https://vnexpress.net/rss/kinh-doanh.rss"
]
DOMAIN = "vnexpress.net"

def parse_rss_date(date_str: str) -> datetime:
    """Parses standard RSS pubDate (RFC 822) into naive UTC datetime."""
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as e:
        logging.warning(f"Failed to parse RSS date '{date_str}': {str(e)}")
        return datetime.utcnow()

def clean_html_from_description(raw_desc: str) -> str:
    """Removes HTML tags (like <img>) from RSS CDATA descriptions."""
    if not raw_desc:
        return ""
    return BeautifulSoup(raw_desc, "html.parser").text.strip()

def scrape_full_content(article_url: str, headers: dict) -> str:
    """
    Visits the actual article URL and extracts the full paragraph content.
    Returns the concatenated text of the article body.
    """
    try:
        res = requests.get(article_url, headers=headers, timeout=10)
        if res.status_code != 200:
            logging.warning(f"Failed to fetch content for {article_url}. Status: {res.status_code}")
            return ""
            
        soup = BeautifulSoup(res.content, "html.parser")
        
        # VnExpress mostly uses <p class="Normal"> for article body paragraphs
        paragraphs = soup.find_all("p", class_="Normal")
        
        # Extract text and filter out empty paragraphs
        content = "\n".join([p.text.strip() for p in paragraphs if p.text.strip()])
        return content
        
    except Exception as e:
        logging.error(f"Error extracting full content from {article_url}: {str(e)}")
        return ""

def scrape_vnexpress_rss_incremental():
    """Scrapes VnExpress RSS feeds incrementally with Batch DB Lookup and Deep HTML Scraping."""
    collection = get_raw_collection("raw_financial_news")
    cutoff_date = get_cutoff_date(collection, DOMAIN)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    total_inserted = 0

    for feed_url in VNEXPRESS_RSS_FEEDS:
        logging.info(f"Processing RSS feed: {feed_url}")
        try:
            response = requests.get(feed_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logging.error(f"Failed to fetch {feed_url}. Status: {response.status_code}")
                continue

            # Parse XML feed
            soup = BeautifulSoup(response.content, "xml")
            items = soup.find_all("item")
            
            # 1. BATCH PRE-FETCH: Extract all URLs from the current RSS feed
            rss_urls = [item.link.text for item in items if item.link]

            # 2. BULK QUERY: Hit the DB only ONCE to find which of these URLs already exist
            existing_records = collection.find(
                {"article_url": {"$in": rss_urls}}, 
                {"article_url": 1} # Only retrieve the URL field to save RAM
            )
            
            # 3. O(1) LOOKUP CACHE: Convert results to a Python Set
            existing_urls_set = {doc["article_url"] for doc in existing_records}
            
            feed_inserted = 0
            for item in items:
                title = item.title.text if item.title else ""
                article_url = item.link.text if item.link else ""
                pub_date_str = item.pubDate.text if item.pubDate else ""
                raw_desc = item.description.text if item.description else ""

                if not article_url:
                    continue

                published_at = parse_rss_date(pub_date_str)

                # INCREMENTAL STOP CHECK (Date Threshold)
                if published_at <= cutoff_date:
                    logging.info(f"Reached older articles (<= {cutoff_date}) in this feed. Moving to next feed.")
                    break

                # O(1) IN-MEMORY DEDUPLICATION: Check if URL was already processed
                if article_url in existing_urls_set:
                    logging.info(f"URL exists in memory cache. Skipping Deep Scrape: {title[:40]}...")
                    continue

                # DEEP SCRAPE: Fetch full HTML content ONLY for genuinely NEW articles
                clean_description = clean_html_from_description(raw_desc)
                full_content = scrape_full_content(article_url, headers)
                
                # Respectful crawling: wait 1 second between HTML requests
                time.sleep(1)

                document = {
                    "source_domain": DOMAIN,
                    "language": "vi",
                    "article_url": article_url,
                    "published_at": published_at,
                    "scraped_at": datetime.utcnow(),
                    "raw_payload": {
                        "title": title,
                        "description": clean_description,
                        "full_content": full_content,
                        "author": "VnExpress RSS",
                    },
                    "processed_status": "PENDING"
                }
                
                try:
                    collection.insert_one(document)
                    feed_inserted += 1
                    logging.info(f"Ingested NEW article with full content: {title[:40]}...")
                except Exception as e:
                    logging.warning(f"Failed to insert article {article_url}: {str(e)}")

            total_inserted += feed_inserted
            logging.info(f"Inserted {feed_inserted} new items from {feed_url}")

        except Exception as e:
            logging.error(f"Error processing feed {feed_url}: {str(e)}")

    logging.info(f"VnExpress RSS & Content ingestion complete. Total new items across all feeds: {total_inserted}")

if __name__ == "__main__":
    scrape_vnexpress_rss_incremental()


# # docker exec -it airflow_webserver python /opt/airflow/scripts/ingestion_vnexpress.py
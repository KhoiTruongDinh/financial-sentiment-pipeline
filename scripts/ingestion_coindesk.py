import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import requests
from bs4 import BeautifulSoup
from mongo_utils import get_raw_collection, get_cutoff_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Fetching macro events from CoinDesk RSS Feed
COINDESK_RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/"
]
DOMAIN = "coindesk.com"

def parse_rss_date(date_str: str) -> datetime:
    """Parses standard RSS pubDate (RFC 822) into naive UTC datetime."""
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as e:
        logging.warning(f"Failed to parse RSS date '{date_str}': {str(e)}")
        return datetime.utcnow()

def clean_html_from_description(raw_desc: str) -> str:
    """Removes HTML tags from RSS CDATA descriptions."""
    if not raw_desc:
        return ""
    return BeautifulSoup(raw_desc, "html.parser").text.strip()

def scrape_full_content(article_url: str, headers: dict) -> str:
    """
    Visits the actual CoinDesk article URL and extracts the full paragraph content.
    Handles multiple 'document-body' blocks separated by inline ads/widgets.
    """
    try:
        res = requests.get(article_url, headers=headers, timeout=10)
        if res.status_code != 200:
            logging.warning(f"Failed to fetch content for {article_url}. Status: {res.status_code}")
            return ""
            
        soup = BeautifulSoup(res.content, "html.parser")

        # 1. Primary extraction: Look for the main article content wrapper
        article_bodies = soup.select("div.article-content-wrapper div.document-body")
        
        # 2. Fallback extraction: If no 'document-body' found, look for alternative wrappers
        if not article_bodies:
            article_bodies = soup.find_all("div", class_="article-content-wrapper") or soup.find_all("div", class_="at-content-wrapper")
            
        # 3. If still no content found, return the entire soup as a last resort
        if not article_bodies:
            article_bodies = [soup]
        
        valid_paragraphs = []
        stop_parsing = False 

        for body in article_bodies:
            if stop_parsing:
                break
                
            elements = body.find_all(["p", "li", "h2", "h3", "h4"])
            
            for el in elements:
                text = el.text.strip()
                lower_text = text.lower()
                
                if not text:
                    continue
                if el.name == "p" and len(text) < 30:
                    continue
                    
                # EARLY STOP: If we encounter a paragraph that indicates the end of the article (like disclaimers, ads, or unrelated content), we stop parsing further.
                if text.startswith("Disclosure & Polices") or "an award-winning media outlet" in lower_text:
                    stop_parsing = True
                    break
                    
                # FILTER OUT: Skip paragraphs that are clearly not part of the main content (like ads, sponsored content, or newsletter prompts).
                if "privacy policy" in lower_text and "terms of use" in lower_text:
                    continue
                if "sponsored" in lower_text or "advertisement" in lower_text:
                    continue
                if "sign up for" in lower_text and "newsletter" in lower_text:
                    continue

                # Format list items
                if el.name == "li":
                    text = f"• {text}"
                
                valid_paragraphs.append(text)
        
        return "\n".join(valid_paragraphs)
        
    except Exception as e:
        logging.error(f"Error extracting full content from {article_url}: {str(e)}")
        return ""

def scrape_coindesk_rss_incremental():
    """Scrapes CoinDesk RSS feeds incrementally with Batch DB Lookup and Deep HTML Scraping."""
    collection = get_raw_collection("raw_financial_news")
    cutoff_date = get_cutoff_date(collection, DOMAIN)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    total_inserted = 0

    for feed_url in COINDESK_RSS_FEEDS:
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

            # 2. BULK QUERY: Hit the DB only ONCE to find which URLs already exist
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
                    "language": "en",
                    "article_url": article_url,
                    "published_at": published_at,
                    "scraped_at": datetime.utcnow(),
                    "raw_payload": {
                        "title": title,
                        "description": clean_description,
                        "full_content": full_content,
                        "author": "CoinDesk RSS",
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

    logging.info(f"CoinDesk RSS & Content ingestion complete. Total new items across all feeds: {total_inserted}")

if __name__ == "__main__":
    scrape_coindesk_rss_incremental()

# # # docker exec -it airflow_webserver python /opt/airflow/scripts/ingestion_coindesk.py
import logging
from datetime import datetime
import requests
from mongo_utils import get_raw_collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
TARGET_CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "binancecoin"]

def fetch_crypto_prices():
    """Fetches real-time crypto prices from CoinGecko REST API and stores raw data in MongoDB."""
    params = {
        "vs_currency": "usd",
        "ids": ",".join(TARGET_CRYPTO_IDS),
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h"
    }
    
    headers = {"Accept": "application/json"}

    logging.info("Fetching live crypto price metrics from CoinGecko API...")
    try:
        response = requests.get(COINGECKO_API_URL, params=params, headers=headers, timeout=10)
        
        if response.status_code == 429:
            logging.warning("CoinGecko Free API Rate limit reached! Skipping current iteration.")
            return
            
        if response.status_code != 200:
            logging.error(f"API request failed with HTTP Status: {response.status_code}")
            return

        data = response.json()
        collection = get_raw_collection("raw_market_prices")
        fetched_at = datetime.utcnow()
        inserted_records = []

        for item in data:
            record = {
                "symbol": item.get("symbol", "").upper(),
                "asset_name": item.get("name"),
                "coingecko_id": item.get("id"),
                "fetched_at": fetched_at,
                "raw_payload": {
                    "price_usd": item.get("current_price"),
                    "market_cap_usd": item.get("market_cap"),
                    "volume_24h_usd": item.get("total_volume"),
                    "high_24h_usd": item.get("high_24h"),
                    "low_24h_usd": item.get("low_24h"),
                    "price_change_percentage_24h": item.get("price_change_percentage_24h"),
                    "price_change_percentage_1h": item.get("price_change_percentage_1h_in_currency")
                }
            }
            inserted_records.append(record)

        if inserted_records:
            collection.insert_many(inserted_records)
            logging.info(f"Successfully ingested {len(inserted_records)} crypto price records into MongoDB.")

    except Exception as e:
        logging.error(f"Error occurred while fetching crypto prices: {str(e)}")

if __name__ == "__main__":
    fetch_crypto_prices()



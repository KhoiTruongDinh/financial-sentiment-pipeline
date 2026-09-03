import os
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_mongo_client():
    """Establishes and returns a connection to MongoDB using environment variables."""
    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", 27017))
    username = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongo")
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongo_password")
    
    mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
    return MongoClient(mongo_uri)

def get_raw_collection(collection_name: str):
    """Retrieves a specific MongoDB collection and ensures essential indices exist."""
    client = get_mongo_client()
    db = client["raw_financial_db"]
    collection = db[collection_name]
    
    if collection_name == "raw_financial_news":
        collection.create_index([("article_url", ASCENDING)], unique=True)
        collection.create_index([("source_domain", ASCENDING), ("published_at", DESCENDING)])
        
    return collection

def get_cutoff_date(collection, source_domain: str, fallback_days: int = 2) -> datetime:
    """
    Shared function: Retrieves the maximum published_at timestamp for a specific domain.
    Returns a fallback threshold if no existing records are found (Cold Start).
    """
    latest_record = collection.find_one(
        {"source_domain": source_domain, "published_at": {"$ne": None}},
        sort=[("published_at", -1)]
    )
    
    if latest_record and "published_at" in latest_record:
        max_date = latest_record["published_at"]
        logging.info(f"[{source_domain}] Incremental threshold found in DB: {max_date} UTC")
        return max_date
        
    fallback_date = datetime.utcnow() - timedelta(days=fallback_days)
    logging.info(f"[{source_domain}] Cold start. Threshold set to: {fallback_date} UTC")
    return fallback_date

import os
import json
import logging
import time
from typing import List
from pydantic import BaseModel, Field
from openai import OpenAI
from mongo_utils import get_raw_collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==========================================
# 1. PYDANTIC SCHEMA DEFINITION
# ==========================================
class SentimentAnalysisOutput(BaseModel):
    sentiment_score: float = Field(..., description="Score from -1.0 (extremely bearish) to 1.0 (extremely bullish).")
    impact_label: str = Field(..., description="Must be one of: 'BULLISH', 'BEARISH', or 'NEUTRAL'.")
    summary: str = Field(..., description="A concise 2-sentence summary of the financial impact in English.")
    mentions: List[str] = Field(..., description="List of financial assets or entities mentioned (e.g., ['BTC', 'ETH', 'FED', 'SEC']).")

# ==========================================
# 2. LLM CLIENT INITIALIZATION
# ==========================================
API_KEY = os.getenv("GROQ_API_KEY") # Or OPENAI_API_KEY
BASE_URL = os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1") 
MODEL_NAME = os.getenv("LLM_MODEL_NAME", "openai/gpt-oss-120b")

client = OpenAI(
    api_key=API_KEY,
    base_url=BASE_URL,
)

# ==========================================
# 3. AI PROCESSING LOGIC
# ==========================================
def analyze_article_sentiment(title: str, content: str) -> dict:
    """
    Sends the article content to the LLM and forces a structured JSON response.
    Input can be English or Vietnamese. Output is strictly English.
    """
    system_prompt = (
        "You are an expert financial Data Analyst AI. Your task is to analyze financial news articles.\n"
        "The input article may be in English or Vietnamese.\n"
        "Your analysis MUST be 100% in English.\n"
        "You must respond ONLY with a valid JSON object matching this schema:\n"
        "{\n"
        "  'sentiment_score': float (-1.0 to 1.0),\n"
        "  'impact_label': string ('BULLISH', 'BEARISH', 'NEUTRAL'),\n"
        "  'summary': string (2 sentences),\n"
        "  'mentions': array of strings (e.g. ['BTC', 'FED'])\n"
        "}"
    )

    user_prompt = f"Title: {title}\n\nContent:\n{content}"

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"}, # Forces JSON output
            temperature=0.1, # Low temperature for analytical consistency
            max_tokens=1500
        )
        
        # Parse the JSON string returned by the LLM
        raw_json = response.choices[0].message.content
        parsed_data = json.loads(raw_json)
        
        # Validate using Pydantic (Throws error if LLM hallucinates schema)
        validated_data = SentimentAnalysisOutput(**parsed_data)
        
        return validated_data.model_dump()

    except Exception as e:
        logging.error(f"LLM API or Parsing Error: {str(e)}")
        return None

# ==========================================
# 4. MAIN PIPELINE (INCREMENTAL BATCHING)
# ==========================================
def process_pending_articles(batch_size: int = 20):
    """
    Fetches 'PENDING' articles from MongoDB, runs AI sentiment analysis, 
    and updates the database with the enriched data.
    """
    collection = get_raw_collection("raw_financial_news")
    
    # Query for articles that haven't been processed yet
    pending_cursor = collection.find({"processed_status": "PENDING"}).limit(batch_size)
    pending_articles = list(pending_cursor)
    
    if not pending_articles:
        logging.info("No PENDING articles found. Pipeline is up to date.")
        return

    logging.info(f"Found {len(pending_articles)} PENDING articles. Starting AI Enrichment...")
    
    success_count = 0
    
    for article in pending_articles:
        doc_id = article["_id"]
        source_domain = article.get("source_domain", "unknown")
        title = article["raw_payload"].get("title", "")
        
        # Prefer full_content, fallback to description if content scrape failed
        content = article["raw_payload"].get("full_content") or article["raw_payload"].get("description", "")
        
        if not content:
            logging.warning(f"Skipping empty article: {doc_id}")
            collection.update_one({"_id": doc_id}, {"$set": {"processed_status": "FAILED_EMPTY_CONTENT"}})
            continue

        logging.info(f"Analyzing [{source_domain}]: {title[:50]}...")
        
        # Call LLM
        ai_result = analyze_article_sentiment(title, content)

        # Respectful rate limiting: wait 3 seconds between LLM calls
        # This is crucial to avoid hitting API rate limits or incurring excessive costs.
        # Can be adjusted based on the LLM provider's guidelines.
        time.sleep(3)
        
        if ai_result:
            # Update MongoDB document with AI insights and mark as COMPLETED
            update_payload = {
                "$set": {
                    "ai_enrichment": ai_result,
                    "processed_status": "COMPLETED"
                }
            }
            collection.update_one({"_id": doc_id}, update_payload)
            success_count += 1
            logging.info(f"Success! Sentiment: {ai_result['impact_label']} ({ai_result['sentiment_score']})")
        else:
            # Mark as FAILED to prevent infinite retry loops on bad data
            collection.update_one({"_id": doc_id}, {"$set": {"processed_status": "FAILED_AI_ERROR"}})
            logging.error(f"Failed to process article: {doc_id}")

    logging.info(f"AI Enrichment complete. Successfully processed {success_count}/{len(pending_articles)} articles.")

if __name__ == "__main__":
    process_pending_articles(batch_size=10)

# docker exec -it airflow_webserver python /opt/airflow/scripts/ai_sentiment_engine.py
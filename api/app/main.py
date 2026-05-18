import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
from typing import Dict, Optional

app = FastAPI(title="Real-time Analytics Serving REST API", version="1.0.0")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ServingAPI")

try:
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
except Exception as e:
    logger.error(f"Failed to create Redis connection: {e}")

class SessionResponse(BaseModel):
    session_id: str
    user_id: str
    event_count: int
    is_purchased: bool
    last_active: int

@app.get("/api/sessions/{session_id}", response_model=SessionResponse)
def get_session(session_id: str):
    try:
        data = redis_client.hgetall(f"session:{session_id}")
        if not data:
            raise HTTPException(status_code=404, detail="Active Session not found or timed out")
        
        return SessionResponse(
            session_id=session_id,
            user_id=data.get("user_id", "unknown"),
            event_count=int(data.get("event_count", 0)),
            is_purchased=data.get("is_purchased", "False").lower() == "true",
            last_active=int(data.get("last_activity_timestamp", 0))
        )
    except Exception as e:
        logger.error(f"Error querying Redis: {e}")
        raise HTTPException(status_code=500, detail="Data layer read error")

@app.get("/api/products/trending")
def get_trending_products():
    # In full system, fan-in reads from Cassandra agg_by_product_1m.
    # Mocking for local health-check or fast response.
    return {
        "timestamp": int(time.time() * 1000),
        "trending": [
            {"product_id": "prod_phone12", "score": 98.4, "category": "electronics"},
            {"product_id": "prod_laptop_pro", "score": 85.1, "category": "electronics"},
            {"product_id": "prod_tshirt_black", "score": 72.3, "category": "apparel"}
        ]
    }

@app.get("/health")
def health():
    try:
        redis_ok = redis_client.ping()
    except Exception:
        redis_ok = False
    return {"status": "healthy", "redis_connected": redis_ok}

import time

# API endpoint metrics

# Session get endpoint

# Multi get options

# CORS setup

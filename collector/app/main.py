import uuid
import time
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Dict, Optional
from .kafka_prod import ClickstreamProducer

app = FastAPI(title="Edge Clickstream Collector API", version="1.0.0")
producer = ClickstreamProducer()

class EventPayload(BaseModel):
    user_id: str = Field(..., example="usr_98231")
    session_id: str = Field(..., example="sess_1234567")
    event_type: str = Field(..., example="page_view")
    client_timestamp: int = Field(..., example=1700000000000)
    ip_address: str = Field(..., example="192.168.1.1")
    user_agent: str = Field(..., example="Mozilla/5.0 ...")
    page_url: Optional[str] = None
    payload: Dict[str, str] = Field(default_factory=dict)

@app.post("/collect", status_code=status.HTTP_202_ACCEPTED)
async def collect_event(event: EventPayload):
    # Server-side decorations
    event_id = str(uuid.uuid4())
    server_ts = int(time.time() * 1000)
    
    kafka_msg = {
        "event_id": event_id,
        "user_id": event.user_id,
        "session_id": event.session_id,
        "event_type": event.event_type,
        "client_timestamp": event.client_timestamp,
        "server_timestamp": server_ts,
        "ip_address": event.ip_address,
        "user_agent": event.user_agent,
        "page_url": event.page_url,
        "payload": event.payload
    }
    
    success = producer.send("clickstream.raw", event.user_id, kafka_msg)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish clickstream event to broker"
        )
    
    return {"status": "accepted", "event_id": event_id, "server_timestamp": server_ts}

@app.get("/health")
def health_check():
    return {"status": "healthy", "kafka_connected": producer.is_ready()}

# Event Collector endpoints final configurations

# Metrics endpoints configured

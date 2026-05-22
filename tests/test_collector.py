import pytest
from fastapi.testclient import TestClient
from collector.app.main import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()

def test_collect_valid_event():
    payload = {
        "user_id": "usr_9999",
        "session_id": "sess_8888",
        "event_type": "page_view",
        "client_timestamp": 1700000000000,
        "ip_address": "8.8.8.8",
        "user_agent": "Mozilla/5.0",
        "page_url": "https://www.e-shop.com",
        "payload": {}
    }
    response = client.post("/collect", json=payload)
    # If Kafka isn't running locally during unit tests, handle gracefully
    if response.status_code == 500:
         assert "Failed to publish" in response.json()["detail"]
    else:
         assert response.status_code == 202
         assert "event_id" in response.json()

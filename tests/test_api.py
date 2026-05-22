import pytest
from fastapi.testclient import TestClient
from api.app.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()

def test_trending():
    response = client.get("/api/products/trending")
    assert response.status_code == 200
    assert "trending" in response.json()

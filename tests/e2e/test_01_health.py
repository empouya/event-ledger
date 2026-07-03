# tests/e2e/test_01_health.py -- Area: Health endpoint (section 1)
import requests


def test_health(base_url):
    health = requests.get(f"{base_url}/health", timeout=30).json()

    assert health["status"] == "healthy"
    assert health["checks"]["dynamodb"] == "ok"

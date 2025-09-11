# dfs_check.py
import os, json
from dotenv import load_dotenv
import httpx

load_dotenv()
BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com").rstrip("/")
LOGIN = os.environ["DATAFORSEO_LOGIN"]
PASS = os.environ["DATAFORSEO_PASSWORD"]

payload = [{
    "target": "example.com",
    "limit": 1,
    "order_by": ["first_seen,desc"]
}]

url = f"{BASE}/v3/backlinks/backlinks/live"

with httpx.Client(timeout=30) as c:
    r = c.post(
        url,
        auth=(LOGIN, PASS),
        json=payload,
        headers={"Accept": "application/json"}
    )
    print("STATUS:", r.status_code)
    print("BODY:", r.text[:800])
    r.raise_for_status()
    data = r.json()
    print("OK -> tasks len:", len(data.get("tasks", [])))

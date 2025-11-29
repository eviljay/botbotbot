import base64
import json
import asyncio
from httpx import AsyncClient, HTTPStatusError

# =======================================================
# ВСТАВ API LOGIN / PASSWORD:
# https://app.dataforseo.com/api/access
# =======================================================

API_LOGIN = "info@seoswiss.online"
API_PASSWORD = "d752edcc5e5dbd73"

BASE_URL = "https://api.dataforseo.com"


async def debug_keywords_for_keywords(keyword: str, location_code: int, language_code: str):
    auth_bytes = f"{API_LOGIN}:{API_PASSWORD}".encode("utf-8")

    headers = {
        "Authorization": "Basic " + base64.b64encode(auth_bytes).decode("utf-8"),
        "Content-Type": "application/json",
    }

    payload = [{
        "keywords": [keyword],
        "location_code": location_code,
        "language_code": language_code,
        "sort_by": "relevance"
    }]

    url = f"{BASE_URL}/v3/keywords_data/google_ads/keywords_for_keywords/live"

    async with AsyncClient(timeout=30) as client:
        try:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
        except HTTPStatusError as e:
            print("❌ HTTP Error:")
            print(f"Status: {e.response.status_code}")
            print("Body:", e.response.text)
            return

        data = response.json()

        # RAW JSON
        print("\n================ RAW DATAFORSEO RESPONSE ================\n")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("\n==========================================================\n")

        # Parsed
        if "result" in data:
            items = data["result"]
        else:
            items = []

        print("============== PARSED KEYWORDS ==================\n")

        if not items:
            print("⚠️ No items found.")
            return

        for i, item in enumerate(items, start=1):
            print(f"{i}. {item.get('keyword')}")
            print(f"   Search volume: {item.get('search_volume')}")
            print(f"   Competition: {item.get('competition')} ({item.get('competition_index')})")
            print(f"   CPC: {item.get('cpc')}")
            print("")

        print("==========================================================\n")


# ====================== MAIN ======================

if __name__ == "__main__":
    print("Запит до DataForSEO...\n")

    asyncio.run(
        debug_keywords_for_keywords(
            keyword="casino online",
            location_code=2036,
            language_code="en"
        )
    )

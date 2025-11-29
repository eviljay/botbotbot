import base64
import json
import asyncio
from typing import Any, Dict, List

from httpx import AsyncClient, HTTPStatusError

# =======================================================
# ВСТАВ API LOGIN / PASSWORD:
# https://app.dataforseo.com/api/access
# =======================================================

API_LOGIN = "info@seoswiss.online"
API_PASSWORD = "d752edcc5e5dbd73"

BASE_URL = "https://api.dataforseo.com"


def extract_kfk_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Дістає список keyword-ів з відповіді keywords_for_keywords.

    Підтримує:
    1) Формат без tasks:
       {
         "status_code": 20000,
         "result_count": 2,
         "result": [ {...}, {...} ]
       }

    2) Формат із tasks/items:
       {
         "tasks": [
           {
             "result": [
               {
                 "items": [ {...}, {...} ]
               }
             ]
           }
         ]
       }
    """
    if not isinstance(data, dict):
        return []

    # Випадок 1: top-level result = список keyword-ів
    if isinstance(data.get("result"), list):
        res_list = data["result"]
        if res_list and isinstance(res_list[0], dict) and "keyword" in res_list[0]:
            return res_list

        # інколи буває result[0].items
        first = res_list[0] if res_list else {}
        if isinstance(first, dict) and isinstance(first.get("items"), list):
            return first["items"]

    # Випадок 2: tasks[0].result[0].items
    tasks = data.get("tasks") or []
    if tasks:
        t0 = tasks[0] or {}
        result_list = t0.get("result") or []
        if result_list:
            r0 = result_list[0] or {}
            items = r0.get("items") or []
            if isinstance(items, list):
                return items

    return []


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

    async with AsyncClient(timeout=60) as client:
        try:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
        except HTTPStatusError as e:
            print("❌ HTTP Error:")
            print(f"Status: {e.response.status_code}")
            print("Body:", e.response.text)
            return

        data = response.json()

        # 1) RAW JSON
        print("\n================ RAW DATAFORSEO RESPONSE ================\n")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("\n==========================================================\n")

        # 2) PARSED KEYWORDS
        items = extract_kfk_items(data)

        print("============== PARSED KEYWORDS ==================\n")

        if not items:
            print("⚠️ No items found by extract_kfk_items().")
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

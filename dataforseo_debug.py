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


def find_keyword_items(node: Any) -> List[Dict[str, Any]]:
    """
    Рекурсивно шукає перший список dict'ів, у яких є ключ 'keyword'.
    Працює з будь-якою вкладеністю.
    """
    # Якщо це список
    if isinstance(node, list):
        # Якщо це список об’єктів з 'keyword' – це те, що нам треба
        if node and all(isinstance(x, dict) and "keyword" in x for x in node):
            return node

        # Інакше обходимо всіх дітей
        for x in node:
            found = find_keyword_items(x)
            if found:
                return found

    # Якщо це dict – обходимо значення
    elif isinstance(node, dict):
        for v in node.values():
            found = find_keyword_items(v)
            if found:
                return found

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

        # 2) Шукаємо keywords будь-де
        items = find_keyword_items(data)

        print("============== PARSED KEYWORDS (found by search) ==================\n")

        if not items:
            print("⚠️ No items with 'keyword' found in JSON.")
            return

        print(f"Знайшов {len(items)} keyword-ів:\n")

        for i, item in enumerate(items, start=1):
            print(f"{i}. {item.get('keyword')}")
            print(f"   Search volume: {item.get('search_volume')}")
            print(f"   Competition: {item.get('competition')} ({item.get('competition_index')})")
            print(f"   CPC: {item.get('cpc')}")
            print("")

        print("===================================================================\n")


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

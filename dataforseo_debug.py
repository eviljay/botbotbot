import base64
import json
from httpx import AsyncClient


LOGIN = "info@seoswiss.online"
PASSWORD = "bidcow-myjzUf-cutxy8"


async def debug_keywords_for_keywords(keyword: str, location: int, language: str):
    auth = f"{LOGIN}:{PASSWORD}".encode("utf-8")
    headers = {
        "Authorization": "Basic " + base64.b64encode(auth).decode(),
        "Content-Type": "application/json"
    }

    payload = [{
        "keywords": [keyword],
        "location_code": location,
        "language_code": language,
        "sort_by": "relevance",
    }]

    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/keywords_for_keywords/live"

    async with AsyncClient(timeout=30) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()

        # Виводимо ВСЕ у консоль красиво
        print("\n================ RAW DATAFORSEO RESPONSE ================\n")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("\n==========================================================\n")

        return data


# ---------------- MAIN ----------------

if __name__ == "__main__":
    import asyncio
    print("Запит до DataForSEO...\n")

    asyncio.run(
        debug_keywords_for_keywords(
            keyword="casino online",
            location=2036,
            language="en"
        )
    )

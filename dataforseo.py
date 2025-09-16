# /root/mybot/dataforseo.py
import base64
from typing import List, Dict, Any
import httpx

def _cc_to_country_name(cc: str) -> str:
    m = {
        "us": "United States",
        "ua": "Ukraine",
        "gb": "United Kingdom",
        "uk": "United Kingdom",
        "de": "Germany",
        "pl": "Poland",
        "fr": "France",
        "es": "Spain",
        "it": "Italy",
        "nl": "Netherlands",
        "cz": "Czech Republic",
        "sk": "Slovakia",
        "tr": "Turkey",
        "ca": "Canada",
        "au": "Australia",
        "in": "India",
        "ru": "Russia",
    }
    return m.get((cc or "").lower(), "United States")

def _cc_to_language_name(cc: str) -> str:
    m = {
        "us": "English",
        "gb": "English",
        "uk": "English",
        "ua": "Ukrainian",
        "ru": "Russian",
        "pl": "Polish",
        "de": "German",
        "fr": "French",
        "es": "Spanish",
        "it": "Italian",
        "nl": "Dutch",
        "cz": "Czech",
        "sk": "Slovak",
        "tr": "Turkish",
        "ca": "English",
        "au": "English",
        "in": "English",
    }
    return m.get((cc or "").lower(), "English")


class DataForSEO:
    def __init__(self, login: str, password: str, base: str = "https://api.dataforseo.com"):
        self.base = base.rstrip("/")
        token = f"{login}:{password}".encode()
        self.auth = {"Authorization": "Basic " + base64.b64encode(token).decode()}

    async def _post_tasks(self, path: str, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Правильний формат для DFS v3: {"tasks": [...]}"""
        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                url,
                headers={
                    **self.auth,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json={"tasks": tasks},
            )
            r.raise_for_status()
            return r.json()

    # -------- Backlinks --------
    async def backlinks_live(self, target: str, limit: int = 20, order_by: str = "first_seen,desc", filters=None):
        task = {"target": target, "limit": int(limit), "order_by": [order_by]}
        if filters:
            task["filters"] = filters
        return await self._post_tasks("/v3/backlinks/backlinks/live", [task])

    async def refdomains_live(self, target: str, limit: int = 50, order_by: str = "backlinks,desc"):
        task = {"target": target, "limit": int(limit), "order_by": [order_by]}
        return await self._post_tasks("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(self, target: str, limit: int = 50, order_by: str = "backlinks,desc"):
        task = {"target": target, "limit": int(limit), "order_by": [order_by]}
        return await self._post_tasks("/v3/backlinks/anchors/live", [task])

    # -------- Research: Keywords for Keywords --------
    async def keyword_suggestions(self, seed: str, cc: str):
        # /v3/keywords_data/google/keywords_for_keywords/live
        task = {
            "keywords": [seed],
            "location_name": _cc_to_country_name(cc),
            "language_name": _cc_to_language_name(cc),
            "depth": 1
        }
        return await self._post_tasks("/v3/keywords_data/google/keywords_for_keywords/live", [task])

    # -------- SERP: Google Organic --------
    async def serp_organic(self, keyword: str, cc: str, limit: int = 10):
        # /v3/serp/google/organic/live
        task = {
            "keyword": keyword,
            "location_name": _cc_to_country_name(cc),
            "language_name": _cc_to_language_name(cc),
            "depth": max(10, min(100, int(limit))),
        }
        return await self._post_tasks("/v3/serp/google/organic/live", [task])

    # -------- Keyword Gap (плейсхолдер поки що, щоб не падало) --------
    async def keyword_gap(self, your_domain: str, competitors: List[str], limit: int = 20):
        # TODO: Реалізувати через /v3/keywords_data/google/keywords_for_site/live
        # і diff по ключах. Поки що повертаємо валідну «порожню» відповідь.
        return {
            "tasks": [{
                "status_code": 20000,
                "result": [{
                    "items": []
                }]
            }]
        }

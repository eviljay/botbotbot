import os
from typing import List, Tuple, Optional

from httpx import AsyncClient, HTTPError, ConnectError


class DataForSEO:
    """
    Простий async-клієнт для DataForSEO v3.
    Працює через POST-запити з body = [ {...} ].
    """

    def __init__(self, login: str, password: str, base_url: str = "https://api.dataforseo.com"):
        self.login = login
        self.password = password
        self.base_url = base_url.rstrip("/")

    # ===== базовий POST =====
    async def _post(self, path: str, payload: list) -> dict:
        url = f"{self.base_url}{path}"
        async with AsyncClient(auth=(self.login, self.password), timeout=60) as client:
            r = await client.post(url, json=payload)
            r.raise_for_status()
            return r.json()

    # ========== SERP GOOGLE ORGANIC ==========
    async def serp_google_organic(
        self,
        keyword: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        depth: int = 10,
    ) -> dict:
        """
        /v3/serp/google/organic/live/advanced

        Повертає tasks -> result -> items (standard DataForSEO structure)
        """
        task = {
            "keyword": keyword,
            "location_name": location_name,
            "language_name": language_name,
            "device": "desktop",
            "depth": depth,
        }
        return await self._post("/v3/serp/google/organic/live/advanced", [task])

    # ========== KEYWORD IDEAS (KEYWORDS FOR KEYWORDS) ==========
    async def keywords_for_keywords(
        self,
        keyword: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 100,
    ) -> dict:
        """
        /v3/keywords_data/google_ads/keywords_for_keywords/live

        ВАЖЛИВО: це вже не SERP, а Keywords Data.
        Response: tasks -> result -> items, де items містять keyword, search_volume, cpc і т.д.
        """
        task = {
            "keywords": [keyword],
            "location_name": location_name,
            "language_name": language_name,
            "page": 1,
            "limit": limit,     # бот усе одно ще раз обрізає по limit
        }
        return await self._post("/v3/keywords_data/google_ads/keywords_for_keywords/live", [task])

    # ========== KEYWORD GAP (LABS: KEYWORD_INTERSECTIONS) ==========
    async def keywords_gap(
        self,
        target: str,
        competitors: List[str],
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 50,
    ) -> dict:
        """
        /v3/dataforseo_labs/google/keyword_intersections/live

        Повертає tasks[], де в кожному task -> data.competitors + result[0].items[]
        """
        task = {
            "target": target,
            "competitors": competitors,
            "location_name": location_name,
            "language_name": language_name,
            "limit": limit,
        }
        return await self._post("/v3/dataforseo_labs/google/keyword_intersections/live", [task])

    # ========== DOMAIN INTERSECTION (якщо захочеш повернути) ==========
    async def domain_intersection(
        self,
        target1: str,
        target2: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 50,
    ) -> dict:
        """
        /v3/dataforseo_labs/google/domain_intersection/live
        """
        task = {
            "se_type": "google",
            "target1": target1,
            "target2": target2,
            "location_name": location_name,
            "language_name": language_name,
            "include_serp_info": False,
            "intersections": False,
            "limit": limit,
        }
        return await self._post("/v3/dataforseo_labs/google/domain_intersection/live", [task])

    # ========== BACKLINKS (огляд / ліст / refdomains / anchors) ==========
    async def backlinks_live(
        self,
        target: str,
        limit: int = 100,
        order_by: Optional[str] = None,
    ) -> dict:
        """
        /v3/backlinks/live
        """
        task = {
            "target": target,
            "limit": limit,
        }
        if order_by:
            task["order_by"] = order_by
        return await self._post("/v3/backlinks/live", [task])

    async def backlinks_all(
        self,
        target: str,
        order_by: Optional[str] = None,
        page_size: int = 1000,
        max_total: int = 200000,
    ) -> Tuple[list, int]:
        """
        Пагінація по /v3/backlinks/live, поки не зберемо все або не впремось в max_total.
        Повертає (items_list, approx_total).
        """
        offset = 0
        collected: List[dict] = []
        approx_total = 0

        while True:
            limit = min(page_size, max_total - len(collected))
            if limit <= 0:
                break

            task = {
                "target": target,
                "limit": limit,
                "offset": offset,
            }
            if order_by:
                task["order_by"] = order_by

            resp = await self._post("/v3/backlinks/live", [task])
            tasks = resp.get("tasks") or []
            if not tasks:
                break
            t = tasks[0] or {}
            result = t.get("result") or []
            if not result:
                break
            r0 = result[0]
            items = r0.get("items") or []
            approx_total = r0.get("total_count") or r0.get("total") or approx_total

            if not items:
                break

            collected.extend(items)
            offset += len(items)

            if len(collected) >= max_total:
                break

        return collected, approx_total

    async def backlinks_summary(self, target: str) -> dict:
        """
        /v3/backlinks/summary/live
        """
        task = {"target": target}
        return await self._post("/v3/backlinks/summary/live", [task])

    async def refdomains_live(
        self,
        target: str,
        limit: int = 10,
        order_by: Optional[str] = None,
    ) -> dict:
        """
        /v3/backlinks/referring_domains/live
        """
        task = {
            "target": target,
            "limit": limit,
        }
        if order_by:
            task["order_by"] = order_by
        return await self._post("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(
        self,
        target: str,
        limit: int = 10,
        order_by: Optional[str] = None,
    ) -> dict:
        """
        /v3/backlinks/anchors/live
        """
        task = {
            "target": target,
            "limit": limit,
        }
        if order_by:
            task["order_by"] = order_by
        return await self._post("/v3/backlinks/anchors/live", [task])

    # ========== ON-PAGE AUDIT ==========
    async def onpage_instant(self, url: str) -> dict:
        """
        /v3/on_page/instant_pages
        """
        task = {"url": url}
        return await self._post("/v3/on_page/instant_pages", [task])

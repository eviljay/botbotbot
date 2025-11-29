import base64
from typing import Any, Dict, List, Tuple

from httpx import AsyncClient, HTTPError


class DataForSEO:
    """
    Невеличкий async-клієнт для DataForSEO v3.
    Працюємо через Basic Auth (login:password), BASE береться з ENV.
    """

    def __init__(self, login: str, password: str, base_url: str = "https://api.dataforseo.com") -> None:
        self.base_url = base_url.rstrip("/")
        auth_bytes = f"{login}:{password}".encode("utf-8")
        self._headers = {
            "Authorization": "Basic " + base64.b64encode(auth_bytes).decode("utf-8"),
            "Content-Type": "application/json",
        }

    async def _post(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with AsyncClient(timeout=60) as client:
            resp = await client.post(url, headers=self._headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    # ========= SERP =========

    async def serp_google_organic(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        depth: int = 10,
    ) -> Dict[str, Any]:
        """
        /v3/serp/google/organic/live
        """
        task = {
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "se_domain": "google.com",
            "depth": depth,
        }
        return await self._post("/v3/serp/google/organic/live", [task])

    # ========= KEYWORDS DATA (Google Ads) =========

    async def keywords_for_keywords(
        self,
        keyword: str,
        location_code: int,
        language_code: str,
        sort_by: str = "relevance",
    ) -> Dict[str, Any]:
        """
        /v3/keywords_data/google_ads/keywords_for_keywords/live

        DataForSEO тут *не* приймає page/limit – тільки keywords[], location_code, language_code
        та додаткові параметри типу sort_by.
        """
        task = {
            "keywords": [keyword],
            "location_code": location_code,
            "language_code": language_code,
            "sort_by": sort_by,
        }
        return await self._post("/v3/keywords_data/google_ads/keywords_for_keywords/live", [task])

    # ========= KEYWORD GAP (Labs: domain_intersection) =========

    async def keywords_gap(
        self,
        target: str,
        competitors: List[str],
        location_code: int,
        language_code: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Використовуємо /v3/dataforseo_labs/google/domain_intersection/live

        target1 — наш домен
        target2..4 — конкуренти
        intersections — з ким саме робити перетин.
        """
        comps = competitors[:3]
        task: Dict[str, Any] = {
            "target1": target,
            "include_subdomains": True,
            "search_partners": False,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
        }
        intersections = []

        if len(comps) >= 1:
            task["target2"] = comps[0]
            intersections.append("target2")
        if len(comps) >= 2:
            task["target3"] = comps[1]
            intersections.append("target3")
        if len(comps) >= 3:
            task["target4"] = comps[2]
            intersections.append("target4")

        if intersections:
            task["intersections"] = intersections

        return await self._post("/v3/dataforseo_labs/google/domain_intersection/live", [task])

    # ========= BACKLINKS =========

    async def backlinks_live(
        self,
        target: str,
        limit: int = 100,
        order_by: str = "first_seen,desc",
    ) -> Dict[str, Any]:
        """
        /v3/backlinks/backlinks/live
        """
        task = {
            "target": target,
            "limit": limit,
            "order_by": order_by,
        }
        return await self._post("/v3/backlinks/backlinks/live", [task])

    async def backlinks_all(
        self,
        target: str,
        order_by: str = "first_seen,desc",
        page_size: int = 1000,
        max_total: int = 200000,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Пагінація backlinks/live через limit + offset.
        """
        all_items: List[Dict[str, Any]] = []
        offset = 0
        total = 0

        while True:
            task = {
                "target": target,
                "limit": page_size,
                "offset": offset,
                "order_by": order_by,
            }
            resp = await self._post("/v3/backlinks/backlinks/live", [task])
            tasks = resp.get("tasks") or []
            if not tasks:
                break
            t0 = tasks[0] or {}
            result = (t0.get("result") or [{}])[0]
            items = result.get("items") or []
            if total == 0:
                total = result.get("total_count") or len(items)

            all_items.extend(items)
            if not items or len(items) < page_size or len(all_items) >= max_total:
                break
            offset += page_size

        return all_items[:max_total], total

    async def backlinks_summary(self, target: str) -> Dict[str, Any]:
        task = {"target": target}
        return await self._post("/v3/backlinks/summary/live", [task])

    async def refdomains_live(
        self,
        target: str,
        limit: int = 10,
        order_by: str = "backlinks,desc",
    ) -> Dict[str, Any]:
        task = {
            "target": target,
            "limit": limit,
            "order_by": order_by,
        }
        return await self._post("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(
        self,
        target: str,
        limit: int = 10,
        order_by: str = "backlinks,desc",
    ) -> Dict[str, Any]:
        task = {
            "target": target,
            "limit": limit,
            "order_by": order_by,
        }
        return await self._post("/v3/backlinks/anchors/live", [task])

    # ========= ON-PAGE =========

    async def onpage_instant(self, url: str) -> Dict[str, Any]:
        """
        /v3/on_page/instant_pages
        """
        task = {
            "url": url,
            "enable_javascript": False,
        }
        return await self._post("/v3/on_page/instant_pages", [task])
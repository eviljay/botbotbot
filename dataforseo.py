import base64
from typing import Any, Dict, List, Tuple

from httpx import AsyncClient, HTTPError


class DataForSEO:
    """
    Async-клієнт для DataForSEO v3.
    Працює через Basic Auth (API_LOGIN:API_PASSWORD).
    """

    def __init__(self, login: str, password: str, base_url: str = "https://api.dataforseo.com") -> None:
        self.base_url = base_url.rstrip("/")
        auth_bytes = f"{login}:{password}".encode("utf-8")
        self._headers = {
            "Authorization": "Basic " + base64.b64encode(auth_bytes).decode("utf-8"),
            "Content-Type": "application/json",
        }

    async def _post(self, path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Базовий POST-запит. Повертає сирий JSON від DataForSEO.
        НІЧОГО не парсимо тут — тільки HTTP і JSON.
        """
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
        return await self._post("/v3/serp/google/organic/live/advanced", [task])

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

        ВАЖЛИВО:
        - метод повертає СИРИЙ JSON від DataForSEO
        - парсинг структури (tasks/result/items) робимо окремо в хелпері
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
        /v3/dataforseo_labs/google/domain_intersection/live

        target1 — наш домен
        target2 — конкурент (по одному таску на конкурента)

        Повертаємо один resp з кількома tasks (по одному на кожного конкурента).
        """
        tasks: List[Dict[str, Any]] = []

        for comp in competitors[:3]:
            tasks.append(
                {
                    "target1": target,
                    "target2": comp,
                    "location_code": location_code,
                    "language_code": language_code,
                    "limit": limit,
                    # це з доки: треба, щоб повернули SERP-елементи з рангами
                    "include_serp_info": True,
                    # можна залишити false, поки не треба клікстрім
                    "include_clickstream_data": False,
                }
            )

        if not tasks:
            return {"tasks": []}

        return await self._post(
            "/v3/dataforseo_labs/google/domain_intersection/live",
            tasks,
        )


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

        Повертає:
        - список всіх items (до max_total)
        - total_count (із result.total_count)
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
            result_list = t0.get("result") or []
            if not result_list:
                break

            result = result_list[0] or {}
            items = result.get("items") or []

            if total == 0:
                total = result.get("total_count") or len(items)

            all_items.extend(items)

            if not items or len(items) < page_size or len(all_items) >= max_total:
                break

            offset += page_size

        return all_items[:max_total], total

    async def backlinks_summary(self, target: str) -> Dict[str, Any]:
        """
        /v3/backlinks/summary/live
        """
        task = {"target": target}
        return await self._post("/v3/backlinks/summary/live", [task])

    async def refdomains_live(
        self,
        target: str,
        limit: int = 10,
        order_by: str = "backlinks,desc",
    ) -> Dict[str, Any]:
        """
        /v3/backlinks/referring_domains/live
        """
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
        """
        /v3/backlinks/anchors/live
        """
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

import base64
from typing import Any, Dict, List, Tuple

from httpx import AsyncClient


class DataForSEO:
    """
    Async-клієнт для DataForSEO v3.
    Працює через Basic Auth (API_LOGIN:API_PASSWORD).
    """
    async def suggest_landing_url(
        self,
        keyword: str,
        target_domain: str,
        location_code: int,
        language_code: str,
        depth: int = 20,
    ) -> str | None:
        """
        Підбір релевантної сторінки сайту для keyword'а.
        Логіка:
        - робимо SERP по keyword
        - шукаємо перший результат, де в URL є target_domain
        - повертаємо цей URL, або None якщо нічого не знайшли
        """
        task = {
            "keyword": keyword,
            "location_code": location_code,
            "language_code": language_code,
            "se_domain": "google.com",
            "depth": depth,
        }
        resp = await self._post("/v3/serp/google/organic/live", [task])

        tasks = resp.get("tasks") or []
        if not tasks:
            return None

        t0 = tasks[0] or {}
        result_list = t0.get("result") or []
        if not result_list:
            return None

        items = result_list[0].get("items") or []
        if not items:
            return None

        target_domain = target_domain.lower().strip()
        if target_domain.startswith("http://"):
            target_domain = target_domain[7:]
        if target_domain.startswith("https://"):
            target_domain = target_domain[8:]
        # вирізаємо шлях, залишаємо домен
        target_domain = target_domain.split("/")[0]

        for it in items:
            url = (it.get("url") or it.get("link") or "").lower()
            if not url:
                continue
            if target_domain in url:
                return url

        return None

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

        Повертає сирий JSON.
        """
        task = {
            "keywords": [keyword],
            "location_code": location_code,
            "language_code": language_code,
            "sort_by": sort_by,
        }
        return await self._post("/v3/keywords_data/google_ads/keywords_for_keywords/live", [task])

    async def keywords_for_site(
        self,
        target: str,
        location_code: int,
        language_code: str,
        sort_by: str = "relevance",
    ) -> Dict[str, Any]:
        """
        /v3/keywords_data/google_ads/keywords_for_site/live

        Автоматичний підбір ключових для сайту.
        """
        task = {
            "target": target,
            "location_code": location_code,
            "language_code": language_code,
            "sort_by": sort_by,
        }
        return await self._post("/v3/keywords_data/google_ads/keywords_for_site/live", [task])

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
        target2..4 — конкуренти
        intersections — з ким робити перетин.
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
    ) -> Dict[str, Any]:
        """
        /v3/backlinks/referring_domains/live

        УВАГА: цей endpoint НЕ підтримує поле `order_by`,
        тому відправляємо тільки target + limit (та інші дозволені поля при потребі).
        """
        task = {
            "target": target,
            "limit": limit,
        }
        return await self._post("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(
        self,
        target: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        /v3/backlinks/anchors/live

        Аналогічно – без `order_by`.
        """
        task = {
            "target": target,
            "limit": limit,
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

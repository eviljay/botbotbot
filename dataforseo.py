import base64
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from httpx import AsyncClient, HTTPError

log = logging.getLogger(__name__)


class DataForSEO:
    """
    Простий async-клієнт для DataForSEO.
    Ми тримаємо тут лише ті методи, які реально використовуються ботом.
    """

    def __init__(self, login: str, password: str, base_url: str = "https://api.dataforseo.com"):
        self.login = login
        self.password = password
        self.base_url = base_url.rstrip("/")
        token = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("ascii")
        self._auth_headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }

    def _client(self) -> AsyncClient:
        return AsyncClient(base_url=self.base_url, headers=self._auth_headers, timeout=30)

    async def _post_array(self, path: str, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Більшість endpoint-ів DataForSEO приймає список задач.
        """
        async with self._client() as client:
            r = await client.post(path, json=tasks)
            r.raise_for_status()
            return r.json()

    # ===== SERP =====

    async def serp_google_organic(
        self,
        keyword: str,
        *,
        location_name: str,
        language_name: str,
        depth: int = 10,
    ) -> Dict[str, Any]:
        task = {
            "keyword": keyword,
            "location_name": location_name,
            "language_name": language_name,
            "depth": depth,
        }
        return await self._post_array(
            "/v3/serp/google/organic/live/advanced",
            [task],
        )

    # ===== BACKLINKS =====

    async def backlinks_live(
        self,
        target: str,
        *,
        limit: int = 100,
        order_by: str = "first_seen,desc",
    ) -> Dict[str, Any]:
        task = {
            "target": target,
            "mode": "as_is",
            "order_by": order_by,
            "limit": limit,
        }
        return await self._post_array(
            "/v3/backlinks/backlinks/live",
            [task],
        )

    async def backlinks_summary(self, target: str) -> Dict[str, Any]:
        task = {
            "target": target,
            "mode": "as_is",
        }
        return await self._post_array(
            "/v3/backlinks/summary/live",
            [task],
        )

    async def refdomains_live(
        self,
        target: str,
        *,
        limit: int = 50,
        order_by: str = "backlinks,desc",
    ) -> Dict[str, Any]:
        task = {
            "target": target,
            "mode": "as_is",
            "order_by": order_by,
            "limit": limit,
        }
        return await self._post_array(
            "/v3/backlinks/referring_domains/live",
            [task],
        )

    async def anchors_live(
        self,
        target: str,
        *,
        limit: int = 50,
        order_by: str = "backlinks,desc",
    ) -> Dict[str, Any]:
        task = {
            "target": target,
            "mode": "as_is",
            "order_by": order_by,
            "limit": limit,
        }
        return await self._post_array(
            "/v3/backlinks/anchors/live",
            [task],
        )

    async def backlinks_all(
        self,
        target: str,
        *,
        order_by: str = "first_seen,desc",
        page_size: int = 1000,
        max_total: int = 200000,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Пагінація по backlinks/live, поки не вичерпаємо все або max_total.
        Повертає (items, total_found)
        """
        all_items: List[Dict[str, Any]] = []
        offset = 0
        total_found = 0

        while True:
            task = {
                "target": target,
                "mode": "as_is",
                "order_by": order_by,
                "limit": page_size,
                "offset": offset,
            }
            resp = await self._post_array(
                "/v3/backlinks/backlinks/live",
                [task],
            )
            tasks = resp.get("tasks") or []
            if not tasks:
                break
            res = tasks[0].get("result") or []
            if not res:
                break
            r0 = res[0]
            items = r0.get("items") or []
            total_found = r0.get("total_count") or total_found
            if not items:
                break
            all_items.extend(items)
            offset += len(items)
            if offset >= max_total:
                break
            if offset >= total_found:
                break

        return all_items, int(total_found or 0)

    # ===== KEYWORDS (Google Ads) =====

    async def keywords_for_keywords(
        self,
        keyword: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        keywords_for_keywords НЕ підтримує поле `limit` у JSON.
        Тому тут ми його не шлемо — обрізання робимо в боті.
        """
        task = {
            "keywords": [keyword],
            "location_name": location_name,
            "language_name": language_name,
        }
        return await self._post_array(
            "/v3/keywords_data/google_ads/keywords_for_keywords/live",
            [task],
        )

    async def related_keywords(
        self,
        keyword: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 50,
    ) -> Dict[str, Any]:
        task = {
            "keywords": [keyword],
            "location_name": location_name,
            "language_name": language_name,
            "limit": limit,
        }
        return await self._post_array(
            "/v3/keywords_data/google_ads/related_keywords/live",
            [task],
        )

    # ===== KEYWORD GAP (Labs: keyword_intersections) =====

    async def keywords_gap(
        self,
        target: str,
        competitors: List[str],
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Використовуємо dataforseo_labs keyword_intersections:
        https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_intersections/live

        Окрема задача для кожного конкурента — так у відповіді видно,
        з ким саме порівнюємо.
        """
        tasks: List[Dict[str, Any]] = []
        for comp in competitors:
            tasks.append(
                {
                    "target": target,
                    "competitors": [comp],
                    "location_name": location_name,
                    "language_name": language_name,
                    "include_serp_info": False,
                    "limit": limit,
                }
            )
        return await self._post_array(
            "/v3/dataforseo_labs/google/keyword_intersections/live",
            tasks,
        )

    # ===== OnPage Instant =====

    async def onpage_instant(self, url: str) -> Dict[str, Any]:
        """
        Простий instant-парсинг однієї сторінки.
        """
        task = {
            "id": url,
            "url": url,
            "pingback_url": "",
        }
        return await self._post_array(
            "/v3/on_page/instant_pages",
            [task],
        )

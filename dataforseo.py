import base64
from typing import Any, Dict, List, Tuple

from httpx import AsyncClient, HTTPStatusError


class DataForSEOError(Exception):
    """Кастомна помилка для DataForSEO."""
    pass


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
        """
        url = f"{self.base_url}{path}"

        # 🔍 Тимчасовий дебаг — можна потім прибрати або замінити на logging
        print(f"[DataForSEO] POST {url}")
        print(f"[DataForSEO] Payload: {payload}")

        async with AsyncClient(timeout=60) as client:
            resp = await client.post(url, headers=self._headers, json=payload)

        # ще трішки дебагу
        print(f"[DataForSEO] Status: {resp.status_code}")
        try:
            debug_json = resp.json()
        except Exception:
            debug_json = resp.text
        print(f"[DataForSEO] Response body: {debug_json}")

        try:
            resp.raise_for_status()
        except HTTPStatusError as e:
            try:
                data = resp.json()
            except Exception:
                data = resp.text
            raise DataForSEOError(f"DataForSEO HTTP {resp.status_code}: {data}") from e

        try:
            return resp.json()
        except Exception as e:
            raise DataForSEOError("Invalid JSON from DataForSEO") from e

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
        resp = await self._post("/v3/serp/google/organic/live/advanced", [task])

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

        target_domain_norm = target_domain.lower().strip()
        if target_domain_norm.startswith("http://"):
            target_domain_norm = target_domain_norm[7:]
        if target_domain_norm.startswith("https://"):
            target_domain_norm = target_domain_norm[8:]
        target_domain_norm = target_domain_norm.split("/")[0]

        for it in items:
            url = (it.get("url") or it.get("link") or "").lower()
            if not url:
                continue
            if target_domain_norm in url:
                return url

        return None

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

    # ========= KEYWORD DIFFICULTY (Labs) =========

    async def keyword_difficulty(
        self,
        keywords: List[str],
        location_code: int,
        language_code: str,
    ) -> Dict[str, Any]:
        """
        /v3/dataforseo_labs/google/keyword_difficulty/live

        Складність ключових слів.
        """
        task = {
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code,
        }
        return await self._post("/v3/dataforseo_labs/google/keyword_difficulty/live", [task])
    # ========= LABS: RELEVANT PAGES + RANKED KEYWORDS =========

    async def relevant_pages(
        self,
        target: str,
        location_code: int,
        language_code: str,
        limit: int = 100,
        include_clickstream_data: bool = True,
    ) -> Dict[str, Any]:
        """
        /v3/dataforseo_labs/google/relevant_pages/live

        Повертає список найважливіших сторінок сайту з aggregated метриками.
        """
        task = {
            "target": target,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
            "historical_serp_mode": "live",
            "ignore_synonyms": False,
            "include_clickstream_data": include_clickstream_data,
        }
        return await self._post("/v3/dataforseo_labs/google/relevant_pages/live", [task])

    async def ranked_keywords_for_url(
        self,
        target: str,
        location_code: int,
        language_code: str,
        relative_url: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        /v3/dataforseo_labs/google/ranked_keywords/live

        Витягує ключі, які ранжуються саме для вказаного relative_url.
        """
        task = {
            "target": target,
            "location_code": location_code,
            "language_code": language_code,
            "limit": limit,
            "filters": [
                "ranked_serp_element.serp_item.relative_url",
                "=",
                relative_url,
            ],
        }
        return await self._post("/v3/dataforseo_labs/google/ranked_keywords/live", [task])

    # ========= KEYWORD GAP (Labs: domain_intersection) =========

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
        Keyword Gap на базі /v3/dataforseo_labs/google/domain_intersection/live

        target  — наш сайт (наприклад, "fotoklok.se")
        competitors — список доменів-конкурентів (до 3 шт.)

        Логіка:
        для КОЖНОГО конкурента окремий task:
          target1 = конкурент
          target2 = наш домен
          intersections = false (ключі, де конкурент ранжується, а ми — ні)
        """

        if not competitors:
            raise DataForSEOError("Для keyword gap потрібен хоча б один конкурент")

        tasks: List[Dict[str, Any]] = []
        for comp in competitors[:3]:
            task = {
                "target1": comp,
                "target2": target,
                "location_code": location_code,
                "language_code": language_code,
                "intersections": False,
                "include_serp_info": True,
                "limit": limit,
            }
            tasks.append(task)

        # НІЯКИХ api/function тут не має бути
        return await self._post(
            "/v3/dataforseo_labs/google/domain_intersection/live",
            tasks,
        )


    # ========= BACKLINKS =========

    async def backlinks_live(
        self,
        target: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        /v3/backlinks/backlinks/live

        Використовує всі прапорці як у прикладі:
        - тільки live посилання
        - включає субдомени
        - виключає внутрішні посилання
        - включає indirect links
        - mode = as_is
        - rank_scale = one_hundred
        """
        task = {
            "target": target,
            "limit": limit,
            "offset": offset,
            "internal_list_limit": 10,
            "backlinks_status_type": "live",
            "include_subdomains": True,
            "exclude_internal_backlinks": True,
            "include_indirect_links": True,
            "mode": "as_is",
            "rank_scale": "one_hundred",
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

        Використовує ті самі прапорці, що і backlinks_live,
        щоб totals відповідали.
        """
        task = {
            "target": target,
            "internal_list_limit": 10,
            "backlinks_status_type": "live",
            "include_subdomains": True,
            "exclude_internal_backlinks": True,
            "include_indirect_links": True,
            "rank_scale": "one_hundred",
        }
        return await self._post("/v3/backlinks/summary/live", [task])

    async def refdomains_live(
        self,
        target: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        /v3/backlinks/referring_domains/live

        УВАГА: endpoint НЕ підтримує поле `order_by`.
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

        Аналогічно — без `order_by`.
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

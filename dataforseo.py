# dataforseo.py
import base64
import httpx
from typing import List, Dict, Any, Optional


class DataForSEO:
    """
    Легка async-обгортка над DataForSEO v3.
    Базується на масивних POST-и з одним або кількома task-ами.
    """

    def __init__(self, login: str, password: str, base: str = "https://api.dataforseo.com"):
        self.base = base.rstrip("/")
        token = f"{login}:{password}".encode()
        self.auth = {"Authorization": "Basic " + base64.b64encode(token).decode()}

    # ========== базові helpers ==========

    async def _post_array(self, path: str, tasks: List[Dict[str, Any]], timeout: int = 60) -> Dict[str, Any]:
        """
        Відправляє масив tasks на конкретний endpoint (path має починатись з /v3/...).
        """
        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(
                url,
                headers={
                    **self.auth,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=tasks,
            )
            r.raise_for_status()
            return r.json()

     # ===== Backlinks =====
    async def backlinks_live(self, target: str, limit: int = 20, order_by: str = "first_seen,desc", filters=None):
        task = {"target": target, "limit": limit, "order_by": [order_by]}
        if filters:
            task["filters"] = filters
        return await self._post_array("/v3/backlinks/backlinks/live", [task])

    async def backlinks_live_page(self, target: str, limit: int = 1000, offset: int = 0,
                                  order_by: str = "first_seen,desc", filters=None):
        """Повертає одну сторінку беклінків + total_count."""
        task = {"target": target, "limit": limit, "offset": offset, "order_by": [order_by]}
        if filters:
            task["filters"] = filters
        resp = await self._post_array("/v3/backlinks/backlinks/live", [task])
        tasks = resp.get("tasks") or []
        t = tasks[0] if tasks else {}
        result = (t.get("result") or [{}])[0]
        items = result.get("items") or []
        total_count = result.get("total_count") or len(items)
        return items, int(total_count)

    async def backlinks_all(self, target: str, order_by: str = "first_seen,desc", filters=None,
                            page_size: int = 1000, max_total: int = 200000):
        """Йде по сторінках поки не збере всі (або до max_total)."""
        all_items = []
        offset = 0
        total = None
        while True:
            items, total_count = await self.backlinks_live_page(
                target, limit=page_size, offset=offset, order_by=order_by, filters=filters
            )
            if total is None:
                total = total_count
            if not items:
                break
            all_items.extend(items)
            offset += len(items)
            if offset >= total_count or offset >= max_total:
                break
        # total повертаємо як мін(total, max_total), щоб не обіцяти більше, ніж віддали
        return all_items, min(total or len(all_items), max_total)

    async def refdomains_live(self, target: str, limit: int = 50, order_by: str = "backlinks,desc"):
        task = {"target": target, "limit": limit, "order_by": [order_by]}
        return await self._post_array("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(self, target: str, limit: int = 50, order_by: str = "backlinks,desc"):
        task = {"target": target, "limit": limit, "order_by": [order_by]}
        return await self._post_array("/v3/backlinks/anchors/live", [task])

    async def backlinks_summary(self, target: str):
        task = {"target": target}
        return await self._post_array("/v3/backlinks/summary/live", [task])

    # ======================================================
    #                       SERP API
    # ======================================================

    async def serp_google_top10(
        self,
        query: str,
        location_name: Optional[str] = None,
        language_code: Optional[str] = None,
        se_name: str = "google.com",
        device: str = "desktop",
        depth: int = 10,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Топ-10 органіки Google (advanced live).
        Через advanced можна отримати більше полів (SERP features).
        """
        task: Dict[str, Any] = {
            "keyword": query,
            "se_name": se_name,          # наприклад: "google.com"
            "device": device,            # desktop / mobile
            "depth": depth,              # 10 -> ~топ-10
        }
        if location_name:
            task["location_name"] = location_name  # наприклад: "Ukraine"
        if language_code:
            task["language_code"] = language_code  # "uk", "en", "ru" тощо

        task.update(extra)
        # advanced endpoint
        return await self._post_array("/v3/serp/google/organic/live/advanced", [task])

    # ======================================================
    #                    KEYWORDS DATA API
    # ======================================================

    async def keywords_ideas(
        self,
        seed: str,
        location_name: Optional[str] = None,
        language_code: Optional[str] = None,
        limit: int = 100,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Ідеї ключових (Google Ads) за seed-запитом.
        Бере ендпоінт 'keywords_for_keywords/live'.
        """
        task: Dict[str, Any] = {
            "keywords": [seed],
            "limit": limit,
        }
        if location_name:
            task["location_name"] = location_name
        if language_code:
            task["language_code"] = language_code

        task.update(extra)
        # За документацією назва сервісу: google_ads
        return await self._post_array("/v3/keywords_data/google_ads/keywords_for_keywords/live", [task])

    async def keywords_search_volume(
        self,
        keywords: List[str],
        location_name: Optional[str] = None,
        language_code: Optional[str] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Отримати search volume / CPC для набору ключових.
        """
        task: Dict[str, Any] = {"keywords": keywords}
        if location_name:
            task["location_name"] = location_name
        if language_code:
            task["language_code"] = language_code

        task.update(extra)
        return await self._post_array("/v3/keywords_data/google_ads/search_volume/live", [task])

    # ======================================================
    #                         LABS API
    # ======================================================

    async def labs_keyword_gap(
        self,
        my_domain: str,
        competitors: List[str],
        limit: int = 100,
        include_intersections: bool = False,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Keyword Gap: унікальні/перетинні ключі між доменами.
        За API DataForSEO Labs (припускаємо endpoint 'keywords_gaps/live').
        """
        task: Dict[str, Any] = {
            "target": my_domain,
            "targets": competitors,
            "limit": limit,
            "include_intersections": include_intersections,
        }
        task.update(extra)
        return await self._post_array("/v3/dataforseo_labs/keywords_gaps/live", [task])

    # ======================================================
    #                       ON-PAGE API
    # ======================================================

    async def onpage_audit(
        self,
        url: str,
        enable_javascript: bool = False,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Швидкий технічний аудит окремого URL.
        'instant_pages' повертає статус-код, мета-теги, H1/H2, canonical тощо.
        """
        task: Dict[str, Any] = {
            "url": url,
            "enable_javascript": enable_javascript,
        }
        task.update(extra)
        return await self._post_array("/v3/on_page/instant_pages", [task])

    # ======================================================
    #                    DOMAIN ANALYTICS API
    # ======================================================

    async def domain_overview(
        self,
        domain: str,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Базовий огляд домену (Labs overview).
        Повертає high-level метрики домену.
        """
        task: Dict[str, Any] = {"domain": domain}
        task.update(extra)
        return await self._post_array("/v3/dataforseo_labs/domain_rank_overview/live", [task])

    async def domain_technologies(
        self,
        domain: str,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Технології на сайті (якщо підключено у вашому тарифі).
        """
        task: Dict[str, Any] = {"domain": domain}
        task.update(extra)
        return await self._post_array("/v3/domain_analytics/technologies/live", [task])

    # ======================================================
    #                   CONTENT ANALYSIS API
    # ======================================================

    async def content_mentions(
        self,
        query: str,
        page: int = 1,
        limit: int = 10,
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Згадки бренду/фрази у новинах/блогах.
        """
        task: Dict[str, Any] = {
            "query": query,
            "page": page,
            "limit": limit,
        }
        task.update(extra)
        return await self._post_array("/v3/content_analysis/mentions/live", [task])

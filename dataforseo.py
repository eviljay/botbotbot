# dataforseo.py
import base64
from typing import List, Tuple, Optional

import httpx


class DataForSEO:
    def __init__(self, login: str, password: str, base: str = "https://api.dataforseo.com"):
        """
        Простий клієнт для DataForSEO v3.
        Використовує Basic Auth: login:password.
        """
        self.base = base.rstrip("/")
        token = f"{login}:{password}".encode()
        self.auth = {"Authorization": "Basic " + base64.b64encode(token).decode()}

    async def _post_array(self, path: str, tasks: List[dict]):
        """
        Всі DataForSEO ендпоінти очікують масив задач.
        """
        url = f"{self.base}{path}"
        async with httpx.AsyncClient(timeout=90) as client:
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

    # ========= Backlinks =========

    async def backlinks_live(
        self,
        target: str,
        limit: int = 20,
        order_by: str = "first_seen,desc",
        filters: Optional[list] = None,
        offset: int = 0,
    ):
        """
        /v3/backlinks/backlinks/live
        """
        task: dict = {
            "target": target,
            "limit": limit,
            "order_by": [order_by],
            "offset": offset,
        }
        if filters:
            task["filters"] = filters
        return await self._post_array("/v3/backlinks/backlinks/live", [task])

    async def refdomains_live(
        self,
        target: str,
        limit: int = 50,
        order_by: str = "backlinks,desc",
    ):
        """
        /v3/backlinks/referring_domains/live
        """
        task = {
            "target": target,
            "limit": limit,
            "order_by": [order_by],
        }
        return await self._post_array("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(
        self,
        target: str,
        limit: int = 50,
        order_by: str = "backlinks,desc",
    ):
        """
        /v3/backlinks/anchors/live
        """
        task = {
            "target": target,
            "limit": limit,
            "order_by": [order_by],
        }
        return await self._post_array("/v3/backlinks/anchors/live", [task])

    async def backlinks_summary(self, target: str):
        """
        /v3/backlinks/summary/live
        """
        task = {"target": target}
        return await self._post_array("/v3/backlinks/summary/live", [task])

    async def backlinks_all(
        self,
        target: str,
        order_by: str = "first_seen,desc",
        page_size: int = 1000,
        max_total: int = 200000,
        filters: Optional[list] = None,
    ) -> Tuple[List[dict], int]:
        """
        Повна вибірка беклінків з пагінацією.
        Повертає (всі_рядки_до_ліміту, оцінка_total).
        """
        all_items: List[dict] = []
        total_estimate = 0
        offset = 0

        while True:
            resp = await self.backlinks_live(
                target=target,
                limit=page_size,
                order_by=order_by,
                filters=filters,
                offset=offset,
            )
            tasks = resp.get("tasks") or []
            if not tasks:
                break
            t0 = tasks[0] or {}
            if t0.get("status_code") and t0["status_code"] != 20000:
                raise RuntimeError(t0.get("status_message") or f"Task error: {t0.get('status_code')}")

            result = t0.get("result") or []
            if not result:
                break
            r0 = result[0]
            items = r0.get("items") or []

            if total_estimate == 0:
                total_estimate = r0.get("total_count") or r0.get("available") or 0

            if not items:
                break

            all_items.extend(items)
            if len(all_items) >= max_total:
                all_items = all_items[:max_total]
                break

            if len(items) < page_size:
                break

            offset += page_size

        if not total_estimate:
            total_estimate = len(all_items)
        return all_items, int(total_estimate)

    # ========= SERP (Google Organic) =========

    async def serp_google_organic(
        self,
        keyword: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        depth: int = 10,
    ):
        """
        /v3/serp/google/organic/live/advanced
        """
        task = {
            "keyword": keyword,
            "location_name": location_name,
            "language_name": language_name,
            "depth": depth,
        }
        return await self._post_array("/v3/serp/google/organic/live/advanced", [task])

    # ========= Keyword Ideas / Volume =========

    async def keywords_for_keywords(
        self,
        seed: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
    ):
        """
        /v3/keywords_data/google_ads/keywords_for_keywords/live

        ВАЖЛИВО:
        Цей ендпоінт НЕ приймає поле 'limit', тому ми його не відправляємо.
        Ліміт ріжемо вже в боті (по кількості items).
        """
        task = {
            "keywords": [seed],
            "location_name": location_name,
            "language_name": language_name,
            # без "limit" – інакше: "Unknown Fields in POST Data: limit"
        }
        return await self._post_array("/v3/keywords_data/google_ads/keywords_for_keywords/live", [task])

    async def google_ads_search_volume(
        self,
        keywords: List[str],
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
    ):
        """
        /v3/keywords_data/google_ads/search_volume/live
        """
        task = {
            "keywords": keywords,
            "location_name": location_name,
            "language_name": language_name,
        }
        return await self._post_array("/v3/keywords_data/google_ads/search_volume/live", [task])

    # ========= GAP / Domain Intersection =========

    async def domain_intersection(
        self,
        target1: str,
        target2: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        intersections: bool = True,
        limit: int = 50,
    ):
        """
        /v3/dataforseo_labs/google/domain_intersection/live

        intersections=True  -> перетин (обоє ранжуються)
        intersections=False -> "gap" (target1 ранжується, target2 — ні)
        """
        task = {
            "target1": target1,
            "target2": target2,
            "location_name": location_name,
            "language_name": language_name,
            "intersections": intersections,
            "limit": limit,
        }
        return await self._post_array("/v3/dataforseo_labs/google/domain_intersection/live", [task])

    async def keywords_gap(
        self,
        target: str,
        competitors: List[str],
        mode: str = "gap_from_competitors",  # "gap_from_competitors" | "gap_from_target" | "intersection"
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 50,
    ):
        """
        Зручна обгортка над /v3/dataforseo_labs/google/domain_intersection/live.

        mode:
          - "gap_from_competitors": КС, де конкурент ранжується, а target — ні
          - "gap_from_target":      КС, де target ранжується, а конкурент — ні
          - "intersection":         спільні КС
        """
        tasks: List[dict] = []

        for comp in competitors:
            if mode == "intersection":
                # просто перетин
                t1, t2 = target, comp
                intersections = True
            elif mode == "gap_from_target":
                # target ранжується, конкурент — ні
                t1, t2 = target, comp
                intersections = False
            else:
                # "gap_from_competitors": конкурент ранжується, target — ні
                t1, t2 = comp, target
                intersections = False

            tasks.append(
                {
                    "target1": t1,
                    "target2": t2,
                    "location_name": location_name,
                    "language_name": language_name,
                    "intersections": intersections,
                    "limit": limit,
                }
            )

        # тут вже точно правильний ендпоінт з полями target1/target2
        return await self._post_array("/v3/dataforseo_labs/google/domain_intersection/live", tasks)

    # ========= On-Page instant =========

    async def onpage_instant(self, url: str):
        """
        /v3/on_page/instant_pages

        Туди передаємо поле "url".
        """
        task = {"url": url}
        return await self._post_array("/v3/on_page/instant_pages", [task])

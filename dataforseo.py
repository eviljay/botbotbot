# dataforseo.py
import base64
from typing import List, Tuple, Optional

import httpx


class DataForSEO:
    def __init__(self, login: str, password: str, base: str = "https://api.dataforseo.com"):
        self.base = base.rstrip("/")
        token = f"{login}:{password}".encode()
        self.auth = {"Authorization": "Basic " + base64.b64encode(token).decode()}

    async def _post_array(self, path: str, tasks: list[dict]):
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
        task = {"target": target, "limit": limit, "order_by": [order_by], "offset": offset}
        if filters:
            task["filters"] = filters
        return await self._post_array("/v3/backlinks/backlinks/live", [task])

    async def refdomains_live(self, target: str, limit: int = 50, order_by: str = "backlinks,desc"):
        task = {"target": target, "limit": limit, "order_by": [order_by]}
        return await self._post_array("/v3/backlinks/referring_domains/live", [task])

    async def anchors_live(self, target: str, limit: int = 50, order_by: str = "backlinks,desc"):
        task = {"target": target, "limit": limit, "order_by": [order_by]}
        return await self._post_array("/v3/backlinks/anchors/live", [task])

    async def backlinks_summary(self, target: str):
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
        """Повертає (всі_рядки_до_ліміту, оцінка_total)."""
        all_items: List[dict] = []
        total_estimate = 0
        offset = 0

        while True:
            resp = await self.backlinks_live(
                target=target, limit=page_size, order_by=order_by, filters=filters, offset=offset
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

        return all_items, int(total_estimate) if total_estimate else len(all_items)

    # ========= SERP (Google Organic) =========
    async def serp_google_organic(
        self,
        keyword: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        depth: int = 10,
    ):
        task = {
            "keyword": keyword,
            "location_name": location_name,
            "language_name": language_name,
            "depth": depth,
        }
        return await self._post_array("/v3/serp/google/organic/live/advanced", [task])

    # ========= Keywords: ideas + volume =========
    async def related_keywords(
        self,
        seed: str,
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 20,
    ):
        task = {
            "keywords": [seed],
            "location_name": location_name,
            "language_name": language_name,
            "limit": limit,
        }
        return await self._post_array("/v3/keywords_data/related_keywords/live", [task])

    async def google_ads_search_volume(
        self,
        keywords: List[str],
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
    ):
        task = {
            "keywords": keywords,
            "location_name": location_name,
            "language_name": language_name,
        }
        return await self._post_array("/v3/keywords_data/google_ads/search_volume/live", [task])

   # DataForSEO Labs: Domain Intersection (GAP)
async def keywords_gap(
        self,
        target: str,
        competitors: list[str],
        location_name: str = "Ukraine",
        language_name: str = "Ukrainian",
        limit: int = 50,
    ):
        """
        GAP-аналіз через DataForSEO Labs: Google Domain Intersection (intersections=False).
        Для кожного конкурента робимо окремий live-запит target1=competitor, target2=your domain.
        """
        target2 = self._clean_domain(target)
        results = []

        for comp in competitors:
            target1 = self._clean_domain(comp)
            task = {
                "target1": target1,
                "target2": target2,
                "location_name": location_name,   # або використай location_code
                "language_name": language_name,   # або language_code
                "intersections": False,           # ← GAP
                "limit": int(limit),
            }
            res = await self._post_array(
                "/v3/dataforseo_labs/google/domain_intersection/live",
                [task],
            )
            results.append({"competitor": target1, "data": res})

        return results 

    # ========= On-Page instant =========
    async def onpage_instant(self, url: str):
        task = {"url": url}
        return await self._post_array("/v3/on_page/instant_pages", [task])

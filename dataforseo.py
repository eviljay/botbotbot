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

      # ===== Keywords Ideas =====
            if aw == "keywords":
                # 1) тягнемо ідеї
                resp = await dfs.related_keywords(
                    main,
                    location_name=country,
                    language_name=lang,
                    limit=limit,
                )
                items = _extract_first_items(resp)
                if not items:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")

                # 2) готуємо список ключів для volume/CPC
                kw_list = []
                for it in items:
                    k = (it.get("keyword")
                         or (it.get("keyword_data") or {}).get("keyword")
                         or it.get("keyword_text"))
                    if k:
                        kw_list.append(k.strip())
                # унікалізуємо, обрізаємо, щоб не переборщити з апі
                kw_list = list(dict.fromkeys(kw_list))[:200]

                # 3) тягнемо обсяги/CPC (якщо є що тягнути)
                vol_map = {}
                if kw_list:
                    vresp = await dfs.google_ads_search_volume(
                        kw_list,
                        location_name=country,
                        language_name=lang,
                    )
                    vitems = _extract_first_items(vresp)
                    for vi in vitems:
                        kk  = vi.get("keyword") or vi.get("keyword_text")
                        vol = (vi.get("search_volume")
                               or vi.get("avg_monthly_searches")
                               or vi.get("search_volume_avg")
                               or 0)
                        cpc = vi.get("cpc") or vi.get("average_cpc") or 0
                        if kk:
                            vol_map[kk.lower()] = (vol, cpc)

                # 4) прев’ю 10 рядків (без Markdown, щоб не ловити Can't parse entities)
                lines = []
                for it in items[:10]:
                    kk = (it.get("keyword")
                          or (it.get("keyword_data") or {}).get("keyword")
                          or it.get("keyword_text")
                          or "—")
                    vol, cpc = vol_map.get((kk or "").lower(), ("-", "-"))
                    lines.append(f"• {kk} — vol: {vol}, CPC: {cpc}")

                preview = "🧠 Ідеї ключових (топ-10):\n" + "\n".join(lines)

                # 5) CSV (повний список із vol/CPC де є)
                import io, csv
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "cpc"])
                for k in kw_list:
                    vol, cpc = vol_map.get(k.lower(), ("", ""))
                    w.writerow([k, vol, cpc])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                # ВАЖЛИВО: без parse_mode, щоб не падало на спецсимволах у ключах
                await update.message.reply_text(preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}")
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="keyword_ideas.csv"),
                    caption="CSV з ідеями ключових (із обсягом/CPC де доступно)"
                )
                return

# ========= Labs: Keyword Gap =========
async def keywords_gap(
    self,
    target: str,
    competitors: List[str],
    location_name: str = "Ukraine",
    language_name: str = "Ukrainian",
    limit: int = 50,
):
    task = {
        "target": target,
        "competitors": competitors,
        "location_name": location_name,
        "language_name": language_name,
        "limit": limit,
    }
    # DataForSEO Labs: google/keyword_intersections/live
    return await self._post_array(
        "/v3/dataforseo_labs/google/keyword_intersections/live",
        [task]
    )


    # ========= On-Page instant =========
    async def onpage_instant(self, url: str):
        task = {"url": url}
        return await self._post_array("/v3/on_page/instant_pages", [task])

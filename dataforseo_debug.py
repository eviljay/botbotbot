 
LOGIN = "info@seoswiss.online"
PASSWORD = "d752edcc5e5dbd73"

#!/usr/bin/env python3
import base64
import json
import sys
import asyncio
from typing import Any, Dict, List

from httpx import AsyncClient, HTTPStatusError


# =================== НАЛАШТУВАННЯ ===================

# ⚠️ СЮДИ ВСТАВ API LOGIN / API PASSWORD Зі СТОРІНКИ:
# https://app.dataforseo.com/api/access
API_LOGIN = "info@seoswiss.online"
API_PASSWORD = "d752edcc5e5dbd73"

BASE_URL = "https://api.dataforseo.com"


# =================== ХЕЛПЕРИ ===================

def extract_kfk_items(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Акуратно дістає список keyword-ів із відповіді keywords_for_keywords.

    Підтримує два формати:
    1) Класичний (як у playground):
       {
         "status_code": 20000,
         "result_count": 2,
         "result": [ {...}, {...} ]
       }

    2) Потенційний формат з tasks (на майбутнє):
       {
         "tasks": [
           {
             "result": [
               {
                 "items": [ {...}, {...} ]
               }
             ]
           }
         ]
       }
    """
    if not isinstance(resp, dict):
        return []

    # 1) Формат як у playground
    if "result" in resp and isinstance(resp["result"], list):
        return resp["result"]

    # 2) Формат із tasks/items
    tasks = resp.get("tasks") or []
    if not tasks:
        return []

    t0 = tasks[0] or {}
    result_list = t0.get("result") or []
    if not result_list:
        return []

    first_result = result_list[0] or {}
    items = first_result.get("items") or []
    return items


async def debug_keywords_for_keywords(
    keyword: str,
    location_code: int,
    language_code: str,
    sort_by: str = "relevance",
) -> Dict[str, Any]:
    """
    Робить запит до /v3/keywords

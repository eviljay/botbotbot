import os
import uuid
from urllib.parse import urlencode

PORTMONE_STATIC_LINK = os.getenv("PORTMONE_PAYMENT_LINK_STATIC", "").strip()

def make_order_id() -> str:
    return uuid.uuid4().hex[:12]

def build_payment_link(order_id: str, amount: float | None, description: str | None) -> str:
    """
    MVP-режим:
    - Якщо у тебе вже є згенероване багаторазове посилання Portmone (коротке prt.mn або pay.portmone...),
      просто повертаємо його як є.
    - Якщо треба додавати параметри (наприклад показувати суму/опис на сторінці) і твій лінк це підтримує,
      можна прикрутити query-параметри (опційно).
    """
    if not PORTMONE_STATIC_LINK:
        raise RuntimeError("PORTMONE_PAYMENT_LINK_STATIC is not set")

    # Базово — просто повертаємо статичний лінк
    url = PORTMONE_STATIC_LINK

    # Якщо твій лінк дозволяє query-параметри (не завжди!), дозапишемо їх:
    qp = {}
    if amount:
        qp["amount"] = f"{amount:.2f}"
    if description:
        qp["desc"] = description[:120]

    if qp:
        sep = "&" if ("?" in url) else "?"
        url = f"{url}{sep}{urlencode(qp)}"

    return url

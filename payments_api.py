# payments_api.py
import os
import json
import base64
import hashlib
import logging

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# ====== Логи ======
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payments-api")

# ====== ENV ======
load_dotenv()

PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "")
PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")   # https://<твой-домен>/thanks
SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")   # https://<твой-домен>/liqpay/callback

if not PUBLIC_KEY or not PRIVATE_KEY:
    raise RuntimeError("Need LIQPAY_PUBLIC_KEY and LIQPAY_PRIVATE_KEY in .env")

app = FastAPI(title="Payments API (LiqPay)")

# ====== Утиліти ======
def liqpay_encode(payload: dict) -> str:
    b = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return base64.b64encode(b).decode()

def liqpay_sign(data_b64: str) -> str:
    s = PRIVATE_KEY + data_b64 + PRIVATE_KEY
    return base64.b64encode(hashlib.sha1(s.encode()).digest()).decode()

# ====== API ======
@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Створення платежу
    Body:
    {
      "user_id": 244142655,
      "amount": 100,
      "currency": "UAH" (optional)
    }
    """
    body = await req.json()
    user_id = body.get("user_id")
    amount = body.get("amount")
    currency = body.get("currency", "UAH")

    if not user_id or not amount:
        raise HTTPException(400, "user_id and amount required")

    payload = {
        "version": "3",
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": str(amount),
        "currency": currency,
        "description": f"Top-up {amount} UAH for {user_id}",
        "result_url": RESULT_URL,
        "server_url": SERVER_URL,
        "order_id": f"{user_id}-{os.urandom(4).hex()}",
    }

    data_b64 = liqpay_encode(payload)
    signature = liqpay_sign(data_b64)

    # робимо запит до LiqPay API
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post("https://www.liqpay.ua/api/request", json={
            "data": data_b64,
            "signature": signature,
        })
        try:
            r.raise_for_status()
        except Exception as e:
            log.error("LiqPay API error: %s", r.text)
            raise HTTPException(502, f"LiqPay API error: {e}")

        resp = r.json()

    log.info("LiqPay resp: %s", resp)

    pay_url = resp.get("href")
    if not pay_url:
        raise HTTPException(502, f"LiqPay did not return href: {resp}")

    return JSONResponse({
        "ok": True,
        "pay_url": pay_url,
        "order_id": payload["order_id"],
    })

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """Обробка callback від LiqPay (сервер-сервер)."""
    data = (await req.form()).get("data")
    signature = (await req.form()).get("signature")
    if not data or not signature:
        raise HTTPException(400, "Missing data or signature")

    # перевірка підпису
    expected = liqpay_sign(data)
    if expected != signature:
        raise HTTPException(400, "Invalid signature")

    payload = json.loads(base64.b64decode(data).decode("utf-8"))
    log.info("Callback payload: %s", payload)
    # TODO: оновити баланс у БД

    return JSONResponse({"ok": True})

@app.get("/thanks")
async def thanks_page():
    return HTML_OK


# ====== HTML для result_url ======
HTML_OK = """
<html><body style="font-family:system-ui">
  <h1>✅ Оплату отримано</h1>
  <p>Дякуємо! Тепер можете повернутися в бот.</p>
</body></html>
"""

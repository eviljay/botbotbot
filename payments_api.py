# payments_api.py
import os
import re
import json
import math
import base64
import hashlib
import logging
import sqlite3
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from dotenv import load_dotenv
import httpx

# ====== Логи ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("payments-api")

# ====== ENV ======
load_dotenv()

PUBLIC_KEY         = os.getenv("LIQPAY_PUBLIC_KEY", "")
PRIVATE_KEY        = os.getenv("LIQPAY_PRIVATE_KEY", "")
RESULT_URL         = os.getenv("LIQPAY_RESULT_URL", "")     # напр.: https://server1.seoswiss.online/thanks
SERVER_URL         = os.getenv("LIQPAY_SERVER_URL", "")     # напр.: https://server1.seoswiss.online/liqpay/callback
DEFAULT_CCY        = os.getenv("LIQPAY_CURRENCY", "UAH")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH            = os.getenv("DB_PATH", "bot.db")
CREDIT_PRICE_UAH   = float(os.getenv("CREDIT_PRICE_UAH", "5"))

if not PUBLIC_KEY or not PRIVATE_KEY:
    raise RuntimeError("Set LIQPAY_PUBLIC_KEY and LIQPAY_PRIVATE_KEY in .env")

# ====== FastAPI ======
app = FastAPI(title="Payments API (LiqPay)")

# Пам'ять для редіректу: /pay/{order_id} -> pay_url
ORDER_CACHE: dict[str, str] = {}

# ====== Утиліти ======
def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def _liqpay_encode(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _b64(raw)

def _liqpay_sign(data_b64: str) -> str:
    # signature = base64( sha1( private_key + data + private_key ) )
    to_sign = (PRIVATE_KEY + data_b64 + PRIVATE_KEY).encode("utf-8")
    digest = hashlib.sha1(to_sign).digest()
    return _b64(digest)

def _gen_order_id(user_id) -> str:
    # короткий унікальний id, з якого можна витягнути user_id у колбеку
    return f"{user_id}-{os.urandom(6).hex()}"

# ====== API ======
@app.get("/health")
async def health():
    return {"ok": True, "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
      { "user_id": 244142655, "amount": 100, "currency": "UAH" }
    Відповідь:
      { ok, provider, order_id, pay_url }
    """
    body = await req.json()
    user_id = body.get("user_id")
    amount  = body.get("amount")
    currency = (body.get("currency") or DEFAULT_CCY).upper()

    if not user_id or not amount:
        raise HTTPException(400, "user_id and amount required")

    order_id = body.get("order_id") or _gen_order_id(user_id)

    payload = {
        "version": "3",
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": f"{float(amount):.2f}",
        "currency": currency,
        "description": f"Top-up {amount} credits",
        "order_id": order_id,
        "server_url": SERVER_URL,
        "result_url": RESULT_URL,
        # "sandbox": "1",  # включай при тестах у LiqPay
    }

    data_b64   = _liqpay_encode(payload)
    signature  = _liqpay_sign(data_b64)
    pay_url    = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    ORDER_CACHE[order_id] = pay_url
    log.info("Create payment: user=%s amount=%s %s order_id=%s", user_id, amount, currency, order_id)

    return JSONResponse({
        "ok": True,
        "provider": "liqpay",
        "order_id": order_id,
        "pay_url": pay_url
    })

@app.get("/pay/{order_id}")
async def pay_redirect(order_id: str):
    pay_url = ORDER_CACHE.get(order_id)
    if not pay_url:
        raise HTTPException(404, "Unknown order_id")
    return RedirectResponse(pay_url, status_code=302)

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    Серверний колбек від LiqPay (POST form-data: data, signature).
    Перевіряємо підпис, оновлюємо баланс і шлемо повідомлення в Telegram.
    """
    form = await req.form()
    data_b64  = form.get("data")
    sign_recv = form.get("signature")

    if not data_b64 or not sign_recv:
        raise HTTPException(400, "Missing data or signature")

    sign_calc = _liqpay_sign(data_b64)
    if sign_calc != sign_recv:
        log.warning("Invalid signature callback")
        raise HTTPException(400, "Invalid signature")

    payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    log.info("LiqPay callback: %s", payload)

    status   = (payload.get("status") or "").lower()      # success, failure, sandbox, etc.
    order_id = payload.get("order_id") or ""
    amount   = float(payload.get("amount") or 0)

    # Витягуємо user_id з order_id "<user>-<hex>"
    m = re.match(r"^(\d+)-", str(order_id))
    if not m:
        log.error("Cannot parse user_id from order_id=%s", order_id)
        return JSONResponse({"ok": False, "reason": "bad_order_id"})
    user_id = int(m.group(1))

    if status in ("success", "sandbox"):
        credits = max(1, math.ceil(amount / CREDIT_PRICE_UAH))

        # 1) Оновлюємо баланс у БД бота
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, balance INTEGER DEFAULT 0, phone TEXT)")
                conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, ?)", (user_id, 0))
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, user_id))
                conn.commit()
        except Exception as e:
            log.exception("DB update error")
            return JSONResponse({"ok": False, "reason": f"db_error: {e}"})

        # 2) Сповіщення в Telegram
        if TELEGRAM_BOT_TOKEN:
            msg = f"✅ Оплату отримано: +{amount:.0f}₴ → +{credits} кредит(и). Дякуємо!"
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    r = await c.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id": user_id, "text": msg}
                    )
                    r.raise_for_status()
            except Exception:
                log.exception("Telegram sendMessage failed")
    else:
        log.info("Payment not successful: status=%s order_id=%s", status, order_id)

    return JSONResponse({"ok": True})

@app.get("/thanks", response_class=HTMLResponse)
async def thanks_page():
    return """
    <html><body style="font-family:system-ui">
      <h1>✅ Оплату отримано</h1>
      <p>Дякуємо! Тепер можете повернутися в бот.</p>
    </body></html>
    """

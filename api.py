# /root/mybot/api.py
import os
import json
import time
import base64
import logging
import sqlite3
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse

# Перевірка підпису LiqPay
from payments.liqpay_utils import verify_signature

# Працюємо з тією ж БД, що й бот
from dao import add_credits  # має бути у dao.py
try:
    from dao import DB_PATH  # якщо є у твоєму dao.py
except Exception:
    # fallback — підстав свій шлях до БД, якщо в dao немає DB_PATH
    DB_PATH = "/root/mybot/bot.db"

logger = logging.getLogger("mybot-api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="MyBot Public API", version="1.2.0")

# За замовчуванням 1 кредит = 5 грн (як у боті на кнопках)
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# ---------- Локальний журнал платежів (idempotency) у тій самій БД ----------
DB_PATH = Path(DB_PATH)

def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _init_payments_table():
    with _db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                order_id   TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                amount     REAL NOT NULL,
                credits    INTEGER NOT NULL,
                status     TEXT NOT NULL,
                raw_json   TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
        """)
_init_payments_table()

def _amount_to_credits(amount: float) -> int:
    # логіка конвертації: 1 кредит за кожні CREDIT_PRICE_UAH грн
    try:
        a = float(amount)
    except Exception:
        a = 0.0
    if CREDIT_PRICE_UAH <= 0:
        return int(round(a))
    return int(a // CREDIT_PRICE_UAH) if a > 0 else 0

# --------------------------- Публічні ендпоінти -----------------------------

@app.get("/health")
async def health():
    return {"ok": True, "service": "mybot-api"}

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    return """
    <!doctype html>
    <html><head><meta charset="utf-8"><title>Дякуємо</title></head>
    <body style="font-family: system-ui; text-align:center; padding: 3rem;">
      <h1>Дякуємо за оплату! ✅</h1>
      <p>Оплату отримано. Можете повернутися до бота.</p>
    </body></html>
    """

@app.post("/liqpay/callback")
async def liqpay_callback(
    request: Request,
    data: Optional[str] = Form(None),
    signature: Optional[str] = Form(None),
):
    """
    Приймаємо callback від LiqPay:
      - валідація підпису
      - idempotency по order_id
      - нарахування кредитів користувачу (user_id передаємо в полі 'info')
    Повертаємо 200 OK завжди, якщо формат валідний (щоб LiqPay не ретраївся безкінечно).
    """

    # Підтримка JSON і form-urlencoded
    if data is None or signature is None:
        try:
            body = await request.json()
            data = data or body.get("data")
            signature = signature or body.get("signature")
        except Exception:
            pass

    if not data or not signature:
        logger.warning("Callback: missing data or signature")
        return PlainTextResponse("missing fields", status_code=400)

    # Перевірка підпису
    if not verify_signature(data, signature):
        logger.warning("Callback: bad signature")
        return PlainTextResponse("bad signature", status_code=403)

    # Розпакувати payload
    try:
        decoded = base64.b64decode(data).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        logger.exception("Callback: failed to decode payload")
        return PlainTextResponse("bad data", status_code=400)

    logger.info("LiqPay callback OK: %s", payload)

    order_id = str(payload.get("order_id") or "").strip()
    status   = str(payload.get("status") or "").strip()
    amount   = float(payload.get("amount") or 0.0)

    # user_id передаємо у полі "info" при створенні платежу
    user_info = str(payload.get("info") or "").strip()
    user_id = user_info

    # fallback: якщо info порожній, пробуємо витягнути user_id з order_id "userId-xxxx"
    if not user_id and "-" in order_id:
        user_id = order_id.split("-", 1)[0]

    if not order_id or not user_id:
        logger.warning("Callback: bad order/user: order_id=%s info=%s", order_id, user_info)
        return PlainTextResponse("bad order_id/info", status_code=400)

    # idempotency: не нараховуємо двічі той самий order_id
    with _db() as conn:
        existing = conn.execute("SELECT 1 FROM payments WHERE order_id = ?", (order_id,)).fetchone()
        if existing:
            return JSONResponse({"ok": True, "repeat": True})

        credits = _amount_to_credits(amount)
        created_at = int(time.time())

        # Записуємо платіж у журнал
        conn.execute(
            "INSERT INTO payments(order_id, user_id, amount, credits, status, raw_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (order_id, user_id, amount, credits, status, json.dumps(payload, ensure_ascii=False), created_at)
        )

    # Нараховуємо тільки на успішні стани (у sandbox теж зараховуємо)
    if status in ("success", "sandbox") and credits > 0:
        try:
            new_balance = add_credits(int(user_id), int(credits))
            logger.info("✅ Credited %s credits to user %s. New balance=%s", credits, user_id, new_balance)
        except Exception:
            logger.exception("Failed to credit user %s", user_id)

    # LiqPay очікує 200 OK
    return {"ok": True}

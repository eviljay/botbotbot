# payments_api.py
import os
import json
import uuid
import math
import base64
import logging
import sqlite3
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

# ===== Логи =====
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mybot-api")

# ===== ENV =====
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "UAH")

# LiqPay
LIQPAY_PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "").strip()
LIQPAY_PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "").strip()
LIQPAY_RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "").strip()        # сторінка "успіх"
LIQPAY_SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "").strip()        # callback URL

# База/бот
DB_PATH = os.getenv("DB_PATH", "/root/mybot/bot.db")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# ===== Імпорт утиліт LiqPay =====
from payments.liqpay_utils import (
    build_checkout_link,
    verify_callback_signature,
)

# ===== FastAPI =====
app = FastAPI(title="Payments API (LiqPay)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ===== Допоміжне =====
def _mk_order_id(user_id: int) -> str:
    return f"{user_id}-{uuid.uuid4().hex[:12]}"

def _ensure_users_table():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
              CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                phone TEXT
              )
            """)
            conn.commit()
    except Exception:
        log.exception("Failed to ensure users table")

def _credit_user(uid: int, amount_uah: float) -> int:
    """Нарахувати кредити за суму в UAH. Повертає нараховану кількість."""
    credits = max(1, math.ceil(float(amount_uah) / CREDIT_PRICE_UAH))
    _ensure_users_table()
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("UPDATE users SET balance = COALESCE(balance,0) + ? WHERE user_id = ?", (credits, uid))
        if cur.rowcount == 0:
            conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, ?)", (uid, credits))
        conn.commit()
    return credits

def _get_balance(uid: int) -> Optional[int]:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT balance FROM users WHERE user_id = ?", (uid,)).fetchone()
            return int(row[0]) if row else None
    except Exception:
        log.exception("Failed to read balance")
        return None

def _notify_user(uid: int, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not uid:
        return
    try:
        with httpx.Client(timeout=10) as c:
            c.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={"chat_id": uid, "text": text},
            )
    except Exception:
        log.exception("Failed to send Telegram message")

# ===== Health =====
@app.get("/ping")
def ping():
    return {"pong": True}

# ===== Створити платіж (LiqPay) =====
@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Очікує JSON:
    {
      "user_id": 12345,
      "amount": 100,
      "description": "Top-up …",   # опційно, згенеруємо самі
      "provider": "liqpay"         # ігноруємо інші – тут реалізовано LiqPay
    }
    """
    body = await req.json()
    user_id = int(body.get("user_id") or 0)
    amount = float(body.get("amount") or 0)
    if user_id <= 0 or amount <= 0:
        raise HTTPException(400, "user_id and amount are required")

    currency = body.get("currency") or DEFAULT_CURRENCY
    description = body.get("description") or f"Top-up {amount:.2f} by {user_id}"
    order_id = _mk_order_id(user_id)

    # Створюємо LiqPay checkout URL локально (без зовнішнього запиту)
    link = build_checkout_link(
        amount=amount,
        currency=currency,
        description=description,
        order_id=order_id,
        result_url=LIQPAY_RESULT_URL or None,
        server_url=LIQPAY_SERVER_URL or None,
        language="uk",
    )
    resp = {
        "ok": True,
        "provider": "liqpay",
        "order_id": order_id,
        "data": link["data"],
        "signature": link["signature"],
        "checkout_url": link["checkout_url"],
        "pay_url": link["checkout_url"],        # уніфіковане поле
        "invoiceUrl": link["checkout_url"],     # зворотна сумісність
        "public_key": LIQPAY_PUBLIC_KEY,
    }
    return JSONResponse(resp)

# ===== Колбек LiqPay: підтримуємо обидва URL для зручності =====
async def _liqpay_callback_core(data_b64: str = Form(""), signature: str = Form("")):
    # 1) валідація підпису
    if not data_b64 or not signature:
        return PlainTextResponse("bad request", status_code=400)
    if not verify_callback_signature(data_b64, signature):
        log.error("Invalid LiqPay signature")
        return PlainTextResponse("invalid signature", status_code=400)

    # 2) парсимо payload
    try:
        payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    except Exception:
        log.exception("Failed to decode LiqPay payload")
        return PlainTextResponse("bad payload", status_code=400)

    log.info("LiqPay callback OK: %r", payload)

    # 3) стани, які вважаємо успішними
    status = (payload.get("status") or "").lower()
    if status not in {"success", "sandbox", "subscribed"}:
        return PlainTextResponse("ignored", status_code=200)

    # 4) витягуємо user_id: спочатку з order_id префіксу, потім із description (fallback)
    uid: Optional[int] = None
    order_id = payload.get("order_id") or ""
    if "-" in order_id:
        prefix = order_id.split("-", 1)[0]
        if prefix.isdigit():
            uid = int(prefix)
    if uid is None:
        desc = payload.get("description", "")
        # очікуємо формат "Top-up <amount> by <uid>"
        import re
        m = re.search(r"\bby\s+(\d+)\b", desc)
        if m:
            uid = int(m.group(1))

    if not uid:
        log.error("Callback without valid user_id (info): %r", payload.get("description"))
        return PlainTextResponse("ok", status_code=200)

    # 5) скільки нарахувати
    amount = float(payload.get("amount", 0))
    credits = _credit_user(uid, amount)
    new_balance = _get_balance(uid)

    # 6) нотифікація юзеру
    msg = f"💳 Оплата успішна!\nНараховано: +{credits} кредит(и)\nСума: {amount:.2f} {payload.get('currency','UAH')}"
    if new_balance is not None:
        msg += f"\nПоточний баланс: {new_balance}"
    _notify_user(uid, msg)

    return PlainTextResponse("ok", status_code=200)

@app.post("/api/payments/liqpay/callback")
async def liqpay_callback_full(data: str = Form(""), signature: str = Form("")):
    return await _liqpay_callback_core(data, signature)

@app.post("/liqpay/callback")
async def liqpay_callback_short(data: str = Form(""), signature: str = Form("")):
    # залишено для сумісності з існуючою конфігурацією
    return await _liqpay_callback_core(data, signature)

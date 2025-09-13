# payments_api.py
import os
import sys
import json
import uuid
import math
import base64
import logging
import sqlite3
import inspect
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx

# ===== Логи =====
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mybot-api")

API_BUILD = "liqpay-v2.2"  # <- МАРКЕР ВЕРСІЇ

# ===== ENV =====
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "UAH")

# LiqPay
LIQPAY_PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "").strip()
LIQPAY_PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "").strip()
LIQPAY_RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "").strip()        # сторінка "успіх"
LIQPAY_SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "").strip()        # callback URL

# База/бот
DB_PATH = os.getenv("DB_PATH", "/root/mybot/bot.db")  # ВАЖЛИВО: абсолютний шлях
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# ===== Утиліти LiqPay =====
from payments.liqpay_utils import build_checkout_link, verify_callback_signature

# ===== FastAPI =====
app = FastAPI(title="Payments API (LiqPay)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ===== Допоміжне =====
def _mk_order_id(user_id: int) -> str:
    # Префікс з user_id для надійного зв’язування платежу і юзера
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

# ===== Health / Debug =====
@app.get("/ping")
def ping():
    return {"pong": True}

@app.get("/env-check")
def env_check():
    return {
        "API_BUILD": API_BUILD,
        "DB_PATH": DB_PATH,
        "CREDIT_PRICE_UAH": CREDIT_PRICE_UAH,
        "PAYMENTS_API_FILE": inspect.getsourcefile(sys.modules[__name__]),
    }

# ===== Створити платіж (LiqPay) =====
@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Очікує JSON:
    {
      "user_id": 12345,
      "amount": 100,
      "description": "Top-up …",   # опційно
      "provider": "liqpay" | "wayforpay"   # тут реалізовано liqpay; інше -> 400
    }
    """
    body = await req.json()
    provider = (body.get("provider") or "liqpay").lower()
    if provider != "liqpay":
        raise HTTPException(400, "Only provider=liqpay supported by this API instance")

    user_id = int(body.get("user_id") or 0)
    amount = float(body.get("amount") or 0)
    if user_id <= 0 or amount <= 0:
        raise HTTPException(400, "user_id and amount are required")

    currency = body.get("currency") or DEFAULT_CURRENCY
    description = body.get("description") or f"Top-up {amount:.2f} by {user_id}"
    order_id = _mk_order_id(user_id)

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
        "pay_url": link["checkout_url"],
        "invoiceUrl": link["checkout_url"],
        "public_key": LIQPAY_PUBLIC_KEY,
    }
    return JSONResponse(resp)

# ===== Колбек LiqPay (обидві адреси ведуть сюди) =====
async def _liqpay_callback_core(data: str = Form(""), signature: str = Form("")):
    # 1) підпис
    if not data or not signature:
        return PlainTextResponse("bad request", status_code=400)
    if not verify_callback_signature(data, signature):
        log.error("Invalid LiqPay signature")
        return PlainTextResponse("invalid signature", status_code=400)

    # 2) payload
    try:
        payload = json.loads(base64.b64decode(data).decode("utf-8"))
    except Exception:
        log.exception("Failed to decode LiqPay payload")
        return PlainTextResponse("bad payload", status_code=400)

    log.info("LiqPay callback OK: %r", payload)

    status = (payload.get("status") or "").lower()
    if status not in {"success", "sandbox", "subscribed"}:
        return PlainTextResponse("ignored", status_code=200)

    # 3) uid: 1) info  2) order_id "<uid>-..."  3) desc "... by <uid>"
    import re
    uid = None

    info = (payload.get("info") or "").strip()
    if info.isdigit():
        uid = int(info)

    order_id = (payload.get("order_id") or "").strip()
    if uid is None and "-" in order_id:
        pref = order_id.split("-", 1)[0]
        if pref.isdigit():
            uid = int(pref)

    desc = (payload.get("description") or "").strip()
    if uid is None:
        m = re.search(r"\bby\s+(\d+)\b", desc)
        if m:
            uid = int(m.group(1))

    log.info("LiqPay parsed uid=%r | info=%r | order_id=%r | desc=%r", uid, info, order_id, desc)

    if not uid:
        log.error("Callback without valid user_id (info): %r | order_id=%r | info=%r", desc, order_id, info)
        return PlainTextResponse("ok", status_code=200)

    # 4) нарахування
    try:
        amount = float(payload.get("amount", 0))
    except Exception:
        amount = 0.0
    credits = _credit_user(uid, amount)
    new_balance = _get_balance(uid)

    # 5) нотиф юзеру
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
    return await _liqpay_callback_core(data, signature)

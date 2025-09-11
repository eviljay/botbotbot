# /root/mybot/api.py
import os
import json
import base64
import logging
import sqlite3
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
import httpx

# LiqPay utils + ціноутворення
from payments.liqpay_utils import build_data, sign, verify_signature, PUBLIC_KEY
from payments.pricing import calc_credits_from_amount

# -----------------------------------------------------------------------------
# Конфіг / ENV
# -----------------------------------------------------------------------------
def _parse_float_env(name: str, default: float) -> float:
    """Дозволяє значення типу '5' або '5 # коментар' у .env"""
    raw = os.getenv(name, str(default))
    raw = raw.split()[0]  # беремо перший токен до пробілу/коментаря
    try:
        return float(raw)
    except ValueError:
        return default

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("mybot-api")

DB_PATH = os.getenv("DB_PATH", "/root/mybot/bot.db")

# Куди LiqPay шле POST після оплати
SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "").rstrip("/")
# Куди повертається користувач після оплати (проста сторінка «дякуємо»)
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "").rstrip("/")

# Телеграм для пуш-повідомлення юзеру
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Ціна 1 кредиту, якщо знадобиться перерахунок (наразі беремо з pricing.calc_credits_from_amount)
CREDIT_PRICE_UAH = _parse_float_env("CREDIT_PRICE_UAH", 5.0)

# Увімкнена пісочниця (1) чи ні (0)
LIQPAY_SANDBOX = int(os.getenv("LIQPAY_SANDBOX", "1"))

# -----------------------------------------------------------------------------
# Ініціалізація БД (створимо таблиці, якщо їх немає)
# -----------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  phone TEXT,
  balance INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id TEXT UNIQUE,
  user_id INTEGER,
  amount REAL,
  credits INTEGER,
  status TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        # sqlite не підтримує execute на кількох statement-ах одним викликом,
        # тому розіб'ємо по ';'
        for stmt in SCHEMA_SQL.strip().split(";\n"):
            s = stmt.strip()
            if s:
                conn.execute(s)
        conn.commit()

init_db()

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="MyBot Public API (LiqPay)")

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    return """
<!doctype html>
<html><head><meta charset="utf-8"><title>Дякуємо</title></head>
<body style="font-family:system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding:24px;">
  <h2>Дякуємо за оплату! ✅</h2>
  <p>Оплату отримано. Можете повернутися до бота.</p>
</body></html>
"""

# -----------------------------------------------------------------------------
# Створення інвойсу (бот викликає це і отримує checkout_url)
# -----------------------------------------------------------------------------
@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Приймає JSON:
      {
        "user_id": <int>,       # обовʼязково
        "amount_uah": <number>, # сума у гривнях
        "description": <str>,   # не обовʼязково
        "order_id": <str>       # не обовʼязково (згенеруємо якщо нема)
      }
    Повертає:
      {
        "order_id": "...",
        "public_key": "...",
        "data": "...",
        "signature": "...",
        "checkout_url": "https://www.liqpay.ua/api/3/checkout?data=...&signature=..."
      }
    """
    body = await req.json()
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(status_code=422, detail="user_id is required")

    amount = float(body.get("amount_uah", 0))
    if amount <= 0:
        raise HTTPException(status_code=422, detail="amount_uah must be > 0")

    description = body.get("description") or f"{int(amount)} UAH topup"
    order_id = body.get("order_id") or f"{user_id}-{os.urandom(6).hex()}"

    params = {
        "version": 3,
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": amount,
        "currency": "UAH",
        "description": description,
        "order_id": order_id,
        "result_url": f"{RESULT_URL}/thanks" if RESULT_URL else "",
        "server_url": f"{SERVER_URL}/liqpay/callback" if SERVER_URL else "",
        "sandbox": LIQPAY_SANDBOX,
        # передамо user_id, щоб у callback нарахувати йому кредити
        "info": str(user_id),
    }

    data_b64 = build_data(params)
    signature = sign(data_b64)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    # Опціонально: зафіксуємо «ініційований платіж» (необовʼязково)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO payments (order_id, user_id, amount, credits, status) VALUES (?, ?, ?, ?, ?)",
                (order_id, int(user_id), float(amount), 0, "created"),
            )
            conn.commit()
    except Exception as e:
        log.warning("Failed to pre-insert payment row: %s", e)

    return JSONResponse({
        "order_id": order_id,
        "public_key": PUBLIC_KEY,
        "data": data_b64,
        "signature": signature,
        "checkout_url": checkout_url,
    })

# -----------------------------------------------------------------------------
# Callback від LiqPay (сюди прийде POST з form-data: data, signature)
# -----------------------------------------------------------------------------
@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    form = await req.form()
    data_b64: Optional[str] = form.get("data")
    signature: Optional[str] = form.get("signature")

    if not data_b64 or not signature:
        raise HTTPException(status_code=400, detail="Missing data/signature")

    if not verify_signature(data_b64, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    log.info("LiqPay callback OK: %s", payload)

    # Ключові поля
    status = payload.get("status")             # success | sandbox | failure | error | ...
    order_id = payload.get("order_id")
    amount = float(payload.get("amount", 0))
    info_user_id = payload.get("info")         # ми клали туди Telegram user_id у create_payment

    if not order_id:
        raise HTTPException(status_code=400, detail="No order_id in payload")

    # В sandbox режимі LiqPay шле status='sandbox'; прирівняємо до success
    is_success = status in ("success", "sandbox")

    # Розрахунок кредитів (власна логіка в payments.pricing)
    credits = calc_credits_from_amount(amount) if is_success else 0

    # Запишемо у БД + нарахуємо баланс
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # якщо інсерт був раніше — оновимо статус/amount/credits
            conn.execute(
                """
                INSERT INTO payments (order_id, user_id, amount, credits, status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                  user_id=excluded.user_id,
                  amount=excluded.amount,
                  credits=excluded.credits,
                  status=excluded.status
                """,
                (order_id, int(info_user_id) if info_user_id else None, amount, credits, status),
            )

            new_balance = None
            if is_success and info_user_id:
                # аналог users.balance += credits
                conn.execute(
                    "UPDATE users SET balance = COALESCE(balance,0) + ? WHERE user_id = ?",
                    (int(credits), int(info_user_id)),
                )
                row = conn.execute(
                    "SELECT balance FROM users WHERE user_id = ?",
                    (int(info_user_id),)
                ).fetchone()
                new_balance = row[0] if row else 0

            conn.commit()
    except sqlite3.Error as e:
        log.exception("DB error on callback: %s", e)
        raise HTTPException(status_code=500, detail="DB error")

    # Якщо оплата пройшла — надішлемо пуш у Telegram юзеру
    if is_success and info_user_id and TELEGRAM_TOKEN:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={
                        "chat_id": int(info_user_id),
                        "text": (
                            f"✅ Оплату отримано!\n"
                            f"Зараховано: +{credits} кредитів.\n"
                            f"Новий баланс: {new_balance} кредитів."
                        ),
                        "disable_web_page_preview": True,
                    },
                )
        except Exception as e:
            log.warning("Failed to push Telegram message: %s", e)

    return JSONResponse({"ok": True})
import os
import json
import base64
import hmac
import hashlib
import sqlite3
import logging
import httpx

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from dotenv import load_dotenv

# === CONFIG ===
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mybot-api")

DB_PATH = os.getenv("DB_PATH", "bot.db")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
LIQPAY_PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "")
LIQPAY_PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")
SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# === DB INIT ===
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
  raw_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
with sqlite3.connect(DB_PATH) as conn:
    conn.executescript(SCHEMA_SQL)

# === HELPERS ===
def sign(data_b64: str) -> str:
    s = LIQPAY_PRIVATE_KEY + data_b64 + LIQPAY_PRIVATE_KEY
    return base64.b64encode(hashlib.sha1(s.encode("utf-8")).digest()).decode("utf-8")

def verify_signature(data_b64: str, signature: str) -> bool:
    return sign(data_b64) == signature

def calc_credits_from_amount(amount: float) -> int:
    return int(amount // CREDIT_PRICE_UAH)

def db():
    return sqlite3.connect(DB_PATH)

async def notify_user(user_id: int, credits: int, balance: int):
    """Надсилає повідомлення користувачу у Telegram про поповнення"""
    if not BOT_TOKEN:
        log.warning("BOT_TOKEN not set, cannot notify user")
        return
    text = f"✅ Оплату отримано! Нараховано {credits} кредитів.\nВаш новий баланс: {balance}"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": user_id, "text": text}
            )
    except Exception as e:
        log.error("Failed to notify user: %s", e)

# === APP ===
app = FastAPI()

@app.get("/", response_class=RedirectResponse)
async def index():
    """Автоматичний редірект на Telegram-бота"""
    return RedirectResponse(f"https://t.me/{os.getenv('TELEGRAM_BOT_NAME', 'my_seo_bot')}")

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    return HTMLResponse(
        "<html><body><h2>Дякуємо за оплату! ✅</h2>"
        "<p>Оплату отримано. Можете повернутися до бота.</p>"
        "<script>setTimeout(() => { window.location.href='https://t.me/"
        + os.getenv("TELEGRAM_BOT_NAME", "my_seo_bot")
        + "'; }, 3000);</script></body></html>"
    )

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    form = await req.form()
    data_b64 = form.get("data")
    signature = form.get("signature")

    if not data_b64 or not signature:
        raise HTTPException(status_code=400, detail="Missing data/signature")

    if not verify_signature(data_b64, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    log.info("LiqPay callback OK: %s", payload)

    order_id = payload.get("order_id")
    amount = float(payload.get("amount", 0))
    status = payload.get("status")
    user_id = payload.get("info") or None
    credits = calc_credits_from_amount(amount)
    raw_json = json.dumps(payload, ensure_ascii=False)

    if not order_id:
        raise HTTPException(status_code=400, detail="Missing order_id")

    try:
        with db() as conn:
            # UPSERT запис у таблицю payments
            conn.execute(
                """
                INSERT INTO payments (order_id, user_id, amount, credits, status, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                  user_id=excluded.user_id,
                  amount=excluded.amount,
                  credits=excluded.credits,
                  status=excluded.status,
                  raw_json=excluded.raw_json
                """,
                (order_id, int(user_id) if user_id else None, amount, credits, status, raw_json),
            )

            if status in ("success", "sandbox") and user_id:
                conn.execute(
                    "INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)",
                    (int(user_id),)
                )
                conn.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (credits, int(user_id))
                )
                balance = conn.execute(
                    "SELECT balance FROM users WHERE user_id = ?",
                    (int(user_id),)
                ).fetchone()[0]

                # Сповіщення у бот
                await notify_user(int(user_id), credits, balance)

    except Exception as e:
        log.exception("DB error on callback")
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return JSONResponse({"ok": True})

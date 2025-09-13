 # /root/mybot/api.py
import os
import json
import sqlite3
import base64
import hmac
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
import httpx
from dotenv import load_dotenv

load_dotenv()
import logging
 log.info("DB_PATH=%s", os.getenv("DB_PATH"))

logger = logging.getLogger("mybot-api")
logging.basicConfig(level=logging.INFO)
# === ENV ==DB_PATH = os.getenv("DB_PATH", "mybot.db")=
DB_PATH = os.getenv("DB_PATH", "/root/mybot/bot.db")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
LIQPAY_PUBLIC_KEY = os.environ["LIQPAY_PUBLIC_KEY"]
LIQPAY_PRIVATE_KEY = os.environ["LIQPAY_PRIVATE_KEY"]
THANKS_REDIRECT_TELEGRAM = os.getenv("THANKS_REDIRECT_TELEGRAM", "https://t.me/SeoSwissKnife_bot")
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))  # 1 кредит = CREDIT_PRICE_UAH грн

# === APP ===
app = FastAPI(title="MyBot Public API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# === DB INIT ===
def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL UNIQUE,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            credits INTEGER NOT NULL,
            status TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0
        )
        """)
        conn.commit()

init_db()
async def tg_send_message(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(url, json={"chat_id": chat_id, "text": text})
            # Якщо бот ніколи не бачив юзера -> 400 chat not found
            if r.status_code >= 400:
                logger.warning("Telegram sendMessage failed: %s | body=%s", r.status_code, r.text)
    except Exception as e:
        logger.warning("Telegram sendMessage exception: %s", e)
# === HELPERS ===
def liqpay_sign(data_b64: str) -> str:
    # signature = base64( sha1( private_key + data + private_key ) )
    raw = f"{LIQPAY_PRIVATE_KEY}{data_b64}{LIQPAY_PRIVATE_KEY}".encode("utf-8")
    sha1 = hashlib.sha1(raw).digest()
    return base64.b64encode(sha1).decode("utf-8")

def verify_liqpay_signature(data_b64: str, signature: str) -> bool:
    return liqpay_sign(data_b64) == signature

def calc_credits(amount_uah: float) -> int:
    # Напр.: 100 грн / 5 грн = 20 кредитів
    return int(amount_uah // CREDIT_PRICE_UAH)

async def tg_send_message(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={"chat_id": chat_id, "text": text})

# === ROUTES ===
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

THANKS_HTML = f"""<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <title>Дякуємо за оплату</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="refresh" content="1;url={THANKS_REDIRECT_TELEGRAM}">
  <style>
    body{{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Ubuntu;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#0b1220;color:#e6e6e6}}
    .card{{max-width:520px;background:#10192b;border:1px solid #1f2a44;border-radius:14px;padding:24px;box-shadow:0 10px 30px rgba(0,0,0,.35)}}
    h1{{margin:0 0 8px;font-size:22px}}
    p{{margin:0;color:#b8c2d6;line-height:1.5}}
    .ok{{display:inline-block;margin-bottom:12px;font-weight:700;color:#41d19a}}
    .small{{margin-top:12px;color:#8390a8;font-size:13px}}
    a{{color:#79b8ff}}
  </style>
</head>
<body>
  <div class="card">
    <div class="ok">✅ Оплату отримано</div>
    <h1>Дякуємо за оплату!</h1>
    <p>Можете повернутися до бота. Зараз вас автоматично перенаправить…</p>
    <p class="small">Якщо редірект не спрацював — відкрийте бота вручну: <a href="{THANKS_REDIRECT_TELEGRAM}">{THANKS_REDIRECT_TELEGRAM}</a></p>
  </div>
  <script>setTimeout(()=>location.href="{THANKS_REDIRECT_TELEGRAM}",1500)</script>
</body>
</html>
"""

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    return HTMLResponse(THANKS_HTML)

@app.post("/liqpay/callback")
async def liqpay_callback(data: str = Form(None), signature: str = Form(None), request: Request = None):
    """
    LiqPay server_url callback.
    Чекаємо form-data з полями 'data' та 'signature'.
    """
    if not data or not signature:
        # деякі процесори присилають form пустим — надамо дружнє пояснення
        raise HTTPException(status_code=400, detail="Missing data/signature")

    if not verify_liqpay_signature(data, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        payload = json.loads(base64.b64decode(data).decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Bad data payload")

    # Лог для дебага
    logger.info("LiqPay callback OK: %s", payload)

    # Витягуємо потрібне
    status = payload.get("status")
    order_id = payload.get("order_id")
    amount = float(payload.get("amount") or 0)
    # user_id ми прокидали в create через поле "info"
    user_id_raw = payload.get("info") or payload.get("description") or ""
    try:
        user_id = int(str(user_id_raw).split("-")[0])
    except Exception:
        user_id = None

    if not order_id:
        raise HTTPException(status_code=400, detail="Missing order_id in payload")
    if user_id is None:
        # не зламаємось: зафіксуємо як 0, але краще перевірити логіку створення інвойсу
        logger.error("Callback without valid user_id (info): %r", user_id_raw)
        user_id = 0

    credits = calc_credits(amount)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Пишемо у БД
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL UNIQUE,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    credits INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    balance INTEGER NOT NULL DEFAULT 0
                )
            """)
            # вставка платежу (явно ставимо created_at, аби не залежати від DEFAULT)
            conn.execute(
                """INSERT INTO payments (order_id, user_id, amount, credits, status, raw_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (order_id, int(user_id), amount, credits, status, json.dumps(payload), now_iso)
            )

            # Нарахування балансу тільки для успішних/пісочниця
            if status in ("success", "sandbox"):
                # upsert користувача
                conn.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)", (int(user_id),))
                conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (int(credits), int(user_id)))

            conn.commit()
    except sqlite3.IntegrityError as e:
        logger.error("DB error on callback (integrity): %s", e)
        # 409 краще під дублікати ордерів
        raise HTTPException(status_code=409, detail="Payment already recorded or DB integrity error")
    except Exception as e:
        logger.error("DB error on callback: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="DB error")

    # Відправляємо повідомлення у бот (якщо user_id валідний)
    try:
        if int(user_id) > 0 and status in ("success", "sandbox"):
            # зчитаємо новий баланс
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute("SELECT balance FROM users WHERE user_id = ?", (int(user_id),)).fetchone()
                new_balance = row[0] if row else 0
            await tg_send_message(
                int(user_id),
                f"💳 Оплату отримано!\n"
                f"+{credits} кредитів (сума {int(amount)}₴)\n"
                f"Новий баланс: {new_balance} кредитів."
            )
    except Exception as e:
        # не валимо відповідь LiqPay, просто логуємо
        logger.error("Failed to notify user in Telegram: %s", e)

    return JSONResponse({"ok": True})

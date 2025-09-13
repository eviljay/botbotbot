# /root/mybot/api.py
import os
import json
import sqlite3
import base64
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import httpx

load_dotenv()

logger = logging.getLogger("mybot-api")
logging.basicConfig(level=logging.INFO)

# ---------- ENV ----------
DB_PATH = os.getenv("DB_PATH", "/root/mybot/data/bot.db")

# підтримуємо нові й старі назви змінних
_LP_PRIV = os.getenv("LIQPAY_PRIVATE_KEY") or os.getenv("LIQPAY_PRIVATE") or ""
_LP_PUB  = os.getenv("LIQPAY_PUBLIC_KEY") or os.getenv("LIQPAY_PUBLIC") or ""
LIQPAY_PRIVATE_KEY = _LP_PRIV.strip()
LIQPAY_PUBLIC_KEY = _LP_PUB.strip()

# можливість вимкнути підпис для тесту продукції
LIQPAY_SKIP_SIGNATURE = os.getenv("LIQPAY_SKIP_SIGNATURE", "0") == "1"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
THANKS_REDIRECT_TELEGRAM = os.getenv("THANKS_REDIRECT_TELEGRAM", "https://t.me/SeoSwissKnife_bot")
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# невеликий лог для діагностики (без секретів)
logger.info("API up | DB=%s | SKIP_SIG=%s | PUB_KEY_SET=%s | PRIV_LEN=%s",
            DB_PATH, LIQPAY_SKIP_SIGNATURE, bool(LIQPAY_PUBLIC_KEY), len(LIQPAY_PRIVATE_KEY))

# ---------- APP ----------
app = FastAPI(title="MyBot Public API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ---------- DB INIT ----------
def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0
        )
        """)
        # не форсуємо свою схему payments — нижче працюємо обережними UPDATE/INSERT

init_db()

# ---------- HELPERS ----------
def liqpay_sign(data_b64: str) -> str:
    digest = hashlib.sha1((LIQPAY_PRIVATE_KEY + data_b64 + LIQPAY_PRIVATE_KEY).encode("utf-8")).digest()
    return base64.b64encode(digest).decode("utf-8")

def verify_liqpay_signature(data_b64: str, signature: str) -> bool:
    if LIQPAY_SKIP_SIGNATURE:
        logger.warning("Signature check DISABLED (LIQPAY_SKIP_SIGNATURE=1)")
        return True
    return liqpay_sign(data_b64) == signature

def calc_credits(amount_uah: float) -> int:
    return int(float(amount_uah) // CREDIT_PRICE_UAH)

async def tg_send_message(chat_id: int, text: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(url, json={"chat_id": chat_id, "text": text})
    except Exception as e:
        logger.warning("Telegram sendMessage exception: %s", e)

# ---------- ROUTES ----------
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

THANKS_HTML = f"""<!doctype html>
<html lang="uk">
<head><meta charset="utf-8"><title>Дякуємо за оплату</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<meta http-equiv="refresh" content="1;url={THANKS_REDIRECT_TELEGRAM}">
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto;display:flex;align-items:center;justify-content:center;height:100vh;background:#f8fafc}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:24px;box-shadow:0 6px 18px rgba(0,0,0,.06)}}
.small{{color:#6b7280;font-size:12px}}
</style></head>
<body><div class="card">
<h2>Дякуємо! Оплату отримано</h2>
<p>Можете повернутися в Telegram 👇</p>
<p class="small"><a href="{THANKS_REDIRECT_TELEGRAM}">{THANKS_REDIRECT_TELEGRAM}</a></p>
</div></body></html>
"""

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    return HTMLResponse(THANKS_HTML)

@app.post("/liqpay/callback")
async def liqpay_callback(data: str = Form(None), signature: str = Form(None), request: Request = None):
    if not data or not signature:
        raise HTTPException(status_code=400, detail="Missing data/signature")

    if not verify_liqpay_signature(data, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    # розбираємо payload
    try:
        payload = json.loads(base64.b64decode(data).decode("utf-8"))
    except Exception:
        logger.exception("Bad payload base64/json")
        raise HTTPException(status_code=400, detail="Bad payload")

    status = payload.get("status")
    order_id = payload.get("order_id")
    amount = float(payload.get("amount", 0))
    currency = payload.get("currency", "UAH")
    user_id = payload.get("sender_phone", 0)  # якщо передавався кастомно — у тебе зберігається в payments; тут не критично

    logger.info("Callback: order_id=%s status=%s amount=%s %s", order_id, status, amount, currency)

    if not order_id:
        raise HTTPException(status_code=400, detail="No order_id")

    if status not in ("success", "sandbox", "subscribed"):
        # нехай 200, щоб LiqPay більше не ретраїв
        return JSONResponse({"ok": True})

    credits = calc_credits(amount)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(DB_PATH) as conn:
        # 1) Спробуємо оновити "розширену" схему (з order_reference/raw_json/updated_at)
        try:
            conn.execute("""
                UPDATE payments
                SET status='success', raw_json=?, updated_at=datetime('now')
                WHERE order_id=? OR order_reference=?
            """, (json.dumps(payload, ensure_ascii=False), order_id, order_id))
        except sqlite3.OperationalError:
            # 2) fallback: мінімальна схема (order_id, raw_json як TEXT, без updated_at)
            try:
                conn.execute("""
                    UPDATE payments
                    SET status='success', raw_json=?
                    WHERE order_id=?
                """, (json.dumps(payload, ensure_ascii=False), order_id))
            except sqlite3.OperationalError:
                # 3) як крайній випадок — вставимо запис
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
                    INSERT OR IGNORE INTO payments(order_id,user_id,amount,credits,status,raw_json)
                    VALUES(?,?,?,?,?,?)
                """, (order_id, int(user_id) or 0, amount, credits, 'success', json.dumps(payload, ensure_ascii=False)))

        # Нарахуємо кредити
        conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, 0)", (int(user_id) or 0,))
        conn.execute("UPDATE users SET balance = COALESCE(balance,0) + ? WHERE user_id = ?", (credits, int(user_id) or 0))
        new_balance = conn.execute("SELECT balance FROM users WHERE user_id=?", (int(user_id) or 0,)).fetchone()[0]

    logger.info("Credited: user_id=%s +%s -> balance=%s", user_id, credits, new_balance)

    # TG повідомлення, якщо знаємо user_id
    if int(user_id) != 0:
        await tg_send_message(int(user_id), f"💳 Оплату отримано!\n+{credits} кредитів (сума {int(amount)}₴)\nБаланс: {new_balance}")

    return JSONResponse({"ok": True})

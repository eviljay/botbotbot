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

from payments.liqpay_utils import build_data, sign, verify_signature, PUBLIC_KEY
from payments.pricing import calc_credits_from_amount

# ---------- ENV / CONFIG ----------
def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    raw = raw.split()[0]
    try:
        return float(raw)
    except ValueError:
        return default

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("mybot-api")

DB_PATH = os.getenv("DB_PATH", "/root/mybot/bot.db")
SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "").rstrip("/")
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "").rstrip("/")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_BOT_URL = os.getenv("TELEGRAM_BOT_URL", "https://t.me/SeoSwissKnife_bot")  # напр. https://t.me/YourBotName
CREDIT_PRICE_UAH = _parse_float_env("CREDIT_PRICE_UAH", 5.0)
LIQPAY_SANDBOX = int(os.getenv("LIQPAY_SANDBOX", "1"))

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
        for stmt in SCHEMA_SQL.strip().split(";\n"):
            s = stmt.strip()
            if s:
                conn.execute(s)
        conn.commit()

init_db()

app = FastAPI(title="MyBot Public API (LiqPay)")

# ---------- helpers ----------
def _resolve_user_id(order_id: Optional[str], info_user_id: Optional[str]) -> Optional[int]:
    """Повертає int user_id: спочатку з info, інакше з префікса order_id '<uid>-xxxx'."""
    if info_user_id:
        try:
            return int(str(info_user_id).strip())
        except Exception:
            pass
    if order_id and "-" in order_id:
        first = order_id.split("-", 1)[0]
        try:
            return int(first)
        except Exception:
            pass
    return None

# ---------- routes ----------
@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    # авто-редірект на бота (2 сек). Якщо TELEGRAM_BOT_URL порожній — просто показуємо сторінку.
    redirect = TELEGRAM_BOT_URL or ""
    meta = f'<meta http-equiv="refresh" content="2;url={redirect}">' if redirect else ""
    link = f'<p><a href="{redirect}">Повернутися до бота</a></p>' if redirect else ""
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Дякуємо</title>{meta}</head>
<body style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;padding:24px;">
  <h2>Дякуємо за оплату! ✅</h2>
  <p>Оплату отримано. Можете повернутися до бота.</p>
  {link}
</body></html>"""

@app.post("/api/payments/create")
async def create_payment(req: Request):
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
        "info": str(user_id),  # важливо для callback
    }

    data_b64 = build_data(params)
    signature = sign(data_b64)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    # прелоґ платіж (може допомогти зв'язати callback навіть без info)
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

    status = payload.get("status")
    order_id = payload.get("order_id")
    amount = float(payload.get("amount", 0))
    info_user_id = payload.get("info")

    if not order_id:
        raise HTTPException(status_code=400, detail="No order_id in payload")

    # resolve user_id (info -> order_id prefix); fallback 0, щоб не падати на NOT NULL схемі
    resolved_uid = _resolve_user_id(order_id, info_user_id)
    uid_for_db = int(resolved_uid) if resolved_uid is not None else 0

    is_success = status in ("success", "sandbox")
    credits = calc_credits_from_amount(amount) if is_success else 0

    new_balance = None
    try:
        with sqlite3.connect(DB_PATH) as conn:
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
                (order_id, uid_for_db, amount, credits, status),
            )

            if is_success and resolved_uid is not None and resolved_uid > 0:
                conn.execute(
                    "UPDATE users SET balance = COALESCE(balance,0) + ? WHERE user_id = ?",
                    (int(credits), int(resolved_uid)),
                )
                row = conn.execute(
                    "SELECT balance FROM users WHERE user_id = ?",
                    (int(resolved_uid),)
                ).fetchone()
                new_balance = row[0] if row else None

            conn.commit()
    except sqlite3.Error as e:
        log.exception("DB error on callback: %s", e)
        raise HTTPException(status_code=500, detail="DB error")

    # пуш у Telegram лише якщо знаємо реальний user_id і є токен
    if is_success and resolved_uid is not None and resolved_uid > 0 and TELEGRAM_TOKEN:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={
                        "chat_id": int(resolved_uid),
                        "text": (
                            f"✅ Оплату отримано!\n"
                            f"Зараховано: +{credits} кредитів.\n"
                            f"Новий баланс: {new_balance if new_balance is not None else 'оновлено'} кредитів."
                        ),
                        "disable_web_page_preview": True,
                    },
                )
        except Exception as e:
            log.warning("Failed to push Telegram message: %s", e)

    return JSONResponse({"ok": True})

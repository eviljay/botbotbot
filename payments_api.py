# payments_api.py
import os
import json
import base64
import hashlib
import logging
import sqlite3
from uuid import uuid4
from typing import Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse
from dotenv import load_dotenv
import httpx

load_dotenv()

# ---------- CONFIG ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("payments-api")

DB_PATH = os.getenv("DB_PATH", "/root/mybot/data/bot.db")
PRICE_PER_CREDIT = int(os.getenv("PRICE_PER_CREDIT", "5"))

# Telegram
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# LiqPay
LIQPAY_PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "")
LIQPAY_PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")
LIQPAY_SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")   # https://<domain>/liqpay/callback
LIQPAY_RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")   # https://<domain>/thanks
LIQPAY_SKIP_SIGNATURE = os.getenv("LIQPAY_SKIP_SIGNATURE", "0") == "1"

# Публічна базова адреса твого бекенду для формування pay_url (заміни на свій домен у .env)
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8001")

app = FastAPI(title="Payments API (LiqPay)")

# ---------- DB ----------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db() -> None:
    with get_conn() as conn:
        # users
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            phone TEXT,
            balance INTEGER NOT NULL DEFAULT 0
        );
        """)

        # payments (канонічна структура з урахуванням твоєї поточної)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            order_reference TEXT NOT NULL UNIQUE,
            amount INTEGER NOT NULL,
            currency TEXT NOT NULL,
            status TEXT NOT NULL,
            raw_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            order_id TEXT
        );
        """)

        # індекси
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_order_reference ON payments(order_reference);")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);")

        # легкі міграції/узгодження на випадок старих таблиць
        cols = {r[1] for r in conn.execute("PRAGMA table_info(payments);").fetchall()}

        if "provider" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN provider TEXT;")
            conn.execute("UPDATE payments SET provider='liqpay' WHERE provider IS NULL;")

        if "order_reference" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN order_reference TEXT;")
            conn.execute("""
                UPDATE payments
                SET order_reference = COALESCE(order_id, printf('legacy-%s', hex(randomblob(6))))
                WHERE order_reference IS NULL;
            """)

        if "order_id" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN order_id TEXT;")
            conn.execute("UPDATE payments SET order_id=order_reference WHERE order_id IS NULL;")

        if "currency" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN currency TEXT DEFAULT 'UAH';")

        if "status" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN status TEXT DEFAULT 'pending';")

    log.info(f"DB ready. DB_PATH={DB_PATH}")

def ensure_user(conn: sqlite3.Connection, user_id: int) -> None:
    conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, 0)", (user_id,))

# ---------- LiqPay helpers ----------
def lp_sign(data_b64: str) -> str:
    # base64( sha1( private_key + data + private_key ) )
    digest = hashlib.sha1((LIQPAY_PRIVATE_KEY + data_b64 + LIQPAY_PRIVATE_KEY).encode("utf-8")).digest()
    return base64.b64encode(digest).decode("utf-8")

def lp_build_data(order_id: str, amount: int, currency: str, description: str) -> Tuple[str, str]:
    payload = {
        "public_key": LIQPAY_PUBLIC_KEY,
        "version": 3,
        "action": "pay",
        "amount": amount,
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "server_url": LIQPAY_SERVER_URL,
        "result_url": LIQPAY_RESULT_URL,
    }
    data_b64 = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode("utf-8")
    signature = lp_sign(data_b64)
    return data_b64, signature

# ---------- Telegram ----------
async def tg_notify(user_id: int, text: str) -> None:
    if not BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN empty; skip notify")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, json={"chat_id": user_id, "text": text})
        try:
            r.raise_for_status()
            log.info("TG notify OK user_id=%s", user_id)
        except Exception:
            log.exception("TG notify failed: %s", r.text)

# ---------- Utils ----------
def credits_from_amount(amount_uah: int) -> int:
    return max(0, int(amount_uah) // max(1, PRICE_PER_CREDIT))

# ---------- Routes ----------
@app.on_event("startup")
def _startup():
    log.info("Starting Payments API...")
    log.info(f"DB_PATH={DB_PATH}, PRICE_PER_CREDIT={PRICE_PER_CREDIT}")
    init_db()

@app.get("/health")
def health():
    return {"ok": True, "db": DB_PATH, "price_per_credit": PRICE_PER_CREDIT}

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "currency": "UAH",           # optional
        "description": "Top-up 100"  # optional
      }
    """
    body = await req.json()
    user_id = int(body["user_id"])
    amount = int(body["amount"])
    currency = body.get("currency", "UAH")
    description = body.get("description", f"Top-up {amount} {currency}")
    provider = "liqpay"

    order_id = uuid4().hex[:12]
    order_reference = order_id  # утримуємо однаковими для простоти відповідності

    with get_conn() as conn:
        ensure_user(conn, user_id)
        conn.execute("""
            INSERT INTO payments (user_id, provider, order_reference, amount, currency, status, raw_json, order_id)
            VALUES (?, ?, ?, ?, ?, 'pending', NULL, ?)
        """, (user_id, provider, order_reference, amount, currency, order_id))

    data_b64, signature = lp_build_data(order_id=order_id, amount=amount, currency=currency, description=description)
    pay_url = f"{PUBLIC_BASE_URL}/pay/{order_id}"

    log.info("Create payment: user_id=%s amount=%s order_id=%s", user_id, amount, order_id)

    return JSONResponse({
        "ok": True,
        "provider": provider,
        "order_id": order_id,
        "order_reference": order_reference,
        "pay_url": pay_url,
        "liqpay": {"data": data_b64, "signature": signature}
    })

@app.get("/pay/{order_id}", response_class=HTMLResponse)
def pay_page(order_id: str):
    """HTML-сторінка, яка авто-постить у LiqPay checkout."""
    with get_conn() as conn:
        row = conn.execute("""
            SELECT amount, currency
            FROM payments
            WHERE order_reference = ? OR order_id = ?
            LIMIT 1
        """, (order_id, order_id)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    amount, currency = row
    data_b64, signature = lp_build_data(order_id=order_id, amount=amount, currency=currency, description=f"Top-up {amount} {currency}")
    checkout_url = "https://www.liqpay.ua/api/3/checkout"

    return f"""<!doctype html>
<html lang="uk">
  <head>
    <meta charset="utf-8" />
    <title>Оплата {amount} {currency}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; display:flex; min-height:100vh; align-items:center; justify-content:center; background:#f8fafc; }}
      .box {{ max-width: 460px; width:100%; padding: 24px; border:1px solid #e5e7eb; border-radius:12px; background:#fff; box-shadow: 0 6px 18px rgba(0,0,0,0.06); }}
      .btn {{ display:inline-block; padding:12px 16px; border-radius:8px; border:0; background:#111827; color:#fff; font-weight:600; cursor:pointer; }}
      .muted {{ color:#6b7280; font-size:14px; }}
    </style>
  </head>
  <body>
    <div class="box">
      <h2>Переходимо до оплати</h2>
      <p>Сума: <b>{amount} {currency}</b></p>
      <form id="lp" method="POST" action="{checkout_url}">
        <input type="hidden" name="data" value="{data_b64}"/>
        <input type="hidden" name="signature" value="{signature}"/>
        <button class="btn" type="submit">Відкрити LiqPay</button>
      </form>
      <p class="muted">Якщо сторінка не відкрилась автоматично — натисніть кнопку.</p>
      <script>document.getElementById('lp').submit();</script>
    </div>
  </body>
</html>"""

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    LiqPay POSTs: data=<base64(json)>, signature=<...>
    На успіх: оновлюємо статус платежу, зараховуємо кредити, шлемо повідомлення у TG.
    """
    form = await req.form()
    data_b64 = form.get("data", "")
    signature = form.get("signature", "")

    # Перевірка підпису (можна вимкнути LIQPAY_SKIP_SIGNATURE=1 для локальних тестів)
    if not LIQPAY_SKIP_SIGNATURE:
        try:
            expected = lp_sign(data_b64)
            if signature != expected:
                log.error("Bad signature")
                return JSONResponse({"ok": False, "err": "bad-signature"}, status_code=400)
        except Exception:
            log.exception("Signature error")
            return JSONResponse({"ok": False, "err": "sign-ex"}, status_code=400)
    else:
        log.warning("Signature check DISABLED (LIQPAY_SKIP_SIGNATURE=1)")

    # Розбір payload
    try:
        payload = json.loads(base64.b64decode(data_b64))
    except Exception:
        log.exception("Bad payload base64/json")
        return JSONResponse({"ok": False, "err": "bad-payload"}, status_code=400)

    status = payload.get("status")
    order_id = payload.get("order_id")
    amount_uah = int(float(payload.get("amount", 0)))
    currency = payload.get("currency", "UAH")

    log.info("Callback: order_id=%s status=%s amount=%s %s", order_id, status, amount_uah, currency)

    if not order_id:
        return JSONResponse({"ok": False, "err": "no-order-id"}, status_code=400)

    # Працюємо лише з успішними статусами
    if status not in ("success", "sandbox", "subscribed"):
        log.info("Non-success status (%s), ignoring credit", status)
        return JSONResponse({"ok": True})

    # Оновлення платежу + зарахування
    user_id: Optional[int] = None
    new_balance: int = 0
    credits: int = 0
    amount_db: int = 0

    with get_conn() as conn:
        row = conn.execute("""
            SELECT id, user_id, amount, status
            FROM payments
            WHERE order_reference = ? OR order_id = ?
            LIMIT 1
        """, (order_id, order_id)).fetchone()

        if not row:
            log.error("Payment not found by order_id=%s", order_id)
            return JSONResponse({"ok": True})

        pid, user_id, amount_db, status_db = row

        # оновити статус + зберегти сирий JSON
        conn.execute("""
            UPDATE payments
            SET status=?, raw_json=?, updated_at=datetime('now')
            WHERE id=?
        """, ("success", json.dumps(payload, ensure_ascii=False), pid))

        # нарахувати кредити (ідемпотентність: простота — допускаємо повторення; можна додати прапорець "credited")
        credits = credits_from_amount(amount_db)
        conn.execute("""
            UPDATE users
            SET balance = COALESCE(balance,0) + ?
            WHERE user_id = ?
        """, (credits, user_id))

        new_balance = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()[0]

    log.info("Credited: user_id=%s credits=%s -> new_balance=%s", user_id, credits, new_balance)

    # Повідомлення у TG
    if user_id:
        text = (
            "💳 Оплату отримано!\n"
            f"+{credits} кредитів (сума {amount_db}₴)\n"
            f"Новий баланс: {new_balance} кредитів."
        )
        await tg_notify(user_id, text)

    return JSONResponse({"ok": True})

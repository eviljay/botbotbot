import os
import json
import base64
import sqlite3
import logging
from contextlib import contextmanager
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv
import httpx

from payments.liqpay_utils import verify_signature

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mybot-api")

app = FastAPI(title="MyBot Public API")

# ==== ENV ====
DB_PATH = os.getenv("DB_PATH", "mybot.db")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "")  # e.g. mybestseobot (без @)

# ==== DB HELPERS ====
@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def ensure_tables_and_schema():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                phone TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE NOT NULL,
                user_id INTEGER NOT NULL DEFAULT 0,
                amount REAL NOT NULL DEFAULT 0,
                credits INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        # ensure created_at column exists with default CURRENT_TIMESTAMP
        cur = conn.execute("PRAGMA table_info(payments)")
        cols = {r[1] for r in cur.fetchall()}
        if "created_at" not in cols:
            # add column with default so NOT NULL won't fail
            conn.execute("ALTER TABLE payments ADD COLUMN created_at DATETIME NOT NULL DEFAULT (CURRENT_TIMESTAMP)")

ensure_tables_and_schema()

def calc_credits_from_amount(amount_uah: float) -> int:
    try:
        return int(amount_uah // CREDIT_PRICE_UAH)
    except Exception:
        return 0

async def notify_user(user_id: int, added_credits: int, new_balance: int) -> None:
    if not TELEGRAM_BOT_TOKEN or user_id <= 0:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    text = (
        "✅ Оплату отримано!\n"
        f"+{added_credits} кредитів зараховано.\n"
        f"Новий баланс: {new_balance}"
    )
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={"chat_id": user_id, "text": text})

# ==== ROUTES ====
@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    redirect = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "#"
    html = f"""<!doctype html>
<html lang="uk">
<head>
<meta charset="utf-8">
<title>Дякуємо за оплату!</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="1;url={redirect}">
<script>
  (function() {{
    var url = "{redirect}";
    if (url && url !== "#") {{
      try {{ window.location.replace(url); }} catch(e) {{ window.location.href = url; }}
    }}
  }})();
</script>
<style>
  body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin:0; padding:40px; }}
  .box {{ max-width: 560px; margin: 0 auto; text-align: center; }}
  a.button {{ display:inline-block; padding:10px 16px; border-radius:8px; background:#10b981; color:#fff; text-decoration:none; }}
</style>
</head>
<body>
  <div class="box">
    <h1>Дякуємо за оплату! ✅</h1>
    <p>Оплату отримано. Можете повернутися до бота.</p>
    <p><a class="button" href="{redirect}">Відкрити бота</a></p>
    <small>Сторінка автоматично перенаправить вас…</small>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)

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

    status: str = str(payload.get("status", ""))
    order_id: str = str(payload.get("order_id", ""))
    amount: float = float(payload.get("amount", 0))
    user_info: Optional[str] = payload.get("info")
    try:
        uid = int(user_info) if user_info is not None else 0
    except ValueError:
        uid = 0

    credits = calc_credits_from_amount(amount)
    raw_json = json.dumps(payload, ensure_ascii=False)

    new_balance: Optional[int] = None

    with db() as conn:
        # insert/update payment row (created_at has DEFAULT, so we don't pass it)
        try:
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
                (order_id, uid, amount, credits, status, raw_json),
            )
        except Exception:
            log.exception("DB error on callback")
            raise HTTPException(status_code=500, detail="DB error")

        # credit balance only on success/sandbox and valid user
        if status in ("success", "sandbox") and uid > 0 and credits > 0:
            conn.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)", (uid,))
            conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, uid))
            row = conn.execute("SELECT balance FROM users WHERE user_id = ?", (uid,)).fetchone()
            new_balance = int(row[0]) if row else 0

    if new_balance is not None:
        try:

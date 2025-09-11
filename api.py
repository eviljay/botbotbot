# /root/mybot/api.py
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

# LiqPay utils
from payments.liqpay_utils import verify_signature

load_dotenv()
log = logging.getLogger("mybot-api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="MyBot Public API")

# ==== ENV ====
DB_PATH = os.getenv("DB_PATH", "mybot.db")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))  # скільки грн за 1 кредит
BOT_USERNAME = os.getenv("BOT_USERNAME", "")  # для редіректу на /thanks (наприклад, mybestseobot)

# ==== DB ====
@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    with db() as conn:
        # таблиця користувачів (якщо ще не створена)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                phone TEXT
            )
        """)
        # таблиця оплат
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

init_db()

def calc_credits_from_amount(amount_uah: float) -> int:
    # цілим числом, без копійок
    try:
        return int(amount_uah // CREDIT_PRICE_UAH)
    except Exception:
        return 0

async def notify_user(user_id: int, added_credits: int, new_balance: int) -> None:
    """Шлемо повідомлення в Telegram про поповнення."""
    if not TELEGRAM_BOT_TOKEN or user_id <= 0:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    txt = (
        f"✅ Оплату отримано!\n"
        f"+{added_credits} кредитів зараховано.\n"
        f"Новий баланс: {new_balance}"
    )
    async with httpx.AsyncClient(timeout=10) as c:
        await c.post(url, json={"chat_id": user_id, "text": txt})

# ==== РОУТИ ====
@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    """
    Статична сторінка подяки з авторедіректом назад у бота (якщо задано BOT_USERNAME).
    """
    redirect = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "#"
    html = f"""
<!doctype html>
<html lang="uk">
<head>
<meta charset="utf-8">
<title>Дякуємо за оплату!</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="3;url={redirect}">
<style>
  body {{ font-family: sans-serif; padding: 40px; }}
  .box {{ max-width: 560px; margin: 0 auto; text-align: center; }}
</style>
</head>
<body>
  <div class="box">
    <h1>Дякуємо за оплату! ✅</h1>
    <p>Оплату отримано. Можете повернутися до бота.</p>
    <p><a href="{redirect}">Перейти до бота</a></p>
    <small>Автоповернення через 3 секунди…</small>
  </div>
</body>
</html>
    """
    return HTMLResponse(content=html, status_code=200)

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    Callback від LiqPay:
      - перевіряємо підпис
      - парсимо payload
      - записуємо в таблицю payments
      - якщо статус success/sandbox і є user_id -> нараховуємо кредити та шлемо нотиф
    """
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
    # телеграмний user_id передається як info
    user_id_raw: Optional[str] = payload.get("info")
    try:
        uid = int(user_id_raw) if user_id_raw is not None else 0
    except ValueError:
        uid = 0

    credits = calc_credits_from_amount(amount)
    raw_json = json.dumps(payload, ensure_ascii=False)

    # пишемо в БД; якщо дубль order_id – оновлюємо
    with db() as conn:
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

        # якщо оплата успішна (у т.ч. sandbox) і маємо валідний uid -> нараховуємо
        if status in ("success", "sandbox") and uid > 0 and credits > 0:
            conn.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)", (uid,))
            conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, uid))
            row = conn.execute("SELECT balance FROM users WHERE user_id = ?", (uid,)).fetchone()
            new_balance = int(row[0]) if row else 0
        else:
            new_balance = None

    # нотиф юзеру поза транзакцією
    if new_balance is not None:
        try:
            await notify_user(uid, credits, new_balance)
        except Exception:
            log.exception("notify_user failed")

    return JSONResponse({"ok": True})

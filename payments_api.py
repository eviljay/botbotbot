# payments_api.py
import os
import re
import hmac
import json
import math
import time
import base64
import hashlib
import logging
import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from dotenv import load_dotenv
import httpx

# ====== Логи ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("payments-api")

# ====== ENV ======
load_dotenv()

# LiqPay
LIQPAY_PUBLIC_KEY   = os.getenv("LIQPAY_PUBLIC_KEY", "")
LIQPAY_PRIVATE_KEY  = os.getenv("LIQPAY_PRIVATE_KEY", "")
LIQPAY_RESULT_URL   = os.getenv("LIQPAY_RESULT_URL", "")     # напр.: https://server1.seoswiss.online/thanks
LIQPAY_SERVER_URL   = os.getenv("LIQPAY_SERVER_URL", "")     # напр.: https://server1.seoswiss.online/liqpay/callback

# WayForPay
WFP_MERCHANT_ACCOUNT = os.getenv("WFP_MERCHANT_ACCOUNT", "")
WFP_MERCHANT_DOMAIN  = os.getenv("WFP_MERCHANT_DOMAIN", "")  # БЕЗ https://
WFP_SECRET_KEY       = os.getenv("WFP_SECRET_KEY", "")
WFP_API_URL          = os.getenv("WFP_API_URL", "https://api.wayforpay.com/api")
WFP_SERVICE_URL      = os.getenv("WFP_SERVICE_URL", "")      # напр.: https://server1.seoswiss.online/wayforpay/callback
# Fallback на стару змінну, якщо MERCHANT_DOMAIN не заданий
if not WFP_MERCHANT_DOMAIN:
    _fallback_dom = (os.getenv("WFP_DOMAIN") or "").strip()
    _fallback_dom = re.sub(r"^https?://", "", _fallback_dom).strip("/")
    WFP_MERCHANT_DOMAIN = _fallback_dom

# Загальні
DEFAULT_CCY        = os.getenv("LIQPAY_CURRENCY", "UAH")    # спільна валюта для обох провайдерів
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH            = os.getenv("DB_PATH", "bot.db")
CREDIT_PRICE_UAH   = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# ====== FastAPI ======
app = FastAPI(title="Payments API (LiqPay + WayForPay)")

# Пам'ять для редіректу: /pay/{order_id} -> pay_url (для LiqPay, якщо треба)
ORDER_CACHE: dict[str, str] = {}

# ====== DB helpers & bootstrap ======
def _db():
    return sqlite3.connect(DB_PATH)

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cols: set[str] = set()
    cur = conn.execute(f"PRAGMA table_info({table})")
    for _, name, *_rest in cur.fetchall():
        cols.add(name)
    return cols

def _init_db():
    with _db() as conn:
        # users
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0,
            phone   TEXT
        )
        """)

        # payments (уніфікована, проста)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            order_id   TEXT PRIMARY KEY,
            provider   TEXT NOT NULL,
            user_id    INTEGER NOT NULL,
            amount     REAL NOT NULL,
            currency   TEXT NOT NULL,
            credits    INTEGER NOT NULL DEFAULT 0,
            status     TEXT NOT NULL,
            raw        TEXT,
            created_at TEXT NOT NULL
        )
        """)

        # Міграції: якщо був старий формат таблиці — додаємо потрібні колонки
        cols = _table_columns(conn, "payments")
        # Деякі старі схеми мали інший primary key — створимо відсутні поля
        if "order_id" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN order_id TEXT")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id)")
        if "credits" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN credits INTEGER NOT NULL DEFAULT 0")
        if "raw" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN raw TEXT")
        if "created_at" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN created_at TEXT")

        # users.phone інколи відсутній
        ucols = _table_columns(conn, "users")
        if "phone" not in ucols:
            conn.execute("ALTER TABLE users ADD COLUMN phone TEXT")

        conn.commit()

_init_db()

# ====== LiqPay утиліти ======
def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def _liqpay_encode(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _b64(raw)

def _liqpay_sign(data_b64: str) -> str:
    # signature = base64( sha1( private_key + data + private_key ) )
    to_sign = (LIQPAY_PRIVATE_KEY + data_b64 + LIQPAY_PRIVATE_KEY).encode("utf-8")
    digest = hashlib.sha1(to_sign).digest()
    return _b64(digest)

def _gen_order_id(user_id) -> str:
    return f"{user_id}-{os.urandom(6).hex()}"

# ====== WayForPay утиліти ======
def _wfp_clean_domain(raw: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r"^https?://", "", raw.strip())
    return raw.strip("/")

def _wfp_make_create_signature(
    merchantAccount: str,
    merchantDomainName: str,
    orderReference: str,
    orderDate: int,
    amount_str: str,          # "100.00"
    currency: str,
    productNames: list[str],
    productCounts: list[int],
    productPrices_str: list[str],  # ["100.00"]
) -> tuple[str, str]:
    """
    Повертає (sign_message, signature_hex).
    Формула (SimpleSignature, CREATE_INVOICE):
    merchantAccount;merchantDomainName;orderReference;orderDate;amount;currency;productName[];productCount[];productPrice[]
    """
    parts: list[str] = [
        merchantAccount,
        merchantDomainName,
        orderReference,
        str(orderDate),
        amount_str,
        currency,
        *productNames,
        *[str(c) for c in productCounts],
        *productPrices_str,
    ]
    sign_message = ";".join(parts)
    signature = hmac.new(WFP_SECRET_KEY.encode("utf-8"), sign_message.encode("utf-8"), hashlib.md5).hexdigest()
    return sign_message, signature

def _wfp_verify_callback_signature(payload: dict) -> bool:
    """
    Callback signature (з доки):
    merchantAccount;orderReference;amount;currency;authCode;cardPan;transactionStatus;reasonCode
    """
    parts = [
        payload.get("merchantAccount", ""),
        payload.get("orderReference", ""),
        str(payload.get("amount", "")),
        payload.get("currency", ""),
        payload.get("authCode", ""),
        payload.get("cardPan", ""),
        payload.get("transactionStatus", ""),
        payload.get("reasonCode", ""),
    ]
    msg = ";".join(parts)
    calc = hmac.new(WFP_SECRET_KEY.encode("utf-8"), msg.encode("utf-8"), hashlib.md5).hexdigest()
    got = (payload.get("merchantSignature") or "").lower()
    if got != calc.lower():
        log.warning("WFP callback: signature mismatch: got=%s calc=%s msg='%s'", got, calc, msg)
        return False
    return True

def _wfp_response_signature(orderReference: str, status: str, time_int: int) -> str:
    msg = ";".join([orderReference, status, str(time_int)])
    return hmac.new(WFP_SECRET_KEY.encode("utf-8"), msg.encode("utf-8"), hashlib.md5).hexdigest()

# ====== Helpers ======
def _credit_amount_to_credits(amount_uah: float) -> int:
    return max(1, math.ceil(amount_uah / CREDIT_PRICE_UAH))

async def _notify_user_tg(user_id: int, text: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": user_id, "text": text}
            )
            r.raise_for_status()
    except Exception:
        log.exception("Telegram sendMessage failed")

def _insert_or_update_payment(conn: sqlite3.Connection, order_id: str, provider: str, user_id: int,
                              amount: float, credits: int, status: str, raw: dict) -> bool:
    """
    Повертає True, якщо вперше зафіксовано success і ми нарахували кредити.
    """
    now_iso = _utc_now_iso()
    # спробуємо прочитати
    cur = conn.execute("SELECT status FROM payments WHERE order_id = ?", (order_id,))
    row = cur.fetchone()

    if row is None:
        conn.execute(
            "INSERT INTO payments(order_id, provider, user_id, amount, currency, credits, status, raw, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (order_id, provider, user_id, amount, DEFAULT_CCY, credits, status, json.dumps(raw, ensure_ascii=False), now_iso)
        )
        if status == "success":
            conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, ?)", (user_id, 0))
            conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, user_id))
            return True
        return False

    prev_status = (row[0] or "").lower()
    if prev_status != "success" and status == "success":
        conn.execute("UPDATE payments SET status = ?, raw = ? WHERE order_id = ?",
                     ("success", json.dumps(raw, ensure_ascii=False), order_id))
        conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, ?)", (user_id, 0))
        conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, user_id))
        return True
    else:
        # просто оновимо raw (історія стану)
        conn.execute("UPDATE payments SET raw = ? WHERE order_id = ?",
                     (json.dumps(raw, ensure_ascii=False), order_id))
        return False

# ====== API ======
@app.get("/health")
async def health():
    return {"ok": True, "time": _utc_now_iso()}

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "currency": "UAH",            # optional
        "provider": "liqpay|wayforpay" # optional, default: liqpay
      }
    Відповідь:
      { ok, provider, order_id, pay_url }
    """
    body = await req.json()
    user_id  = body.get("user_id")
    amount   = body.get("amount")
    currency = (body.get("currency") or DEFAULT_CCY).upper()
    provider = (body.get("provider") or "liqpay").lower()

    if not user_id or not amount:
        raise HTTPException(400, "user_id and amount required")

    try:
        amount_f = float(amount)
        if amount_f <= 0:
            raise ValueError
    except Exception:
        raise HTTPException(400, "amount must be a number > 0")

    order_id = str(body.get("order_id") or _gen_order_id(user_id))

    # ---------- LiqPay ----------
    if provider == "liqpay":
        if not (LIQPAY_PUBLIC_KEY and LIQPAY_PRIVATE_KEY and LIQPAY_SERVER_URL and LIQPAY_RESULT_URL):
            raise HTTPException(500, "LiqPay is not configured")

        payload = {
            "version": "3",
            "public_key": LIQPAY_PUBLIC_KEY,
            "action": "pay",
            "amount": f"{amount_f:.2f}",
            "currency": currency,
            "description": f"Top-up {int(round(amount_f))} UAH",
            "order_id": order_id,
            "server_url": LIQPAY_SERVER_URL,
            "result_url": LIQPAY_RESULT_URL,
            # "sandbox": "1",
        }

        data_b64   = _liqpay_encode(payload)
        signature  = _liqpay_sign(data_b64)
        pay_url    = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

        ORDER_CACHE[order_id] = pay_url
        log.info("Create payment [LiqPay]: user=%s amount=%.2f %s order_id=%s", user_id, amount_f, currency, order_id)

        with _db() as conn:
            _insert_or_update_payment(conn, order_id, "liqpay", int(user_id), amount_f,
                                      _credit_amount_to_credits(amount_f), "pending",
                                      {"req": "create_liqpay", "payload": payload})

        return JSONResponse({"ok": True, "provider": "liqpay", "order_id": order_id, "pay_url": pay_url})

    # ---------- WayForPay ----------
    elif provider in ("wayforpay", "wfp"):
        mdomain = _wfp_clean_domain(WFP_MERCHANT_DOMAIN)
        if not (WFP_MERCHANT_ACCOUNT and mdomain and WFP_SECRET_KEY):
            raise HTTPException(500, "WayForPay is not configured")

        order_ts = int(time.time())

        # Формат для ПІДПИСУ — рівно 2 знаки
        amt_dec = Decimal(str(amount_f)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        amount_for_sign = f"{amt_dec:.2f}"          # "100.00"
        product_names  = ["Top-up credits"]
        product_counts = [1]
        product_prices_for_sign = [amount_for_sign] # ["100.00"]

        sign_msg, signature = _wfp_make_create_signature(
            WFP_MERCHANT_ACCOUNT,
            mdomain,
            order_id,
            order_ts,
            amount_for_sign,
            currency,
            product_names,
            product_counts,
            product_prices_for_sign
        )
        log.info("WFP sign_message='%s' signature='%s'", sign_msg, signature)

        # У JSON — ЧИСЛА (Decimal -> float), WayForPay це любить
        amount_for_json = float(amt_dec)           # 100.0
        product_prices_json = [float(amt_dec)]     # [100.0]

        req_payload = {
            "transactionType": "CREATE_INVOICE",
            "merchantAccount": WFP_MERCHANT_ACCOUNT,
            "merchantAuthType": "SimpleSignature",
            "merchantDomainName": mdomain,
            "merchantSignature": signature,
            "apiVersion": 1,
            "serviceUrl": (WFP_SERVICE_URL or None),
            "orderReference": order_id,
            "orderDate": order_ts,
            "amount": amount_for_json,
            "currency": currency,
            "productName": product_names,
            "productPrice": product_prices_json,
            "productCount": product_counts,
        }
        req_payload = {k: v for k, v in req_payload.items() if v is not None}

        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(WFP_API_URL, json=req_payload)
                r.raise_for_status()
                resp = r.json()
        except Exception as e:
            log.exception("WayForPay create invoice failed")
            raise HTTPException(502, f"WayForPay error: {e}")

        invoice_url = resp.get("invoiceUrl")
        if not invoice_url:
            log.error("WFP create error: %s ; sent payload: %s", resp, req_payload)
            return JSONResponse({"ok": False, "provider": "wayforpay", "error": resp}, status_code=502)

        with _db() as conn:
            _insert_or_update_payment(conn, order_id, "wayforpay", int(user_id), amount_f,
                                      _credit_amount_to_credits(amount_f), "pending",
                                      {"req": "create_wfp", "payload": req_payload, "resp": resp})

        log.info("Create payment [WFP]: user=%s amount=%.2f %s order_id=%s", user_id, amount_f, currency, order_id)
        return JSONResponse({"ok": True, "provider": "wayforpay", "order_id": order_id, "pay_url": invoice_url})

    else:
        raise HTTPException(400, "Unknown provider")

@app.get("/pay/{order_id}")
async def pay_redirect(order_id: str):
    pay_url = ORDER_CACHE.get(order_id)
    if not pay_url:
        raise HTTPException(404, "Unknown order_id")
    return RedirectResponse(pay_url, status_code=302)

# ====== LiqPay callback ======
@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    Серверний колбек від LiqPay (POST form-data: data, signature).
    """
    form = await req.form()
    data_b64  = form.get("data")
    sign_recv = form.get("signature")

    if not data_b64 or not sign_recv:
        raise HTTPException(400, "Missing data or signature")

    sign_calc = _liqpay_sign(data_b64)
    if sign_calc != sign_recv:
        log.warning("Invalid signature callback (LiqPay)")
        raise HTTPException(400, "Invalid signature")

    payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    log.info("LiqPay callback: %s", payload)

    status   = (payload.get("status") or "").lower()      # success, failure, sandbox, etc.
    order_id = payload.get("order_id") or ""
    amount   = float(payload.get("amount") or 0)

    m = re.match(r"^(\d+)-", str(order_id))
    if not m:
        log.error("Cannot parse user_id from order_id=%s", order_id)
        return JSONResponse({"ok": False, "reason": "bad_order_id"})
    user_id = int(m.group(1))

    new_status = "success" if status in ("success", "sandbox") else status
    try:
        with _db() as conn:
            credits = _credit_amount_to_credits(amount)
            newly_credited = _insert_or_update_payment(conn, order_id, "liqpay", user_id, amount, credits, new_status, payload)
            conn.commit()
    except Exception as e:
        log.exception("DB update error (LiqPay)")
        return JSONResponse({"ok": False, "reason": f"db_error: {e}"})

    if new_status == "success" and newly_credited:
        await _notify_user_tg(user_id, f"✅ Оплату отримано: +{amount:.0f}₴ → +{_credit_amount_to_credits(amount)} кредит(и). Дякуємо!")

    return JSONResponse({"ok": True})

# ====== WayForPay callback ======
@app.post("/wayforpay/callback")
async def wayforpay_callback(req: Request):
    """
    WayForPay serviceUrl callback (JSON).
    Очікується JSON зі стандартними полями і merchantSignature.
    Відповідь (accept/reject) теж підписується.
    """
    try:
        payload = await req.json()
    except Exception:
        log.exception("WFP callback: bad JSON")
        raise HTTPException(400, "Bad JSON")

    order_id = payload.get("orderReference") or ""
    amount   = float(payload.get("amount") or 0)
    status_w = (payload.get("transactionStatus") or "").lower()  # Approved/Declined/Expired/Voided
    user_id  = None

    m = re.match(r"^(\d+)-", str(order_id))
    if m:
        try:
            user_id = int(m.group(1))
        except Exception:
            user_id = None

    if not _wfp_verify_callback_signature(payload):
        log.warning("Invalid signature callback (WFP)")
        ts = int(time.time())
        resp = {
            "orderReference": order_id,
            "status": "reject",
            "time": ts,
            "signature": _wfp_response_signature(order_id, "reject", ts)
        }
        return JSONResponse(resp)

    if user_id is None:
        log.error("WFP callback: cannot extract user_id from orderReference=%s", order_id)
        ts = int(time.time())
        resp = {
            "orderReference": order_id,
            "status": "reject",
            "time": ts,
            "signature": _wfp_response_signature(order_id, "reject", ts)
        }
        return JSONResponse(resp)

    new_status = "success" if status_w == "approved" else status_w

    try:
        with _db() as conn:
            credits = _credit_amount_to_credits(amount)
            newly_credited = _insert_or_update_payment(conn, order_id, "wayforpay", user_id, amount, credits, new_status, payload)
            conn.commit()
    except Exception:
        log.exception("DB update error (WFP)")
        ts = int(time.time())
        resp = {
            "orderReference": order_id,
            "status": "reject",
            "time": ts,
            "signature": _wfp_response_signature(order_id, "reject", ts)
        }
        return JSONResponse(resp)

    ts = int(time.time())
    resp_status = "accept"
    resp = {
        "orderReference": order_id,
        "status": resp_status,
        "time": ts,
        "signature": _wfp_response_signature(order_id, resp_status, ts)
    }

    if newly_credited and new_status == "success":
        await _notify_user_tg(user_id, f"✅ Оплату отримано: +{amount:.0f}₴ → +{_credit_amount_to_credits(amount)} кредит(и). Дякуємо!")

    return JSONResponse(resp)

# ====== THANKS ======
@app.get("/thanks", response_class=HTMLResponse)
async def thanks_page():
    try:
        bot_url = (os.getenv("TELEGRAM_BOT_URL") or "").strip()
        start_param = os.getenv("TELEGRAM_START_PARAM", "paid")

        if bot_url:
            sep = "&" if "?" in bot_url else "?"
            dest = f"{bot_url}{sep}start={start_param}"
            log.info("THANKS redirect -> %s", dest)
            return RedirectResponse(dest, status_code=302)

        return HTMLResponse(
            """
            <html><body style="font-family:system-ui; text-align:center; padding:40px">
              <h1>✅ Оплату отримано</h1>
              <p>Тепер можете повернутися в бот.</p>
            </body></html>
            """,
            status_code=200,
        )
    except Exception:
        log.exception("/thanks failed")
        return HTMLResponse(
            """
            <html><body style="font-family:system-ui; text-align:center; padding:40px">
              <h1>✅ Оплату отримано</h1>
              <p>(fallback) Поверніться в бот вручну.</p>
            </body></html>
            """,
            status_code=200,
        )

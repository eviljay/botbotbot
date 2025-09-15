# payments_api.py
import os
import re
import hmac
import json
import math
import base64
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, List

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
LIQPAY_PUBLIC_KEY  = os.getenv("LIQPAY_PUBLIC_KEY", "")
LIQPAY_PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")
LIQPAY_RESULT_URL  = os.getenv("LIQPAY_RESULT_URL", "")     # напр.: https://server1.seoswiss.online/thanks
LIQPAY_SERVER_URL  = os.getenv("LIQPAY_SERVER_URL", "")     # напр.: https://server1.seoswiss.online/liqpay/callback
LIQPAY_SANDBOX     = os.getenv("LIQPAY_SANDBOX", "0") in ("1", "true", "True")

# WayForPay
WFP_MERCHANT_ACCOUNT = os.getenv("WFP_MERCHANT_ACCOUNT", "")
WFP_MERCHANT_DOMAIN  = os.getenv("WFP_MERCHANT_DOMAIN", "")  # ВАЖЛИВО: лише домен, без https://
WFP_SECRET_KEY       = os.getenv("WFP_SECRET_KEY", "")
WFP_API_URL          = os.getenv("WFP_API_URL", "https://api.wayforpay.com/api")
WFP_SERVICE_URL      = os.getenv("WFP_SERVICE_URL", "")      # напр.: https://server1.seoswiss.online/wayforpay/callback

# Загальні
DEFAULT_CCY        = os.getenv("LIQPAY_CURRENCY", "UAH")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH            = os.getenv("DB_PATH", "data/bot.db")
CREDIT_PRICE_UAH   = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# ====== FastAPI ======
app = FastAPI(title="Payments API (LiqPay + WayForPay)")

# Пам'ять для редіректу /pay/{order_id} (LiqPay)
ORDER_CACHE: Dict[str, str] = {}

# ====== DB ======
def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _table_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any((r[1] == col) for r in cur.fetchall())

def _init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with _db() as conn:
        # users
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id  INTEGER PRIMARY KEY,
            balance  INTEGER NOT NULL DEFAULT 0,
            phone    TEXT
        )
        """)
        # payments (уніфікована)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            order_id    TEXT PRIMARY KEY,
            provider    TEXT NOT NULL,
            user_id     INTEGER NOT NULL,
            amount      REAL NOT NULL,
            currency    TEXT NOT NULL,
            credits     INTEGER NOT NULL,
            status      TEXT NOT NULL,
            raw         TEXT,
            created_at  TEXT NOT NULL
        )
        """)
        # self-heal старих інсталяцій
        # (нічого не робимо, якщо таблиця вже відповідає новій схемі)
        conn.commit()

_init_db()

# ====== Утиліти ======
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

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

def _ensure_user_and_add_credits(conn: sqlite3.Connection, user_id: int, credits: int):
    conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, 0)", (user_id,))
    conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, user_id))

def _insert_or_update_payment(conn: sqlite3.Connection, order_id: str, provider: str, user_id: int,
                              amount: float, credits: int, currency: str,
                              status: str, raw: dict) -> bool:
    """
    Повертає True, якщо вперше зафіксовано success і нараховано кредити.
    """
    cur = conn.execute("SELECT status FROM payments WHERE order_id=?", (order_id,))
    row = cur.fetchone()
    now_iso = _utc_now_iso()

    if row is None:
        conn.execute(
            "INSERT INTO payments(order_id, provider, user_id, amount, currency, credits, status, raw, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (order_id, provider, user_id, amount, currency, credits, status, json.dumps(raw, ensure_ascii=False), now_iso)
        )
        if status == "success":
            _ensure_user_and_add_credits(conn, user_id, credits)
            return True
        return False
    else:
        prev = (row["status"] or "").lower()
        if prev != "success" and status == "success":
            conn.execute("UPDATE payments SET status=?, raw=? WHERE order_id=?",
                         ("success", json.dumps(raw, ensure_ascii=False), order_id))
            _ensure_user_and_add_credits(conn, user_id, credits)
            return True
        else:
            conn.execute("UPDATE payments SET raw=? WHERE order_id=?",
                         (json.dumps(raw, ensure_ascii=False), order_id))
            return False

def _gen_order_id(user_id: int) -> str:
    return f"{user_id}-{os.urandom(6).hex()}"

# ====== LiqPay helpers ======
def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def _liqpay_encode(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _b64(raw)

def _liqpay_sign(data_b64: str) -> str:
    to_sign = (LIQPAY_PRIVATE_KEY + data_b64 + LIQPAY_PRIVATE_KEY).encode("utf-8")
    digest = hashlib.sha1(to_sign).digest()
    return _b64(digest)

# ====== WayForPay helpers ======
def _fmt_num_for_signature(n: float) -> str:
    # Використовуємо цілі числа без .00, якщо можливо — саме так у тебе підпис проходив
    if abs(n - int(n)) < 1e-9:
        return str(int(n))
    s = f"{n:.2f}"
    # прибрати зайві нулі/крапку (на випадок .10 -> .1)
    s = s.rstrip("0").rstrip(".")
    return s

def _wfp_join(parts: List[str]) -> str:
    return ";".join(parts)

def _wfp_hmac_md5(message: str) -> str:
    return hmac.new(WFP_SECRET_KEY.encode("utf-8"), message.encode("utf-8"), hashlib.md5).hexdigest()

def _wfp_make_request_signature(merchantAccount: str, merchantDomainName: str,
                                orderReference: str, orderDate: int,
                                amount: float, currency: str,
                                productNames: List[str], productCounts: List[int], productPrices: List[float]) -> str:
    parts: List[str] = [
        merchantAccount,
        merchantDomainName,
        orderReference,
        str(orderDate),
        _fmt_num_for_signature(amount),
        currency,
        *productNames,
        *[str(c) for c in productCounts],
        *[_fmt_num_for_signature(p) for p in productPrices],
    ]
    msg = _wfp_join(parts)
    sig = _wfp_hmac_md5(msg)
    log.warning("WFP sign: domain='%s' msg='%s' sig='%s'", merchantDomainName, msg, sig)
    return sig

def _wfp_verify_callback_signature(payload: Dict[str, Any]) -> bool:
    parts = [
        str(payload.get("merchantAccount", "")),
        str(payload.get("orderReference", "")),
        str(payload.get("amount", "")),
        str(payload.get("currency", "")),
        str(payload.get("authCode", "")),
        str(payload.get("cardPan", "")),
        str(payload.get("transactionStatus", "")),
        str(payload.get("reasonCode", "")),
    ]
    calc = _wfp_hmac_md5(_wfp_join(parts))
    got  = (payload.get("merchantSignature") or "").lower()
    if got != calc.lower():
        log.warning("WFP callback: signature mismatch: got=%s calc=%s msg='%s'", got, calc, _wfp_join(parts))
    return got == calc

def _wfp_response_signature(orderReference: str, status: str, time_int: int) -> str:
    return _wfp_hmac_md5(_wfp_join([orderReference, status, str(time_int)]))

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
    """
    body = await req.json()
    user_id  = int(body.get("user_id") or 0)
    amount   = body.get("amount")
    currency = (body.get("currency") or DEFAULT_CCY).upper()
    provider = (body.get("provider") or "liqpay").lower()

    if not user_id or amount is None:
        raise HTTPException(400, "user_id and amount required")
    try:
        amount_f = float(amount)
        if amount_f <= 0:
            raise ValueError
    except Exception:
        raise HTTPException(400, "amount must be a number > 0")

    order_id = str(body.get("order_id") or _gen_order_id(user_id))

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
        }
        if LIQPAY_SANDBOX:
            payload["sandbox"] = "1"

        data_b64  = _liqpay_encode(payload)
        signature = _liqpay_sign(data_b64)
        pay_url   = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

        ORDER_CACHE[order_id] = pay_url
        log.info("Create payment [LiqPay]: user=%s amount=%.2f %s order_id=%s", user_id, amount_f, currency, order_id)

        with _db() as conn:
            _insert_or_update_payment(conn, order_id, "liqpay", user_id, amount_f,
                                      _credit_amount_to_credits(amount_f), currency,
                                      "pending", {"req": "create_liqpay", "payload": payload})
            conn.commit()
        return JSONResponse({"ok": True, "provider": "liqpay", "order_id": order_id, "pay_url": pay_url})

    elif provider in ("wayforpay", "wfp"):
        if not (WFP_MERCHANT_ACCOUNT and WFP_MERCHANT_DOMAIN and WFP_SECRET_KEY):
            raise HTTPException(500, "WayForPay is not configured")

        order_ts = int(datetime.now(timezone.utc).timestamp())
        product_names  = ["Top-up credits"]
        product_counts = [1]
        product_prices = [amount_f]

        signature = _wfp_make_request_signature(
            WFP_MERCHANT_ACCOUNT,
            WFP_MERCHANT_DOMAIN,
            order_id,
            order_ts,
            amount_f,
            currency,
            product_names,
            product_counts,
            product_prices
        )

        req_payload = {
            "transactionType": "CREATE_INVOICE",
            "merchantAccount": WFP_MERCHANT_ACCOUNT,
            "merchantAuthType": "SimpleSignature",
            "merchantDomainName": WFP_MERCHANT_DOMAIN,
            "merchantSignature": signature,
            "apiVersion": 1,
            "serviceUrl": WFP_SERVICE_URL or None,
            "orderReference": order_id,
            "orderDate": order_ts,
            "amount": amount_f,
            "currency": currency,
            "productName": product_names,
            "productPrice": product_prices,
            "productCount": product_counts,
        }
        req_payload = {k: v for k, v in req_payload.items() if v is not None}

        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(WFP_API_URL, json=req_payload)
                r.raise_for_status()
                resp = r.json()
        except Exception:
            log.exception("WayForPay create invoice failed (network)")
            raise HTTPException(502, "WayForPay network error")

        invoice_url = resp.get("invoiceUrl")
        if not invoice_url:
            log.error("WFP create error: %s ; sent payload: %s", resp, req_payload)
            return JSONResponse({"ok": False, "provider": "wayforpay", "error": resp}, status_code=502)

        with _db() as conn:
            _insert_or_update_payment(conn, order_id, "wayforpay", user_id, amount_f,
                                      _credit_amount_to_credits(amount_f), currency,
                                      "pending", {"req": "create_wfp", "payload": req_payload, "resp": resp})
            conn.commit()

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

    status    = (payload.get("status") or "").lower()    # success, failure, sandbox...
    order_id  = payload.get("order_id") or ""
    amount    = float(payload.get("amount") or 0)
    currency  = payload.get("currency") or DEFAULT_CCY

    m = re.match(r"^(\d+)-", str(order_id))
    if not m:
        log.error("Cannot parse user_id from order_id=%s", order_id)
        return JSONResponse({"ok": False, "reason": "bad_order_id"})
    user_id = int(m.group(1))

    newly_credited = False
    try:
        with _db() as conn:
            credits = _credit_amount_to_credits(amount)
            new_status = "success" if status in ("success", "sandbox") else status
            newly_credited = _insert_or_update_payment(conn, order_id, "liqpay", user_id, amount, credits, currency, new_status, payload)
            conn.commit()
    except Exception as e:
        log.exception("DB update error (LiqPay)")
        return JSONResponse({"ok": False, "reason": f"db_error: {e}"})

    if newly_credited:
        await _notify_user_tg(user_id, f"✅ Оплату отримано: +{amount:.0f}₴ → +{_credit_amount_to_credits(amount)} кредит(и). Дякуємо!")

    return JSONResponse({"ok": True})

# ====== WayForPay callback ======
@app.post("/wayforpay/callback")
async def wayforpay_callback(req: Request):
    try:
        payload = await req.json()
    except Exception:
        log.exception("WFP callback: bad JSON")
        raise HTTPException(400, "Bad JSON")

    order_id = str(payload.get("orderReference") or "")
    amount   = float(payload.get("amount") or 0)
    currency = str(payload.get("currency") or DEFAULT_CCY)
    status_w = (payload.get("transactionStatus") or "").lower()  # Approved / Declined / ...
    user_id  = None

    # Відповідь одразу готуємо (на випадок reject)
    ts = int(datetime.now(timezone.utc).timestamp())
    resp_status = "reject"
    resp = {
        "orderReference": order_id,
        "status": resp_status,
        "time": ts,
        "signature": _wfp_response_signature(order_id, resp_status, ts)
    }

    if not _wfp_verify_callback_signature(payload):
        log.warning("Invalid signature callback (WFP)")
        return JSONResponse(resp)

    m = re.match(r"^(\d+)-", order_id)
    if m:
        try:
            user_id = int(m.group(1))
        except Exception:
            user_id = None

    if user_id is None:
        log.error("WFP callback: cannot extract user_id from orderReference=%s", order_id)
        return JSONResponse(resp)

    new_status = "success" if status_w == "approved" else status_w

    try:
        with _db() as conn:
            credits = _credit_amount_to_credits(amount)
            newly_credited = _insert_or_update_payment(conn, order_id, "wayforpay", user_id, amount, credits, currency, new_status, payload)
            conn.commit()
    except Exception:
        log.exception("DB update error (WFP)")
        return JSONResponse(resp)

    # ok → accept
    resp_status = "accept"
    resp = {
        "orderReference": order_id,
        "status": resp_status,
        "time": ts,
        "signature": _wfp_response_signature(order_id, resp_status, ts)
    }

    if new_status == "success" and newly_credited:
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

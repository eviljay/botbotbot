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
from typing import Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from dotenv import load_dotenv
import httpx

# ===== Логування =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("payments-api")

load_dotenv()

def _env(name: str, default: str = "") -> str:
    val = os.getenv(name, default)
    return (val or "").strip()

# ===== ENV =====
# LiqPay
LIQPAY_PUBLIC_KEY   = _env("LIQPAY_PUBLIC_KEY")
LIQPAY_PRIVATE_KEY  = _env("LIQPAY_PRIVATE_KEY")
LIQPAY_RESULT_URL   = _env("LIQPAY_RESULT_URL")
LIQPAY_SERVER_URL   = _env("LIQPAY_SERVER_URL")

# WayForPay
WFP_MERCHANT_ACCOUNT = _env("WFP_MERCHANT_ACCOUNT") or _env("WFP_ACCOUNT")
WFP_MERCHANT_DOMAIN  = _env("WFP_MERCHANT_DOMAIN") or _env("WFP_DOMAIN")
WFP_SECRET_KEY       = _env("WFP_SECRET_KEY")
WFP_API_URL          = _env("WFP_API_URL") or "https://api.wayforpay.com/api"
WFP_SERVICE_URL      = _env("WFP_SERVICE_URL")

# Загальне
DEFAULT_CCY        = _env("LIQPAY_CURRENCY", "UAH").upper()
TELEGRAM_BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN")
DB_PATH            = _env("DB_PATH", "bot.db")
CREDIT_PRICE_UAH   = float(_env("CREDIT_PRICE_UAH", "5"))

# ===== FastAPI =====
app = FastAPI(title="Payments API (LiqPay + WayForPay)")
ORDER_CACHE: Dict[str, str] = {}

# ===== DB =====
def _db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _table_columns(conn: sqlite3.Connection, table: str) -> set:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}

def _init_db():
    # Нічого не ламаємо: якщо таблиці вже є – не чіпаємо схему,
    # тільки створюємо за потреби й додаємо відсутні поля.
    with _db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0,
            phone   TEXT
        )""")
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
        )""")
        cols = _table_columns(conn, "payments")
        # додаємо сумісні поля, якщо треба
        if "order_id" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN order_id TEXT")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id)")
        if "credits" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN credits INTEGER NOT NULL DEFAULT 0")
        if "raw" not in cols and "raw_json" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN raw TEXT")
        if "created_at" not in cols:
            conn.execute("ALTER TABLE payments ADD COLUMN created_at TEXT")
        ucols = _table_columns(conn, "users")
        if "phone" not in ucols:
            conn.execute("ALTER TABLE users ADD COLUMN phone TEXT")
        conn.commit()

_init_db()

# ===== LiqPay helpers =====
def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def _liqpay_encode(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _b64(raw)

def _liqpay_sign(data_b64: str) -> str:
    to_sign = (LIQPAY_PRIVATE_KEY + data_b64 + LIQPAY_PRIVATE_KEY).encode("utf-8")
    digest = hashlib.sha1(to_sign).digest()
    return _b64(digest)

def _gen_order_id(user_id) -> str:
    return f"{user_id}-{os.urandom(6).hex()}"

# ===== WayForPay helpers =====
def _host_from_url(url: str) -> str:
    if not url:
        return ""
    s = re.sub(r"^https?://", "", url.strip(), flags=re.I)
    s = s.split("/")[0]
    s = s.split(":")[0]
    return s.strip().lower()

def _wfp_clean_domain(raw: str) -> str:
    host = _host_from_url(raw)
    if not host:
        return ""
    if not re.fullmatch(r"[a-z0-9.-]+", host):
        return ""
    return host

def _wfp_resolve_domain() -> str:
    for candidate in (WFP_MERCHANT_DOMAIN, _env("WFP_DOMAIN"), WFP_SERVICE_URL):
        host = _wfp_clean_domain(candidate or "")
        if host:
            return host
    return ""

def _wfp_amount_str(amount_f: float) -> str:
    """
    WayForPay хоче amount/price БЕЗ зайвих нулів:
    100.0 -> "100", 100.50 -> "100.5", 100.25 -> "100.25"
    """
    dec = Decimal(str(amount_f)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    s = format(dec.normalize(), 'f')
    if "." in s:
        whole, frac = s.split(".", 1)
        frac = frac[:2].rstrip("0")
        s = whole if frac == "" else f"{whole}.{frac}"
    return s

def _wfp_build_create_sign_message(merchantAccount: str, merchantDomainName: str,
                                   orderReference: str, orderDate: int,
                                   amount_str: str, currency: str,
                                   productNames: list, productCounts: list, productPrices_str: list) -> str:
    parts = [
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
    return ";".join(parts)

def _wfp_hmac_md5(message: str) -> str:
    return hmac.new(WFP_SECRET_KEY.encode("utf-8"), message.encode("utf-8"), hashlib.md5).hexdigest()

def _wfp_make_create_signature(merchantAccount: str, merchantDomainName: str,
                               orderReference: str, orderDate: int,
                               amount_str: str, currency: str,
                               productNames: list, productCounts: list, productPrices_str: list) -> Tuple[str, str]:
    msg = _wfp_build_create_sign_message(
        merchantAccount, merchantDomainName, orderReference, orderDate,
        amount_str, currency, productNames, productCounts, productPrices_str
    )
    sig = _wfp_hmac_md5(msg)
    return msg, sig

def _wfp_verify_callback_signature(payload: dict) -> bool:
    def _s(key: str) -> str:
        v = payload.get(key, "")
        return str(v if v is not None else "")

    parts = [
        _s("merchantAccount"),
        _s("orderReference"),
        _s("amount"),
        _s("currency"),
        _s("authCode"),
        _s("cardPan"),
        _s("transactionStatus"),
        _s("reasonCode"),
    ]
    msg = ";".join(parts)
    calc = _wfp_hmac_md5(msg)
    got  = (_s("merchantSignature")).lower()

    if got == calc.lower():
        return True

    # запасний варіант: деякі інтеграції підписують amount у «зрізаному» вигляді
    try:
        amt = float(payload.get("amount", 0))
        msg2_parts = parts[:]
        msg2_parts[2] = _wfp_amount_str(amt)  # 100 -> "100", 100.50 -> "100.5"
        msg2 = ";".join(msg2_parts)
        calc2 = _wfp_hmac_md5(msg2)
        if got == calc2.lower():
            log.warning("WFP callback: matched with normalized amount. msg='%s'", msg2)
            return True
    except Exception:
        pass

    log.warning("WFP callback: signature mismatch: got=%s calc=%s msg='%s'", got, calc, msg)
    return False

def _wfp_response_signature(orderReference: str, status: str, time_int: int) -> str:
    msg = ";".join([orderReference, status, str(time_int)])
    return _wfp_hmac_md5(msg)

# ===== Helpers =====
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

# ===== Узгодження зі «старою» схемою payments =====
def _payments_has_legacy_columns(cols: set) -> bool:
    # ознаки старої схеми: є order_reference/raw_json/updated_at/id
    return ("order_reference" in cols) or ("raw_json" in cols) or ("updated_at" in cols) or ("id" in cols)

def _select_existing_status(conn: sqlite3.Connection, order_id: str) -> str | None:
    cols = _table_columns(conn, "payments")
    # пріоритет: order_id, потім order_reference
    if "order_id" in cols:
        cur = conn.execute("SELECT status FROM payments WHERE order_id = ?", (order_id,))
        row = cur.fetchone()
        if row:
            return row[0]
    if "order_reference" in cols:
        cur = conn.execute("SELECT status FROM payments WHERE order_reference = ?", (order_id,))
        row = cur.fetchone()
        if row:
            return row[0]
    return None

def _insert_payment_row(conn: sqlite3.Connection, order_id: str, provider: str, user_id: int,
                        amount: float, currency: str, credits: int,
                        status: str, raw_json: dict):
    cols = _table_columns(conn, "payments")
    raw_txt = json.dumps(raw_json, ensure_ascii=False)

    if _payments_has_legacy_columns(cols):
        # будуємо INSERT динамічно під наявні стовпці
        col_vals: Dict[str, Any] = {
            "user_id": user_id,
            "provider": provider,
            "amount": amount,
            "currency": currency,
            "status": status,
        }
        if "order_id" in cols:
            col_vals["order_id"] = order_id
        if "order_reference" in cols:
            # критично: у старій схемі NOT NULL → підставляємо order_id
            col_vals["order_reference"] = order_id
        if "credits" in cols:
            col_vals["credits"] = credits
        if "raw_json" in cols:
            col_vals["raw_json"] = raw_txt
        elif "raw" in cols:
            col_vals["raw"] = raw_txt
        if "created_at" in cols:
            col_vals["created_at"] = _utc_now_iso()
        if "updated_at" in cols:
            col_vals["updated_at"] = _utc_now_iso()

        cols_list = list(col_vals.keys())
        placeholders = ",".join(["?"] * len(cols_list))
        sql = f"INSERT INTO payments({','.join(cols_list)}) VALUES({placeholders})"
        conn.execute(sql, tuple(col_vals.values()))
    else:
        # наша «нова» схема
        conn.execute(
            "INSERT INTO payments(order_id, provider, user_id, amount, currency, credits, status, raw, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (order_id, provider, user_id, amount, currency, credits, status, raw_txt, _utc_now_iso())
        )

def _update_payment_row(conn: sqlite3.Connection, order_id: str, status: str, raw_json: dict):
    cols = _table_columns(conn, "payments")
    raw_txt = json.dumps(raw_json, ensure_ascii=False)

    if _payments_has_legacy_columns(cols):
        sets = []
        args = []
        # статус
        sets.append("status = ?")
        args.append(status)
        # raw/raw_json
        if "raw_json" in cols:
            sets.append("raw_json = ?")
            args.append(raw_txt)
        elif "raw" in cols:
            sets.append("raw = ?")
            args.append(raw_txt)
        # updated_at
        if "updated_at" in cols:
            sets.append("updated_at = ?")
            args.append(_utc_now_iso())

        set_sql = ", ".join(sets)
        if "order_id" in cols:
            conn.execute(f"UPDATE payments SET {set_sql} WHERE order_id = ?", (*args, order_id))
        elif "order_reference" in cols:
            conn.execute(f"UPDATE payments SET {set_sql} WHERE order_reference = ?", (*args, order_id))
        else:
            # fallback — не повинно статись
            conn.execute(f"UPDATE payments SET {set_sql} WHERE order_id = ?", (*args, order_id))
    else:
        conn.execute("UPDATE payments SET status = ?, raw = ? WHERE order_id = ?", (status, raw_txt, order_id))

def _insert_or_update_payment(conn: sqlite3.Connection, order_id: str, provider: str, user_id: int,
                              amount: float, credits: int, status: str, raw: dict) -> bool:
    """
    Повертає True, якщо вперше перевели у success і нарахували кредити.
    """
    prev_status = _select_existing_status(conn, order_id)
    if prev_status is None:
        _insert_payment_row(conn, order_id, provider, user_id, amount, DEFAULT_CCY, credits, status, raw)
        if status == "success":
            conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?,0)", (user_id,))
            conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, user_id))
            return True
        return False

    # було, оновлюємо
    newly = (prev_status.lower() != "success" and status == "success")
    _update_payment_row(conn, order_id, status, raw)
    if newly:
        conn.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?,0)", (user_id,))
        conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (credits, user_id))
        return True
    return False

# ===== API =====
@app.get("/health")
async def health():
    return {
        "ok": True,
        "time": _utc_now_iso(),
        "wfp_domain": _wfp_resolve_domain() or None,
        "wfp_account": WFP_MERCHANT_ACCOUNT or None
    }

@app.post("/api/payments/create")
async def create_payment(req: Request):
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

    # ---- LiqPay ----
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
        data_b64  = _liqpay_encode(payload)
        signature = _liqpay_sign(data_b64)
        pay_url   = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

        ORDER_CACHE[order_id] = pay_url
        with _db() as conn:
            _insert_or_update_payment(conn, order_id, "liqpay", int(user_id),
                                      amount_f, _credit_amount_to_credits(amount_f),
                                      "pending", {"req": "create_liqpay", "payload": payload})
            conn.commit()
        log.info("Create payment [LiqPay]: user=%s amount=%.2f %s order_id=%s", user_id, amount_f, currency, order_id)
        return JSONResponse({"ok": True, "provider": "liqpay", "order_id": order_id, "pay_url": pay_url})

    # ---- WayForPay ----
    elif provider in ("wayforpay", "wfp"):
        mdomain = _wfp_resolve_domain()
        if not (WFP_MERCHANT_ACCOUNT and mdomain and WFP_SECRET_KEY):
            raise HTTPException(500, "WayForPay is not configured")
        if not re.fullmatch(r"[a-z0-9.-]+", mdomain) or mdomain.count(".") < 1:
            raise HTTPException(500, f"Invalid WFP_MERCHANT_DOMAIN='{mdomain}'. Use bare host like 'example.com'")

        order_ts = int(time.time())
        amount_for_sign = _wfp_amount_str(amount_f)
        product_names    = ["Top-up credits"]
        product_counts   = [1]
        product_prices_s = [amount_for_sign]

        sign_msg, signature = _wfp_make_create_signature(
            WFP_MERCHANT_ACCOUNT, mdomain, order_id, order_ts,
            amount_for_sign, currency, product_names, product_counts, product_prices_s
        )
        log.warning("WFP sign: domain='%s' msg='%s' sig='%s'", mdomain, sign_msg, signature)

        try:
            amt_num = float(Decimal(amount_for_sign))
        except Exception:
            amt_num = amount_f

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
            "amount": amt_num,
            "currency": currency,
            "productName": product_names,
            "productPrice": [amt_num],
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
            _insert_or_update_payment(conn, order_id, "wayforpay", int(user_id),
                                      amount_f, _credit_amount_to_credits(amount_f),
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

# ===== LiqPay callback =====
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

    status   = (payload.get("status") or "").lower()
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

# ===== WayForPay callback =====
@app.post("/wayforpay/callback")
async def wayforpay_callback(req: Request):
    try:
        payload = await req.json()
    except Exception:
        log.exception("WFP callback: bad JSON")
        raise HTTPException(400, "Bad JSON")

    order_id = payload.get("orderReference") or ""
    amount   = float(payload.get("amount") or 0)
    status_w = (payload.get("transactionStatus") or "").lower()

    if not _wfp_verify_callback_signature(payload):
        log.warning("Invalid signature callback (WFP)")
        ts = int(time.time())
        return JSONResponse({
            "orderReference": order_id,
            "status": "reject",
            "time": ts,
            "signature": _wfp_response_signature(order_id, "reject", ts)
        })

    m = re.match(r"^(\d+)-", str(order_id))
    if not m:
        log.error("WFP callback: cannot extract user_id from orderReference=%s", order_id)
        ts = int(time.time())
        return JSONResponse({
            "orderReference": order_id,
            "status": "reject",
            "time": ts,
            "signature": _wfp_response_signature(order_id, "reject", ts)
        })

    user_id = int(m.group(1))
    new_status = "success" if status_w == "approved" else status_w

    try:
        with _db() as conn:
            credits = _credit_amount_to_credits(amount)
            newly_credited = _insert_or_update_payment(conn, order_id, "wayforpay", user_id, amount, credits, new_status, payload)
            conn.commit()
    except Exception:
        log.exception("DB update error (WFP)")
        ts = int(time.time())
        return JSONResponse({
            "orderReference": order_id,
            "status": "reject",
            "time": ts,
            "signature": _wfp_response_signature(order_id, "reject", ts)
        })

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

# ===== THANKS =====
@app.get("/thanks", response_class=HTMLResponse)
async def thanks_page():
    try:
        bot_url = _env("TELEGRAM_BOT_URL")
        start_param = _env("TELEGRAM_START_PARAM", "paid")
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

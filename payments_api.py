# payments_api.py
import os
import re
import json
import base64
import uuid
import hashlib
import logging
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv
import requests

# =========================
# ENV / CONFIG
# =========================
load_dotenv()

# LiqPay keys
LIQPAY_PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "").strip()
LIQPAY_PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "").strip()

# Callback/result URLs MUST be public HTTPS for production
SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "").strip()   # e.g. https://your.domain/liqpay/callback
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "").strip()   # e.g. https://your.domain/thanks

# Optional: sandbox status treat as success (useful while testing)
TREAT_SANDBOX_SUCCESS = os.getenv("LIQPAY_TREAT_SANDBOX_AS_SUCCESS", "true").lower() in ("1", "true", "yes")

# Telegram bot for user notifications
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# App settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

"""
Sample .env:

LIQPAY_PUBLIC_KEY=your_public_key_here
LIQPAY_PRIVATE_KEY=your_private_key_here
LIQPAY_SERVER_URL=https://server1.seoswiss.online/liqpay/callback
LIQPAY_RESULT_URL=https://server1.seoswiss.online/thanks
TELEGRAM_BOT_TOKEN=123456789:ABC-DEF...
LIQPAY_TREAT_SANDBOX_AS_SUCCESS=true
LOG_LEVEL=INFO
"""

logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("payments-api")


# =========================
# FASTAPI APP
# =========================
app = FastAPI(title="Payments API (LiqPay)")

# CORS (за потреби відкрий свій фронт)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # підкоригуй на проді
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# UTILS
# =========================
def make_order_id() -> str:
    """Short unique id."""
    return uuid.uuid4().hex[:12]


def b64encode_str(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")


def build_data(payload: Dict[str, Any]) -> str:
    """LiqPay requires base64-encoded JSON as 'data'."""
    js = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return base64.b64encode(js.encode("utf-8")).decode("utf-8")


def sign_data(data_b64: str) -> str:
    """signature = base64( sha1( PRIVATE_KEY + data + PRIVATE_KEY ) )"""
    raw = (LIQPAY_PRIVATE_KEY or "") + data_b64 + (LIQPAY_PRIVATE_KEY or "")
    digest = hashlib.sha1(raw.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("utf-8")


def verify_signature(data_b64: str, signature: str) -> bool:
    expected = sign_data(data_b64)
    ok = (expected == signature)
    if not ok:
        log.warning("Bad LiqPay signature: expected %s, got %s", expected, signature)
    return ok


def build_checkout_link(
    *,
    amount: float,
    currency: str,
    description: str,
    order_id: str,
    server_url: str,
    result_url: str,
    sandbox: Optional[int] = None,
    **extra,
) -> str:
    """
    Returns a GET-able LiqPay checkout URL constructed from data+signature.
    For embedded widget you usually render a POST form; but this URL also works.
    """
    payload = {
        "public_key": LIQPAY_PUBLIC_KEY,
        "version": 3,
        "action": "pay",
        "amount": amount,
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "server_url": server_url,
        "result_url": result_url,
        **extra,  # allow custom fields if needed
    }
    if sandbox is not None:
        payload["sandbox"] = sandbox

    data_b64 = build_data(payload)
    signature = sign_data(data_b64)
    # LiqPay's standard flow is via a form POST, but their /checkout supports data+signature in query
    return f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"


# ---- very simple idempotency memory (replace with DB/Redis in prod) ----
PROCESSED_TX = set()

def has_processed(tx_id: Optional[str]) -> bool:
    return bool(tx_id) and tx_id in PROCESSED_TX

def mark_processed(tx_id: Optional[str]):
    if tx_id:
        PROCESSED_TX.add(tx_id)

# ---- stubs: replace with your real storage/DAO ----
def credit_user(user_id: int, amount: float, currency: str):
    """
    TODO: UPDATE USER BALANCE IN YOUR DB/STORE HERE.
    This is only a stub with logging.
    """
    log.info(f"[CREDIT] +{amount} {currency} -> user {user_id}")


def notify_user(user_id: int, text: str):
    """
    Sends a Telegram message to the user chat if TELEGRAM_BOT_TOKEN is set.
    """
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN is not set; skipping notify")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": user_id, "text": text},
            timeout=5,
        )
        if resp.status_code != 200:
            log.error(f"Telegram notify failed: {resp.status_code} {resp.text}")
    except Exception as e:
        log.error(f"Telegram notify exception: {e}")


def extract_user_id_from_order(order_id: str) -> Optional[int]:
    """
    We use order_id like '{user_id}-{random}', e.g. '244142655-93af1b0c12d3'
    """
    try:
        if "-" in order_id:
            return int(order_id.split("-", 1)[0])
    except Exception:
        pass
    return None


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/thanks", response_class=HTMLResponse)
def thanks():
    return HTMLResponse("<h1>Дякуємо! Платіж обробляється.</h1>")


@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "currency": "UAH",               # optional (default "UAH")
        "description": "100 UAH topup"   # optional
      }

    Returns:
      {
        "ok": true,
        "order_id": "...",
        "checkout_url": "https://www.liqpay.ua/api/3/checkout?...",
        "data": "...",                   # optional (if you want to render form)
        "signature": "..."
      }
    """
    body = await req.json()
    user_id = body.get("user_id")
    amount = body.get("amount")
    currency = body.get("currency", "UAH")
    description = body.get("description")

    if user_id is None or amount is None:
        raise HTTPException(status_code=400, detail="user_id and amount are required")

    if not LIQPAY_PUBLIC_KEY or not LIQPAY_PRIVATE_KEY:
        raise HTTPException(status_code=500, detail="LiqPay keys are not configured")

    if not SERVER_URL or not RESULT_URL:
        raise HTTPException(status_code=500, detail="SERVER_URL/RESULT_URL are not configured")

    order_id = f"{user_id}-{make_order_id()}"
    description = description or f"Top-up {amount} by user {user_id}"

    # pass sandbox=1 if you want to force sandbox mode on LiqPay side
    payload = {
        "public_key": LIQPAY_PUBLIC_KEY,
        "version": 3,
        "action": "pay",
        "amount": amount,
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "server_url": SERVER_URL,
        "result_url": RESULT_URL,
        # "sandbox": 1,  # uncomment to force sandbox payments
    }

    data_b64 = build_data(payload)
    signature = sign_data(data_b64)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    log.info(f"Created payment: order_id={order_id} amount={amount} {currency} user={user_id}")

    return JSONResponse(
        {
            "ok": True,
            "order_id": order_id,
            "checkout_url": checkout_url,
            "data": data_b64,       # if front wants to render a <form> POST to /api/3/checkout
            "signature": signature,
        }
    )


@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    LiqPay sends form-data with fields 'data' and 'signature'
    Docs: signature = base64( sha1( PRIVATE_KEY + data + PRIVATE_KEY ) )
    """
    form = await req.form()
    data_b64 = form.get("data")
    signature = form.get("signature")

    if not data_b64 or not signature:
        raise HTTPException(status_code=400, detail="No data/signature")

    if not verify_signature(data_b64, signature):
        raise HTTPException(status_code=400, detail="Bad signature")

    try:
        payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Bad payload: {e}")

    # Extract important fields
    status = payload.get("status")          # success / sandbox / failure / wait_secure / processing ...
    action = payload.get("action")          # pay / hold / subscribe ...
    order_id = payload.get("order_id", "")
    currency = payload.get("currency", "UAH")
    amount = float(payload.get("amount", 0) or 0)

    # tx id can be in different fields depending on flow
    transaction_id = (
        payload.get("transaction_id")
        or payload.get("payment_id")
        or payload.get("liqpay_order_id")
    )

    log.info(f"[CALLBACK] action={action} status={status} order_id={order_id} amount={amount} {currency} tx={transaction_id}")

    # Idempotency
    if has_processed(transaction_id):
        return {"ok": True}

    # Extract user_id
    user_id = extract_user_id_from_order(order_id)
    if not user_id:
        # fallback: try description/info
        info = payload.get("info") or payload.get("description") or ""
        m = re.search(r"\b(\d{6,})\b", info)
        if m:
            try:
                user_id = int(m.group(1))
            except Exception:
                user_id = None

    # Decide if we treat it as success
    success_statuses = {"success"}
    if TREAT_SANDBOX_SUCCESS:
        success_statuses.add("sandbox")

    if action == "pay" and status in success_statuses and amount > 0:
        if not user_id:
            # Do not throw 4xx here to avoid infinite retries by LiqPay
            log.error(f"[CALLBACK] Missing user_id for order_id={order_id}; payload info={payload.get('info')}")
            return {"ok": True}

        # 1) Update balance
        credit_user(user_id, amount, currency)

        # 2) Mark processed
        mark_processed(transaction_id)

        # 3) Notify
        notify_user(user_id, f"✅ Рахунок поповнено: +{amount} {currency}\nЗамовлення: {order_id}")

        return {"ok": True}

    # Other statuses: acknowledge
    return {"ok": True}

# app/payments_api.py
import os
import uuid
import json
import logging
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from payments.wayforpay_utils import (
    build_create_invoice_payload,
    create_invoice_request,
    verify_callback_signature as wfp_verify,
    build_accept_response as wfp_accept,
)
from payments.liqpay_utils import (
    PUBLIC_KEY as LIQPAY_PUBLIC_KEY,
    PRIVATE_KEY as LIQPAY_PRIVATE_KEY,
    build_checkout_link as liqpay_checkout,
    verify_callback_signature as liqpay_verify,
)

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payments-api")

app = FastAPI(title="Payments API (WayForPay + LiqPay)")

# ==== WayForPay ENV ====
WAYFORPAY_MERCHANT_ACCOUNT = os.getenv("WAYFORPAY_MERCHANT_ACCOUNT", "").strip()
WAYFORPAY_SECRET_KEY = os.getenv("WAYFORPAY_SECRET_KEY", "").strip()
MERCHANT_DOMAIN = os.getenv("MERCHANT_DOMAIN", "seoswiss.online").strip()
WAYFORPAY_RESULT_URL = os.getenv("WAYFORPAY_RESULT_URL", "").strip()
WAYFORPAY_CALLBACK_URL = os.getenv("WAYFORPAY_CALLBACK_URL", "").strip()

# ==== LiqPay ENV ====
LIQPAY_RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "").strip()
LIQPAY_SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "").strip()
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "UAH").strip()

def _order_id() -> str:
    return uuid.uuid4().hex[:12]

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "description": "Top-up",
        "currency": "UAH",
        "provider": "wayforpay" | "liqpay"
      }
    Response (уніфіковано):
      { "ok": true, "order_id": "...", "pay_url": "https://..." }
    """
    body = await req.json()
    user_id = body.get("user_id")
    amount = body.get("amount")
    description: Optional[str] = body.get("description")
    currency = (body.get("currency") or DEFAULT_CURRENCY).upper()
    provider = (body.get("provider") or "wayforpay").lower()

    if user_id is None:
        raise HTTPException(400, "user_id is required")
    try:
        amount = float(amount)
        if amount <= 0:
            raise ValueError
    except Exception:
        raise HTTPException(400, "amount must be positive number")

    order_id = _order_id()
    title = description or f"Top-up by user {user_id}"

    # ---- WayForPay ----
    if provider == "wayforpay":
        if not WAYFORPAY_MERCHANT_ACCOUNT or not WAYFORPAY_SECRET_KEY:
            raise HTTPException(500, "WayForPay is not configured")
        payload = build_create_invoice_payload(
            merchant_account=WAYFORPAY_MERCHANT_ACCOUNT,
            secret_key=WAYFORPAY_SECRET_KEY,
            merchant_domain=MERCHANT_DOMAIN,
            order_reference=order_id,
            amount=amount,
            currency=currency,
            product_names=[title],
            product_prices=[amount],
            product_counts=[1],
            service_url=WAYFORPAY_CALLBACK_URL or None,
            return_url=WAYFORPAY_RESULT_URL or None,
            language="UA",
            api_version=1,
        )
        try:
            wfp = await create_invoice_request(payload)
        except Exception as e:
            log.exception("WayForPay API error")
            raise HTTPException(502, f"WayForPay API error: {e}")

        url = wfp.get("invoiceUrl")
        if not url:
            return JSONResponse({"ok": False, "order_id": order_id, "provider": "wayforpay", "wfp": wfp}, status_code=502)
        log.info("WFP invoice created: order=%s amount=%.2f", order_id, amount)
        return JSONResponse({"ok": True, "order_id": order_id, "provider": "wayforpay", "pay_url": url})

    # ---- LiqPay ----
    if provider == "liqpay":
        if not LIQPAY_PUBLIC_KEY or not LIQPAY_PRIVATE_KEY:
            raise HTTPException(500, "LiqPay is not configured")
        link = liqpay_checkout(
            amount=amount,
            currency=currency,
            description=title,
            order_id=order_id,
            result_url=LIQPAY_RESULT_URL or None,
            server_url=LIQPAY_SERVER_URL or None,
            language="uk",
        )
        url = link["checkout_url"]
        log.info("LiqPay checkout: order=%s amount=%.2f", order_id, amount)
        # Можеш також повернути data/sign, якщо на фронті захочеш робити форму:
        return JSONResponse({
            "ok": True, "order_id": order_id, "provider": "liqpay",
            "pay_url": url, "data": link["data"], "signature": link["signature"]
        })

    raise HTTPException(400, "Unknown provider (use 'wayforpay' or 'liqpay')")

# ---- WayForPay Callback ----
@app.post("/api/payments/wayforpay/callback")
async def wayforpay_callback(req: Request):
    data = await req.json()
    log.info("WFP callback: %s", data)
    if not wfp_verify(data, WAYFORPAY_SECRET_KEY):
        raise HTTPException(403, "Invalid signature")
    order_ref = data.get("orderReference")
    status = data.get("transactionStatus")  # Approved / Declined / InPending
    amount = data.get("amount")
    currency = data.get("currency")

    # TODO: тут оновити БД по order_ref (status), нарахувати баланс/доступ, надіслати нотиф у бота

    return JSONResponse(wfp_accept(order_ref, WAYFORPAY_SECRET_KEY))

# ---- LiqPay Callback ----
@app.post("/api/payments/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    LiqPay шле form-data або JSON: беремо data, signature
    """
    try:
        # пробуємо як form-data
        form = await req.form()
        data_b64 = form.get("data")
        signature = form.get("signature")
    except Exception:
        # резерв — як JSON
        body = await req.json()
        data_b64 = body.get("data")
        signature = body.get("signature")

    if not data_b64 or not signature:
        raise HTTPException(400, "missing data/signature")

    if not liqpay_verify(data_b64, signature):
        raise HTTPException(403, "invalid signature")

    payload = json.loads(base64_decode(data_b64))
    log.info("LiqPay callback payload: %s", payload)

    order_id = payload.get("order_id")
    status = payload.get("status")  # success, failure, error, sandbox, wait_accept
    amount = payload.get("amount")
    currency = payload.get("currency")

    # TODO: оновити БД по order_id, якщо status == "success" → зарахувати

    return JSONResponse({"ok": True})

def base64_decode(s: str) -> str:
    import base64
    # LiqPay надсилає стандартний base64 без переносів
    return base64.b64decode(s.encode("utf-8")).decode("utf-8")

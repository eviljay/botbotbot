import os
import uuid
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse

# === INIT ===
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payments-api")

app = FastAPI(title="Payments API (LiqPay + WayForPay)")

# === LIQPAY UTILS (ваш існуючий модуль) ===
from payments.liqpay_utils import build_data, sign, PUBLIC_KEY

LIQPAY_SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")   # https://<domain>/liqpay/callback
LIQPAY_RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")   # https://<domain>/thanks

def make_liqpay_order_id() -> str:
    return uuid.uuid4().hex[:12]

# === WAYFORPAY ===
from payments.wayforpay_utils import (
    enabled as wfp_enabled,
    build_purchase_fields, create_payment_link, verify_callback_signature,
    build_ack
)

# === DAO ===
# має бути у вас: charge(user_id, delta_credits)
from dao import charge

# ------------- LIQPAY -------------
@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Створити платіж LiqPay.
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "currency": "UAH",            # optional
        "description": "100 UAH topup" # optional
      }
    """
    body = await req.json()
    user_id = body.get("user_id")
    if user_id is None:
        return JSONResponse({"ok": False, "error": "user_id required"}, status_code=400)

    amount = float(body.get("amount", 0))
    if amount <= 0:
        return JSONResponse({"ok": False, "error": "amount must be > 0"}, status_code=400)

    currency = body.get("currency", "UAH")
    description = body.get("description", f"Top-up {int(amount)} credits")

    order_id = make_liqpay_order_id()
    payload = {
        "public_key": PUBLIC_KEY,
        "version": "3",
        "action": "pay",
        "amount": f"{amount:.2f}",
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "server_url": LIQPAY_SERVER_URL,
        "result_url": LIQPAY_RESULT_URL,
    }
    data = build_data(payload)
    signature = sign(data)
    return {"ok": True, "provider": "liqpay", "order_id": order_id, "data": data, "signature": signature}

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    LiqPay callback приходить з полями data/signature (base64).
    Ви вже реалізовували — лишаємо вашу логіку зарахування тут.
    """
    try:
        body = await req.json()
    except Exception:
        body = await req.form()

    log.info(f"LiqPay callback: {dict(body)}")

    # TODO: перевірка підпису (як у вас зроблено в liqpay_utils) + парсинг data
    # Приклад: якщо status == 'success' -> charge(user_id, +amount_credits)
    # Тут user_id зазвичай не приходить явно — у вас логіка, яка мапить order_id до user_id (чи з опису).
    # Залишаю як є, щоб не зламати ваш прод-кейс.

    return JSONResponse({"ok": True})

# ------------- WAYFORPAY -------------
@app.post("/api/payments/wayforpay/create")
async def wfp_create(req: Request):
    """
    Створити платіж WayForPay (offline link).
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "description": "Top-up 100 credits"  # optional
      }
    """
    if not wfp_enabled():
        return JSONResponse({"ok": False, "error": "WayForPay disabled"}, status_code=503)

    body = await req.json()
    user_id = int(body["user_id"])
    amount = float(body["amount"])
    desc = body.get("description") or f"Top-up {int(amount)} credits"

    order_ref = f"{user_id}-{int(amount)}"  # короткий префікс; у підписі не критично
    fields = build_purchase_fields(order_ref=order_ref, amount=amount, product_name=desc)
    url = await create_payment_link(fields)
    return {"ok": True, "provider": "wayforpay", "order_id": order_ref, "url": url}

@app.post("/wayforpay/callback")
async def wfp_callback(req: Request):
    """
    WayForPay serviceUrl callback (JSON).
    1) перевіряємо підпис
    2) якщо Approved/1100 — зараховуємо
    3) повертаємо ACK (orderReference;status;time)
    """
    payload = await req.json()
    log.info(f"WFP callback: {payload}")

    if not verify_callback_signature(payload):
        log.error("WFP invalid signature")
        return JSONResponse({"error": "invalid signature"}, status_code=400)

    order_ref = str(payload.get("orderReference", ""))
    txn_status = payload.get("transactionStatus")
    reason_code = str(payload.get("reasonCode"))
    amount = float(payload.get("amount", 0))

    # Дістаємо user_id з orderReference (формат: "<user>-<amount>")
    user_id = None
    try:
        user_id = int(order_ref.split("-")[0])
    except Exception:
        log.error(f"WFP: cannot parse user_id from orderReference '{order_ref}'")

    if txn_status == "Approved" and reason_code == "1100" and user_id:
        try:
            charge(user_id, +int(amount))  # 1 грн = 1 кредит (як у вас)
            log.info(f"WFP: charged +{amount} for user {user_id}")
        except Exception as e:
            log.exception(f"WFP charge error: {e}")

    return JSONResponse(build_ack(order_ref))

# ------------- THANKS PAGE -------------
@app.get("/thanks")
async def thanks():
    return HTMLResponse("<h1>Дякуємо!</h1><p>Оплата обробляється. Можна повернутися в бот і натиснути «Перевірити баланс».</p>")

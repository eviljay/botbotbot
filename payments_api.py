import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from payments.portmone_utils import build_payment_link, make_order_id

load_dotenv()
log = logging.getLogger("payments-api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Payments API (Portmone)")

SERVER_URL = os.getenv("PORTMONE_CALLBACK_URL", "")   # https://<domain>/api/payments/portmone/callback
RESULT_URL = os.getenv("PORTMONE_RESULT_URL", "")     # https://<domain>/thanks
CALLBACK_SECRET = os.getenv("PAYMENTS_CALLBACK_SECRET", "")

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
      {
        "user_id": 244142655,
        "amount": 100,
        "currency": "UAH",            # optional, для відображення; Portmone = UAH
        "description": "100 UAH topup" # optional
      }
    Повертаємо payment_url (Hosted Checkout Portmone).
    """
    body = await req.json()
    user_id = body.get("user_id")
    amount = body.get("amount")  # float|int|None
    description = body.get("description")

    if user_id is None:
        raise HTTPException(400, "user_id is required")

    order_id = make_order_id()
    payment_url = build_payment_link(order_id, float(amount) if amount else None, description)

    # Тут можеш зберігати замовлення в БД (order_id, user_id, amount, status=pending)
    log.info("Create payment: user=%s order=%s amount=%s", user_id, order_id, amount)

    return JSONResponse({"ok": True, "order_id": order_id, "payment_url": payment_url})

@app.post("/api/payments/portmone/callback")
async def portmone_callback(req: Request):
    """
    Portmone шле статус оплати (налаштовується в кабінеті як callback/webhook).
    Раджу додати свій секрет у header або query при налаштуванні (якщо Portmone дозволяє),
    тут робимо просту перевірку.
    """
    # Простий захист: ?secret=...
    secret = req.query_params.get("secret", "")
    if CALLBACK_SECRET and secret != CALLBACK_SECRET:
        raise HTTPException(403, "bad secret")

    data = await req.json()
    # Очікувані поля з Portmone (назви залежать від конкретного формату; адаптуєш після першого живого Ping):
    order_id = data.get("shop_order_number") or data.get("order_id")
    status   = data.get("status")  # APPROVED | DECLINED | PENDING | ...
    amount   = data.get("amount")

    if not order_id:
        raise HTTPException(400, "order_id missing")

    log.info("Portmone callback: order=%s status=%s amount=%s", order_id, status, amount)

    # TODO: онови БД: знайти order_id -> виставити статус
    # if status == "APPROVED": видати доступ/баланс у боті

    return JSONResponse({"ok": True})

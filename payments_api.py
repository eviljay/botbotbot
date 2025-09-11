import os
import uuid
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from payments.liqpay_utils import build_data, sign, PUBLIC_KEY

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payments-api")

app = FastAPI(title="Payments API (LiqPay)")

SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")   # https://<domain>/liqpay/callback
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")   # https://<domain>/thanks

def make_order_id() -> str:
    return uuid.uuid4().hex[:12]

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
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
        return JSONResponse({"ok": False, "error": "user_id is required"}, status_code=422)

    amount = float(body.get("amount", 100))
    currency = body.get("currency", "UAH")
    description = body.get("description", f"{int(amount)} UAH topup")
    order_id = f"{user_id}-{body.get('order_id') or make_order_id()}"

    params = {
        "version": 3,
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": amount,
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "result_url": RESULT_URL,
        "server_url": SERVER_URL,
        "sandbox": 1,          # remove in prod
        "info": str(user_id),  # IMPORTANT: used in callback to credit the right user
    }

    data_b64 = build_data(params)
    signature = sign(data_b64)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    resp = {
        "ok": True,
        "order_id": order_id,
        "public_key": PUBLIC_KEY,
        "data": data_b64,
        "signature": signature,
        "checkout_url": checkout_url,
        "invoiceUrl": checkout_url,    # дублюємо ключ для сумісності з ботом
    }
    log.info("Invoice created: %s (user_id=%s, amount=%.2f)", order_id, user_id, amount)
    return JSONResponse(resp)

# /root/mybot/payments_api.py
import os
import uuid
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# наші утиліти LiqPay
from payments.liqpay_utils import build_data, sign, PUBLIC_KEY

load_dotenv()
log = logging.getLogger("payments-api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Payments API (LiqPay)")

# .env
SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")   # https://<your-domain>/liqpay/callback
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")   # https://<your-domain>/thanks

def make_order_id() -> str:
    return f"{uuid.uuid4().hex[:12]}"

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Очікує JSON:
      {
        "user_id": 244142655,
        "amount": 100,               # UAH
        "currency": "UAH",           # опц.
        "description": "100 UAH topup"  # опц.
      }
    Повертає data/signature/checkout_url для LiqPay.
    """
    body = await req.json()

    user_id = body.get("user_id")
    if user_id is None:
        return JSONResponse({"ok": False, "error": "user_id is required"}, status_code=422)

    amount = float(body.get("amount", 100))
    currency = body.get("currency", "UAH")
    description = body.get("description", f"{int(amount)} UAH topup")
    order_id = body.get("order_id") or make_order_id()

    params = {
        "version": 3,
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": amount,
        "currency": currency,
        "description": description,
        "order_id": f"{user_id}-{order_id}",  # зшиваємо з user_id для надійності
        "result_url": RESULT_URL,
        "server_url": SERVER_URL,
        "sandbox": 1,                 # прибрати у проді
        "info": str(user_id),         # ← обов’язково! щоб прийшов у callback
    }

    data_b64 = build_data(params)
    signature = sign(data_b64)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    resp = {
        "ok": True,
        "order_id": params["order_id"],
        "public_key": PUBLIC_KEY,
        "data": data_b64,
        "signature": signature,
        "checkout_url": checkout_url,
    }
    log.info("Invoice created: %s", resp["order_id"])
    return JSONResponse(resp)

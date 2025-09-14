import os
import uuid
import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from dotenv import load_dotenv

# === INIT ===
load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payments-api")

app = FastAPI(title="Payments API (LiqPay)")

# === LiqPay utils (твій готовий модуль) ===
from payments.liqpay_utils import build_data, sign, PUBLIC_KEY
from dao import charge  # твоя функція для зарахування кредитів

# URLs для LiqPay
LIQPAY_SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")
LIQPAY_RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")


def make_liqpay_order_id() -> str:
    """Генерує унікальний order_id для LiqPay"""
    return uuid.uuid4().hex[:12]


# === 1. Створення платежу (бот звертається сюди) ===
@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Створює платіж LiqPay і повертає data + signature для checkout.
    """
    body = await req.json()
    user_id = body.get("user_id")
    if not user_id:
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

    return {
        "ok": True,
        "provider": "liqpay",
        "order_id": order_id,
        "data": data,
        "signature": signature
    }


# === 2. Callback від LiqPay ===
@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    LiqPay надсилає data + signature (base64).
    Тут треба перевірити підпис і зарахувати кредити.
    """
    try:
        body = await req.json()
    except Exception:
        body = await req.form()

    log.info(f"LiqPay callback: {dict(body)}")

    # TODO: тут розпарсити `body["data"]`, перевірити підпис через sign()
    # Наприклад:
    # decoded = decode_data(body["data"])
    # if body["signature"] == sign(body["data"]) and decoded["status"] == "success":
    #     user_id = ...  # треба визначити з order_id чи description
    #     charge(user_id, int(decoded["amount"]))

    return JSONResponse({"ok": True})

@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    return """
    <html><body style="font-family:system-ui">
      <h1>✅ Оплату отримано</h1>
      <p>Дякуємо! Тепер можете повернутися в бот.</p>
    </body></html>
    """

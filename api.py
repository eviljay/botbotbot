import os, json, base64, hashlib
from fastapi import FastAPI, Form, Request, HTTPException
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()

LIQPAY_PRIVATE = os.getenv("LIQPAY_PRIVATE")

app = FastAPI()

def b64(s: bytes) -> str:
    return base64.b64encode(s).decode("utf-8")

def liqpay_signature(data_b64: str) -> str:
    raw = (LIQPAY_PRIVATE + data_b64 + LIQPAY_PRIVATE).encode("utf-8")
    import hashlib
    return b64(hashlib.sha1(raw).digest())

def decode_data(data_b64: str) -> dict:
    raw = base64.b64decode(data_b64)
    return json.loads(raw)

@app.post("/liqpay/callback")
async def liqpay_callback(data: str = Form(...), signature: str = Form(...)):
    # Перевірка підпису
    expected = liqpay_signature(data)
    if signature != expected:
        raise HTTPException(status_code=400, detail="Bad signature")

    payload = decode_data(data)
    # корисні поля: status, order_id, amount, currency, description, transaction_id, paytype ...
    status = payload.get("status")
    order_id = payload.get("order_id")

    # Обробка статусів: success / sandbox / wait_accept / failure / error / reversed …
    if status in ("success", "sandbox"):
        # TODO: нарахуй кредити користувачу за order_id,
        # перевір чи не оброблявся цей order_id раніше (idempotency)
        # збережи запис у БД / файл-лог
        pass
    elif status in ("failure", "error"):
        # TODO: логування/повідомлення
        pass

    # LiqPay очікує просто 200 OK. Можеш повернути "ok".
    return {"ok": True}

@app.get("/thanks")
def thanks():
    return {"message": "Дякуємо! Якщо оплата пройшла, кредити будуть нараховані протягом хвилини."}

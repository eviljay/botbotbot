
from fastapi import FastAPI
from pydantic import BaseModel
import uuid
from payments.liqpay_utils import build_data, PUBLIC_KEY

app = FastAPI()

class InvoiceIn(BaseModel):
    user_id: int
    amount: float
    description: str = "Credits package"

@app.post("/api/payments/invoice")
def create_invoice(body: InvoiceIn):
    order_id = f"{body.user_id}-{uuid.uuid4().hex[:12]}"
    data_b64, signature = build_data(order_id, body.amount, body.description)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"
    return {
        "order_id": order_id,
        "public_key": PUBLIC_KEY,
        "data": data_b64,
        "signature": signature,
        "checkout_url": checkout_url
    }

# 🔁 Додаємо сумісний шлях, який очікує бот
@app.post("/api/payments/create")
def create_payment(body: InvoiceIn):
    return create_invoice(body)

@app.get("/healthz")
def healthz():
    return {"ok": True}

# /root/mybot/payments_api.py
from typing import Optional, Union
from decimal import Decimal, InvalidOperation
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from payments.liqpay_utils import build_data, PUBLIC_KEY

app = FastAPI()

class InvoiceIn(BaseModel):
    # дозволяємо і int, і str
    user_id: Union[int, str]
    # дозволяємо і float/Decimal, і str
    amount: Union[float, Decimal, str]
    description: Optional[str] = "Credits package"

def _normalize_amount(amount_in) -> Decimal:
    try:
        return Decimal(str(amount_in))
    except InvalidOperation:
        raise HTTPException(status_code=400, detail="Invalid amount")

@app.post("/api/payments/invoice")
def create_invoice(body: InvoiceIn):
    user_id_str = str(body.user_id)
    amount_dec = _normalize_amount(body.amount)
    description = body.description or "Credits package"

    order_id = f"{user_id_str}-{uuid.uuid4().hex[:12]}"
    data_b64, signature = build_data(order_id, float(amount_dec), description)
    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"
    return {
        "order_id": order_id,
        "public_key": PUBLIC_KEY,
        "data": data_b64,
        "signature": signature,
        "checkout_url": checkout_url
    }

# АЛІАС під твій бот
@app.post("/api/payments/create")
def create_payment(body: InvoiceIn):
    return create_invoice(body)

@app.get("/healthz")
def healthz():
    return {"ok": True}
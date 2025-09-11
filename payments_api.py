import uuid
from decimal import Decimal, InvalidOperation
from typing import Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# твій модуль для роботи з LiqPay
from payments.liqpay_utils import build_data, PUBLIC_KEY

app = FastAPI(title="Payments API", version="1.0.0")


# --------- Моделі ---------
class InvoiceIn(BaseModel):
    user_id: Union[int, str]
    amount: Union[str, float, Decimal]
    description: Optional[str] = "Credits package"


# --------- Утиліти ---------
def _normalize_amount(amount_in) -> Decimal:
    try:
        return Decimal(str(amount_in))
    except (InvalidOperation, ValueError, TypeError):
        raise HTTPException(status_code=400, detail=f"Invalid amount: {amount_in!r}")


def _make_order_id(user_id: Union[int, str]) -> str:
    return f"{user_id}-{uuid.uuid4().hex[:12]}"


# --------- Ендпоінти ---------
@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/api/payments/invoice")
def create_invoice(body: InvoiceIn):
    user_id_str = str(body.user_id)
    amount = _normalize_amount(body.amount)
    description = body.description or "Credits package"

    order_id = _make_order_id(user_id_str)
    data_b64, signature = build_data(order_id, float(amount), description)

    checkout_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    return {
        "order_id": order_id,
        "public_key": PUBLIC_KEY,
        "data": data_b64,
        "signature": signature,
        "checkout_url": checkout_url,
    }


# Аліас для коду бота (щоб не міняти bot.py)
@app.post("/api/payments/create")
def create_payment(body: InvoiceIn):
    return create_invoice(body)

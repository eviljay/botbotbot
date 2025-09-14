# payments_api.py
import os
import json
import base64
import hashlib
import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from dotenv import load_dotenv

# ====== Логи ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("payments-api")

# ====== ENV ======
load_dotenv()

PUBLIC_KEY  = os.getenv("LIQPAY_PUBLIC_KEY", "")
PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")
RESULT_URL  = os.getenv("LIQPAY_RESULT_URL", "")   # наприклад: https://server1.seoswiss.online/thanks
SERVER_URL  = os.getenv("LIQPAY_SERVER_URL", "")   # наприклад: https://server1.seoswiss.online/liqpay/callback
DEFAULT_CCY = os.getenv("LIQPAY_CURRENCY", "UAH")

if not PUBLIC_KEY or not PRIVATE_KEY:
    raise RuntimeError("Set LIQPAY_PUBLIC_KEY and LIQPAY_PRIVATE_KEY in .env")

# ====== FastAPI ======
app = FastAPI(title="Payments API (LiqPay)")

# ====== Утиліти ======
def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def _liqpay_encode(payload: dict) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _b64(raw)

def _liqpay_sign(data_b64: str) -> str:
    # signature = base64( sha1( private_key + data + private_key ) )
    to_sign = (PRIVATE_KEY + data_b64 + PRIVATE_KEY).encode("utf-8")
    digest = hashlib.sha1(to_sign).digest()
    return _b64(digest)

# ====== API ======
@app.get("/health")
async def health():
    return {"ok": True, "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/api/payments/create")
async def create_payment(req: Request):
    """
    Body:
    { "user_id": 244142655, "amount": 100, "currency": "UAH" }
    Повертає прямий LiqPay URL у полі pay_url.
    """
    body = await req.json()
    user_id = body.get("user_id")
    amount  = body.get("amount")
    currency = (body.get("currency") or DEFAULT_CCY).upper()

    if not user_id or not amount:
        raise HTTPException(400, "user_id and amount required")

    order_id = body.get("order_id") or _gen_order_id(user_id)

    payload = {
        "version": "3",
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": f"{float(amount):.2f}",
        "currency": currency,
        "description": f"Top-up {amount} credits",
        "order_id": order_id,
        "server_url": SERVER_URL,
        "result_url": RESULT_URL,
        # за потреби: "sandbox": "1",
    }

    data_b64   = _liqpay_encode(payload)
    signature  = _liqpay_sign(data_b64)
    # Пряме посилання на checkout-сторінку LiqPay:
    pay_url = f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={signature}"

    log.info("Create payment: user=%s amount=%s %s order_id=%s pay_url=%s",
             user_id, amount, currency, order_id, pay_url)

    return JSONResponse({
        "ok": True,
        "provider": "liqpay",
        "order_id": order_id,
        "pay_url": pay_url,          # <-- бот підставляє це в кнопку
        # Якщо хочеш — можеш також повертати data/signature (не обов'язково для бота)
        # "data": data_b64,
        # "signature": signature,
    })

@app.post("/liqpay/callback")
async def liqpay_callback(req: Request):
    """
    Серверний колбек від LiqPay. Приходять form-data: data, signature.
    Тут перевіряємо підпис і оновлюємо стан/баланс у БД.
    """
    form = await req.form()
    data_b64  = form.get("data")
    sign_recv = form.get("signature")

    if not data_b64 or not sign_recv:
        raise HTTPException(400, "Missing data or signature")

    sign_calc = _liqpay_sign(data_b64)
    if sign_calc != sign_recv:
        log.warning("Invalid signature callback")
        raise HTTPException(400, "Invalid signature")

    payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
    log.info("LiqPay callback payload: %s", payload)

    # TODO: оновити баланс користувача відповідно до status (success, failure тощо)
    # status = payload.get("status")  # success, failure, sandbox, etc.
    # order_id = payload.get("order_id")
    # amount = payload.get("amount")

    return JSONResponse({"ok": True})

@app.get("/thanks", response_class=HTMLResponse)
async def thanks_page():
    return """
    <html><body style="font-family:system-ui">
      <h1>✅ Оплату отримано</h1>
      <p>Дякуємо! Тепер можете повернутися в бот.</p>
    </body></html>
    """

# ====== helpers ======
def _gen_order_id(user_id) -> str:
    # короткий унікальний id
    return f"{user_id}-{os.urandom(6).hex()}"

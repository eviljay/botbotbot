# /root/mybot/api.py
import json
import logging
from typing import Optional

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, PlainTextResponse

# беремо перевірку підпису з твого модуля
from payments.liqpay_utils import verify_signature

logger = logging.getLogger("mybot-api")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="MyBot Public API", version="1.0.0")


@app.get("/thanks", response_class=HTMLResponse)
async def thanks():
    # проста сторінка "Дякуємо"
    return """
    <!doctype html>
    <html><head><meta charset="utf-8"><title>Дякуємо</title></head>
    <body style="font-family: system-ui; text-align:center; padding: 3rem;">
      <h1>Дякуємо за оплату! ✅</h1>
      <p>Оплату отримано. Можете повернутися до бота.</p>
    </body></html>
    """


@app.post("/liqpay/callback")
async def liqpay_callback(
    request: Request,
    data: Optional[str] = Form(None),
    signature: Optional[str] = Form(None),
):
    """
    LiqPay надсилає POST з полями data (base64) і signature.
    Ми валідуємо підпис і логуємо payload. Тут же можна робити нарахування кредитів.
    """
    # Підтримуємо і JSON-варіант на всякий випадок
    if data is None or signature is None:
        try:
            body = await request.json()
            data = data or body.get("data")
            signature = signature or body.get("signature")
        except Exception:
            pass

    if not data or not signature:
        logger.warning("Callback: missing data or signature")
        return PlainTextResponse("missing fields", status_code=400)

    # Перевіряємо підпис
    if not verify_signature(data, signature):
        logger.warning("Callback: bad signature")
        return PlainTextResponse("bad signature", status_code=403)

    # Розпаковуємо payload (base64 → json)
    try:
        import base64
        decoded = base64.b64decode(data).decode("utf-8")
        payload = json.loads(decoded)
    except Exception as e:
        logger.exception("Callback: failed to decode payload")
        return PlainTextResponse("bad data", status_code=400)

    # Тут робиш idempotency + нарахування кредитів
    # order_id = payload.get("order_id")
    # status = payload.get("status")
    # amount = payload.get("amount")
    logger.info("LiqPay callback OK: %s", payload)

    # LiqPay очікує 200 OK
    return {"ok": True}

# payments_api.py
import os
import time
import hmac
import hashlib

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import httpx
from dotenv import load_dotenv  # ← додали

from dao import init_db, insert_payment, update_payment_status, find_payment, add_balance

# підтягуємо .env з поточної директорії
load_dotenv()

def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

# === WayForPay config ===
WFP_API_URL      = "https://api.wayforpay.com/api"
MERCHANT_ACCOUNT = require_env("WFP_MERCHANT_ACCOUNT")
MERCHANT_DOMAIN  = require_env("WFP_MERCHANT_DOMAIN")
WFP_SECRET       = require_env("WFP_SECRET")
SERVICE_URL      = require_env("WFP_SERVICE_URL")  # публічний HTTPS URL на цей бекенд
CURRENCY         = "UAH"

# === Credits pricing ===
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))  # 1 кредит = 5 грн

# (опціонально) повідомляти користувача у Telegram після зарахування
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

app = FastAPI()
init_db()


def hmac_md5(s: str, key: str) -> str:
    return hmac.new(key.encode("utf-8"), s.encode("utf-8"), hashlib.md5).hexdigest()


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/api/payments/create")
async def create_invoice(payload: dict):
    """
    Очікує: {"user_id": <int>, "amount_uah": <float|int>}
    Повертає: {"invoiceUrl": "...", "orderReference": "...", "credits": <int>}
    """
    try:
        user_id = int(payload["user_id"])
        amount  = float(payload["amount_uah"])
    except Exception:
        raise HTTPException(status_code=400, detail="Bad payload")

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be > 0")

    credits = int(amount // CREDIT_PRICE_UAH)
    if credits < 1:
        raise HTTPException(status_code=400, detail="Amount too small for credits")

    order_ref  = f"TG{user_id}-{int(time.time())}"
    order_date = int(time.time())

    product_name  = [f"TG Credits ({credits})"]
    product_count = [1]
    product_price = [amount]

    base_str = ";".join([
        MERCHANT_ACCOUNT,
        MERCHANT_DOMAIN,
        order_ref,
        str(order_date),
        f"{amount:.2f}",
        CURRENCY,
        *product_name,
        *[str(c) for c in product_count],
        *[f"{p:.2f}" for p in product_price],
    ])
    signature = hmac_md5(base_str, WFP_SECRET)

    req = {
        "transactionType": "CREATE_INVOICE",
        "merchantAccount": MERCHANT_ACCOUNT,
        "merchantAuthType": "SimpleSignature",
        "merchantDomainName": MERCHANT_DOMAIN,
        "merchantSignature": signature,
        "apiVersion": 1,
        "serviceUrl": SERVICE_URL,
        "orderReference": order_ref,
        "orderDate": order_date,
        "amount": amount,
        "currency": CURRENCY,
        "productName": product_name,
        "productPrice": product_price,
        "productCount": product_count,
        "clientAccountId": str(user_id),
        "language": "UA",
    }

    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(WFP_API_URL, json=req)
        r.raise_for_status()
        data = r.json()

    invoice_url = data.get("invoiceUrl")
    if not invoice_url:
        raise HTTPException(status_code=400, detail=f"WayForPay error: {data}")

    insert_payment(user_id, "wayforpay", order_ref, int(amount * 100), CURRENCY, "new", data)
    return {"invoiceUrl": invoice_url, "orderReference": order_ref, "credits": credits}


@app.post("/wfp/callback")
async def wfp_callback(request: Request):
    """
    Вебхук від WayForPay.
    Перевіряємо підпис, оновлюємо статус платежу.
    На Approved — зараховуємо кредити за формулою floor(amount/CREDIT_PRICE_UAH).
    """
    body = await request.json()

    # Перевірка підпису колбека:
    # merchantAccount;orderReference;amount;currency;authCode;cardPan;transactionStatus;reasonCode
    check_str = ";".join([
        body.get("merchantAccount", ""),
        body.get("orderReference", ""),
        str(body.get("amount", "")),
        body.get("currency", ""),
        body.get("authCode", ""),
        body.get("cardPan", ""),
        body.get("transactionStatus", ""),
        str(body.get("reasonCode", "")),
    ])
    calc_sig = hmac_md5(check_str, WFP_SECRET)
    if calc_sig != body.get("merchantSignature"):
        raise HTTPException(status_code=400, detail="Bad signature")

    order_ref = body.get("orderReference")
    status    = body.get("transactionStatus")
    ok        = (status == "Approved")

    p = find_payment(order_ref)
    if not p:
        raise HTTPException(status_code=404, detail="Payment not found")
    user_id, amount_cents, currency, old_status = p

    if ok:
        update_payment_status(order_ref, "paid", body)
        paid_uah = amount_cents / 100.0
        credits = int(paid_uah // CREDIT_PRICE_UAH)
        if credits > 0:
            add_balance(int(user_id), int(credits))
        if BOT_TOKEN:
            try:
                async with httpx.AsyncClient(timeout=10) as c:
                    await c.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": int(user_id), "text": f"✅ Баланс поповнено на {credits} кредитів."}
                    )
            except Exception:
                pass
    else:
        update_payment_status(order_ref, "declined", body)

    resp_time = int(time.time())
    resp_status = "accept"
    resp_sig = hmac_md5(";".join([order_ref, resp_status, str(resp_time)]), WFP_SECRET)
    return JSONResponse({"orderReference": order_ref, "status": resp_status, "time": resp_time, "signature": resp_sig})

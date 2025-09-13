# payments/wayforpay_utils.py
import os, time, uuid, hmac, hashlib, httpx
from typing import Tuple, Dict, Any

MERCHANT = os.getenv("WAYFORPAY_MERCHANT_ACCOUNT", "")
DOMAIN   = os.getenv("WAYFORPAY_DOMAIN", "")
SECRET   = os.getenv("WAYFORPAY_SECRET_KEY", "")
CURRENCY = os.getenv("WAYFORPAY_CURRENCY", "UAH")
SERVICE_URL = os.getenv("WAYFORPAY_SERVICE_URL", "")
RETURN_URL  = os.getenv("WAYFORPAY_RETURN_URL", "")

def enabled() -> bool:
    return os.getenv("WAYFORPAY_ENABLED", "0") == "1" and MERCHANT and SECRET and DOMAIN

def _hmac_md5(s: str) -> str:
    return hmac.new(SECRET.encode("utf-8"), s.encode("utf-8"), hashlib.md5).hexdigest()

def build_purchase_fields(*, order_ref: str, amount: float, product_name: str) -> Dict[str, Any]:
    # обов'язкові поля для "Accept payment (Purchase)"
    order_date = int(time.time())
    fields = {
        "merchantAccount": MERCHANT,
        "merchantAuthType": "simpleSignature",
        "merchantDomainName": DOMAIN,
        "orderReference": order_ref,
        "orderDate": order_date,
        "amount": f"{amount:.2f}",
        "currency": CURRENCY,
        "productName[]": [product_name],
        "productPrice[]": [f"{amount:.2f}"],
        "productCount[]": ["1"],
        "serviceUrl": SERVICE_URL,
        "returnUrl": RETURN_URL,
        "language": "UA",
        # опційно можна обмежити спосіб на сторінці:
        # "defaultPaymentSystem": "card",
    }
    # формування підпису строго у визначеному порядку:
    base = ";".join([
        fields["merchantAccount"],
        fields["merchantDomainName"],
        fields["orderReference"],
        str(fields["orderDate"]),
        fields["amount"],
        fields["currency"],
        product_name,    # productName[0]
        "1",             # productCount[0]
        f"{amount:.2f}", # productPrice[0]
    ])
    fields["merchantSignature"] = _hmac_md5(base)
    return fields

async def create_payment_link(fields: Dict[str, Any]) -> str:
    """
    Режим behavior=offline повертає JSON з url платежу.
    """
    async with httpx.AsyncClient(timeout=15.0) as client:
        # важливо: відправляємо ті ж поля, плюс behavior=offline
        data = fields.copy()
        data["behavior"] = "offline"
        r = await client.post("https://secure.wayforpay.com/pay", data=data)
        r.raise_for_status()
        js = r.json()
        # очікувано {"url": "https://secure.wayforpay.com/page?Vkh=..."}
        return js["url"]

def make_order_ref(user_id: int, amount: float) -> str:
    # інкапсулюємо user_id в orderReference, щоб у callback легко ідентифікувати
    return f"{user_id}-{int(amount)}-{uuid.uuid4().hex[:8]}"

def verify_callback_signature(payload: Dict[str, Any]) -> bool:
    """
    Для serviceUrl підтвердження робиться HMAC_MD5 по:
    merchantAccount;orderReference;amount;currency;authCode;cardPan;transactionStatus;reasonCode
    """
    keys = ["merchantAccount","orderReference","amount","currency","authCode",
            "cardPan","transactionStatus","reasonCode"]
    base = ";".join(str(payload.get(k, "")) for k in keys)
    expected = _hmac_md5(base)
    return expected == payload.get("merchantSignature")

def build_ack(order_ref: str) -> Dict[str, Any]:
    """
    Відповідь мерчанта: підпис по orderReference;status;time
    """
    status = "accept"
    t = int(time.time())
    base = ";".join([order_ref, status, str(t)])
    return {"orderReference": order_ref, "status": status, "time": t,
            "signature": _hmac_md5(base)}

import json, base64, hashlib, os

LIQPAY_PUBLIC = os.getenv("LIQPAY_PUBLIC")
LIQPAY_PRIVATE = os.getenv("LIQPAY_PRIVATE")
LIQPAY_SANDBOX = int(os.getenv("LIQPAY_SANDBOX", "1"))
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")

def b64(s: bytes) -> str:
    return base64.b64encode(s).decode("utf-8")

def liqpay_signature(data_b64: str) -> str:
    # signature = base64( sha1(private_key + data + private_key) )
    raw = (LIQPAY_PRIVATE + data_b64 + LIQPAY_PRIVATE).encode("utf-8")
    sha1 = hashlib.sha1(raw).digest()
    return b64(sha1)

def make_checkout_data(order_id: str, amount: float, description: str, currency="UAH", action="pay"):
    payload = {
        "version": 3,
        "public_key": LIQPAY_PUBLIC,
        "action": action,               # 'pay'
        "amount": str(amount),
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "sandbox": LIQPAY_SANDBOX,
        # LiqPay надсилатиме POST сюди (потрібен https)
        "server_url": f"{BASE_URL}/liqpay/callback",
        # Куди повертати юзера після оплати (опційно, можна зробити свою сторінку "Дякуємо")
        "result_url": f"{BASE_URL}/thanks"
    }
    data_b64 = b64(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    sign = liqpay_signature(data_b64)
    return data_b64, sign

def make_checkout_url(order_id: str, amount: float, description: str):
    data_b64, sign = make_checkout_data(order_id, amount, description)
    return f"https://www.liqpay.ua/api/3/checkout?data={data_b64}&signature={sign}"

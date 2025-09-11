import base64
import hashlib
import json
import os

PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC")
PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE")
SANDBOX = int(os.getenv("LIQPAY_SANDBOX", "1"))
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def sign_string(data_b64: str) -> str:
    raw = (PRIVATE_KEY + data_b64 + PRIVATE_KEY).encode("utf-8")
    sha1 = hashlib.sha1(raw).digest()
    return _b64(sha1)

def build_data(order_id: str, amount: float, description: str, currency="UAH", action="pay"):
    payload = {
        "version": 3,
        "public_key": PUBLIC_KEY,
        "action": action,
        "amount": str(amount),
        "currency": currency,
        "description": description,
        "order_id": order_id,
        "sandbox": SANDBOX,
        "server_url": f"{BASE_URL}/liqpay/callback",
        "result_url": f"{BASE_URL}/thanks"
    }
    data_b64 = _b64(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    signature = sign_string(data_b64)
    return data_b64, signature

def verify_signature(data_b64: str, signature: str) -> bool:
    return signature == sign_string(data_b64)

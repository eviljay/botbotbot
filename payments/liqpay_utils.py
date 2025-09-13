import os
import json
import base64
import hashlib
from dotenv import load_dotenv

load_dotenv()

PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "")
PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")

def build_data(params: dict) -> str:
    """
    Готує base64(data) для LiqPay checkout
    """
    data_str = json.dumps(params, separators=(",", ":"), ensure_ascii=False)
    return base64.b64encode(data_str.encode("utf-8")).decode("utf-8")

def sign(data_b64: str) -> str:
    """
    Підпис = base64( sha1( PRIVATE_KEY + data + PRIVATE_KEY ) )
    """
    raw = PRIVATE_KEY + data_b64 + PRIVATE_KEY
    sha1 = hashlib.sha1(raw.encode("utf-8")).digest()
    return base64.b64encode(sha1).decode("utf-8")

def verify_signature(data_b64: str, signature: str) -> bool:
    """
    Перевірка коректності підпису
    """
    return signature == sign(data_b64)

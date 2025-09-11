import base64
import json
import hashlib
import os

PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "")
PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "")

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")

def build_data(params: dict) -> str:
    # JSON без пробілів
    payload = json.dumps(params, separators=(",", ":")).encode("utf-8")
    return _b64(payload)

def sign(data_b64: str) -> str:
    raw = (PRIVATE_KEY + data_b64 + PRIVATE_KEY).encode("utf-8")
    return _b64(hashlib.sha1(raw).digest())

def verify_signature(data_b64: str, signature: str) -> bool:
    return sign(data_b64) == signature
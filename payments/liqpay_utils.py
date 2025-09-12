# payments/liqpay_utils.py
import os
import time
import json
import base64
import hashlib
import uuid
from typing import Dict, Any, Optional

LIQPAY_CHECKOUT = "https://www.liqpay.ua/api/3/checkout"

PUBLIC_KEY = os.getenv("LIQPAY_PUBLIC_KEY", "").strip()
PRIVATE_KEY = os.getenv("LIQPAY_PRIVATE_KEY", "").strip()

def make_order_id() -> str:
    return uuid.uuid4().hex[:12]

def _b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")

def build_data(params: Dict[str, Any]) -> str:
    """
    Обов'язкові для LiqPay:
      public_key, version=3, action=pay, amount, currency, description, order_id,
      (опційно) result_url, server_url, language
    """
    # Без пробілів та з ensure_ascii=False (щоб українська збереглась)
    return _b64(json.dumps(params, ensure_ascii=False, separators=(",", ":")))

def sign(data_b64: str) -> str:
    """
    signature = base64( sha1( private_key + data + private_key ) )
    """
    raw = PRIVATE_KEY + data_b64 + PRIVATE_KEY
    digest = hashlib.sha1(raw.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("utf-8")

def build_checkout_link(
    amount: float,
    currency: str,
    description: str,
    order_id: str,
    result_url: Optional[str],
    server_url: Optional[str],
    language: str = "uk",
) -> Dict[str, str]:
    if not PUBLIC_KEY or not PRIVATE_KEY:
        raise RuntimeError("LIQPAY_PUBLIC_KEY / LIQPAY_PRIVATE_KEY are not set")

    params = {
        "public_key": PUBLIC_KEY,
        "version": 3,
        "action": "pay",
        "amount": float(f"{amount:.2f}"),
        "currency": currency,
        "description": description[:255],
        "order_id": order_id,
        "language": language,
    }
    if result_url:
        params["result_url"] = result_url
    if server_url:
        params["server_url"] = server_url

    data_b64 = build_data(params)
    sig = sign(data_b64)
    checkout_url = f"{LIQPAY_CHECKOUT}?data={data_b64}&signature={sig}"
    return {"data": data_b64, "signature": sig, "checkout_url": checkout_url}

def verify_callback_signature(data_b64: str, signature: str) -> bool:
    """
    На server_url LiqPay шле form-data: data, signature.
    Перевірка: sign(data) == signature
    """
    expected = sign(data_b64)
    return expected == signature

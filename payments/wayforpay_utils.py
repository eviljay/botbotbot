# payments/wayforpay_utils.py
import time
import hmac
import hashlib
import httpx
from typing import List, Dict, Any, Optional

WFP_API = "https://api.wayforpay.com/api"

def _hmac_md5(message: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.md5).hexdigest()

def _join(values: List[Any]) -> str:
    return ";".join("" if v is None else str(v) for v in values)

def build_create_invoice_payload(
    merchant_account: str,
    secret_key: str,
    merchant_domain: str,
    order_reference: str,
    amount: float,
    currency: str,
    product_names: List[str],
    product_prices: List[float],
    product_counts: List[int],
    service_url: Optional[str] = None,
    return_url: Optional[str] = None,
    language: str = "UA",
    api_version: int = 1,
    payment_systems: Optional[str] = None,
    client_first_name: Optional[str] = None,
    client_last_name: Optional[str] = None,
    client_email: Optional[str] = None,
    client_phone: Optional[str] = None,
) -> Dict[str, Any]:
    order_date = int(time.time())
    sign_parts: List[Any] = [
        merchant_account,
        merchant_domain,
        order_reference,
        order_date,
        f"{amount:.2f}",
        currency,
    ]
    sign_parts.extend(product_names)
    sign_parts.extend(product_counts)
    sign_parts.extend([f"{p:.2f}" for p in product_prices])

    signature_source = _join(sign_parts)
    merchant_signature = _hmac_md5(signature_source, secret_key)

    payload: Dict[str, Any] = {
        "transactionType": "CREATE_INVOICE",
        "merchantAccount": merchant_account,
        "merchantAuthType": "SimpleSignature",
        "merchantDomainName": merchant_domain,
        "merchantSignature": merchant_signature,
        "apiVersion": api_version,
        "language": language,
        "orderReference": order_reference,
        "orderDate": order_date,
        "amount": float(f"{amount:.2f}"),
        "currency": currency,
        "productName": product_names,
        "productPrice": [float(f"{p:.2f}") for p in product_prices],
        "productCount": product_counts,
    }
    if service_url:
        payload["serviceUrl"] = service_url
    if return_url:
        payload["returnUrl"] = return_url
    if payment_systems:
        payload["paymentSystems"] = payment_systems
    if client_first_name:
        payload["clientFirstName"] = client_first_name
    if client_last_name:
        payload["clientLastName"] = client_last_name
    if client_email:
        payload["clientEmail"] = client_email
    if client_phone:
        payload["clientPhone"] = client_phone

    return payload

async def create_invoice_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(WFP_API, json=payload)
        r.raise_for_status()
        return r.json()

def verify_callback_signature(data: Dict[str, Any], secret_key: str) -> bool:
    required = [
        "merchantAccount","orderReference","amount","currency",
        "authCode","cardPan","transactionStatus","reasonCode",
        "merchantSignature",
    ]
    if any(k not in data for k in required):
        return False
    base = _join([
        data.get("merchantAccount"),
        data.get("orderReference"),
        data.get("amount"),
        data.get("currency"),
        data.get("authCode"),
        data.get("cardPan"),
        data.get("transactionStatus"),
        data.get("reasonCode"),
    ])
    expected = _hmac_md5(base, secret_key)
    return expected == data.get("merchantSignature")

def build_accept_response(order_reference: str, secret_key: str) -> Dict[str, Any]:
    now = int(time.time())
    status = "accept"
    src = _join([order_reference, status, now])
    sig = _hmac_md5(src, secret_key)
    return {"orderReference": order_reference, "status": status, "time": now, "signature": sig}

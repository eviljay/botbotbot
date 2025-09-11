# bot.py
import os
import csv
import io
import logging

from dotenv import load_dotenv
from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
from httpx import HTTPError, AsyncClient, ConnectError
from liqpay_utils import make_checkout_url

from dataforseo import DataForSEO
from dao import init_db, ensure_user, get_balance, charge, get_phone, register_or_update_phone

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# === ENV ===
load_dotenv()
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DFS_LOGIN = os.environ["DATAFORSEO_LOGIN"]
DFS_PASS = os.environ["DATAFORSEO_PASSWORD"]
DFS_BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")
BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")

PRICE_10 = int(os.getenv("PRICE_10", "5"))
PRICE_ALL = int(os.getenv("PRICE_ALL", "20"))
INITIAL_BONUS = int(os.getenv("INITIAL_BONUS", "10"))
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))
TOPUP_OPTIONS = [int(x.strip()) for x in os.getenv("TOPUP_OPTIONS", "100,250,500").split(",") if x.strip().isdigit()]

# === INIT ===
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE)

async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # Генеруємо order_id (унікальний)
    order_id = f"{user_id}-{uuid.uuid4().hex[:12]}"
    amount = 99.00
    description = "100 credits package"

    pay_url = make_checkout_url(order_id, amount, description)
    keyboard = [[InlineKeyboardButton("Оплатити через LiqPay", url=pay_url)]]
    await update.message.reply_text(
        f"Пакет: {description}\nСума: {amount} UAH\nOrder: {order_id}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
# === HELPERS ===
def _extract_items(resp: dict) -> list[dict]:
    tasks = resp.get("tasks") or []
    if not tasks:
        return []
    t = tasks[0] or {}
    if t.get("status_code") and t["status_code"] != 20000:
        raise RuntimeError(t.get("status_message") or f"Task error: {t.get('status_code')}")
    res = t.get("result") or []
    if not res:
        return []
    return res[0].get("items") or []

def _fmt_preview(items: list[dict], cap: int) -> str:
    lines = []
    for it in items[:cap]:
        url_from = (it.get("page_from") or {}).get("url_from") or it.get("url_from")
        anchor = (it.get("anchor") or "").strip()
        first_seen = it.get("first_seen")
        lines.append(f"• {url_from}\n  anchor: {anchor[:80]} | first_seen: {first_seen}")
    return "\n".join(lines)
import os, uuid, base64, json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from payments.liqpay_utils import build_data, sign, verify_signature, PUBLIC_KEY
from payments.pricing import calc_credits_from_amount

app = FastAPI()

SERVER_URL = os.getenv("LIQPAY_SERVER_URL", "")
RESULT_URL = os.getenv("LIQPAY_RESULT_URL", "")

def make_order_id() -> str:
    return "ord_" + uuid.uuid4().hex[:16]

@app.post("/api/payments/create")
async def create_payment(req: Request):
    body = await req.json()
    amount   = float(body.get("amount", 99))
    currency = body.get("currency", "UAH")
    package  = body.get("package", "credits_100")
    user_id  = body.get("user_id")
    order_id = body.get("order_id") or make_order_id()

    params = {
        "version": 3,
        "public_key": PUBLIC_KEY,
        "action": "pay",
        "amount": amount,
        "currency": currency,
        "description": f"Buy credits: {package}",
        "order_id": order_id,
        "result_url": RESULT_URL,
        "server_url": SERVER_URL,
        "sandbox": 1,               # прибрати у проді
        "info": str(user_id or ""),
    }

    data_b64 = build_data(params)
    signature = sign(data_b64)

    return JSONResponse({"data": data_b64, "signature": signature, "order_id": order_id})

@app.post("/api/payments/callback")
async def liqpay_callback(req: Request):
    form = await req.form()
    data_b64 = form.get("data")
    signature = form.get("signature")

    if not data_b64 or not signature:
        raise HTTPException(status_code=400, detail="Missing data/signature")

    if not verify_signature(data_b64, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))

    status   = payload.get("status")
    order_id = payload.get("order_id")
    amount   = float(payload.get("amount", 0))
    user_id  = payload.get("info")

    # Лог транзакції
    print("TX CALLBACK:", payload)

    if status in ("success", "sandbox"):
        credits = calc_credits_from_amount(amount)
        print(f"✅ Нарахувати {credits} кредитів користувачу {user_id}")

    return JSONResponse({"ok": True})


def _items_to_csv_bytes(items: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["url_from", "url_to", "anchor", "dofollow", "first_seen", "last_seen", "domain_from"])
    for it in items:
        writer.writerow([
            (it.get("page_from") or {}).get("url_from") or it.get("url_from"),
            it.get("url_to"),
            (it.get("anchor") or "").replace("\n", " ").strip(),
            it.get("dofollow"),
            it.get("first_seen"),
            it.get("last_visited"),
            it.get("domain_from")
        ])
    return buf.getvalue().encode()

# === HANDLERS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg_line = "Статус реєстрації: ✅ телефон додано" if phone else "Статус реєстрації: ❌ немає телефону (використайте /register)"
    await update.message.reply_text(
        "Привіт! Я SEO-бот з балансом.\n"
        "Команди:\n"
        "/register — додати номер телефону (новим користувачам бонус)\n"
        "/backlinks <домен> — вибір 10/всі + перегляд/CSV (списання кредитів)\n"
        "/balance — показати баланс\n"
        "/topup — поповнити баланс через WayForPay\n\n"
        f"{reg_line}\nВаш баланс: {bal} кредитів"
    )

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    kb = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
    await update.message.reply_text(
        "Натисніть кнопку, щоб поділитися власним номером телефону:",
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True)
    )

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    contact = update.message.contact
    if not contact:
        return
    if contact.user_id and contact.user_id != uid:
        return await update.message.reply_text(
            "Здається, це не ваш номер. Будь ласка, поділіться саме своїм контактом.",
            reply_markup=ReplyKeyboardRemove()
        )
    is_new, credited = register_or_update_phone(uid, contact.phone_number, initial_bonus=INITIAL_BONUS)
    bal = get_balance(uid)
    if is_new and credited > 0:
        txt = f"✅ Дякуємо за реєстрацію! Нараховано бонус: +{credited} кредитів.\nВаш баланс: {bal}"
    else:
        txt = f"✅ Телефон збережено. Ваш баланс: {bal}"
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardRemove())

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg_line = "✅ телефон додано" if phone else "❌ немає телефону (використайте /register)"
    await update.message.reply_text(f"Баланс: {bal} кредитів\nРеєстрація: {reg_line}")

async def topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    rows = []
    for amount in TOPUP_OPTIONS:
        credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([InlineKeyboardButton(f"Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])
    await update.message.reply_text("Оберіть суму поповнення:", reply_markup=InlineKeyboardMarkup(rows))

async def backlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args:
        return await update.message.reply_text("Приклад: /backlinks yourdomain.com")
    domain = args[0]
    kb = [
        [
            InlineKeyboardButton("👀 Показати 10 останніх", callback_data=f"show|{domain}|10"),
            InlineKeyboardButton("⬇️ CSV (10)", callback_data=f"csv|{domain}|10"),
        ],
        [
            InlineKeyboardButton("👀 Показати всі", callback_data=f"show|{domain}|all"),
            InlineKeyboardButton("⬇️ CSV (всі)", callback_data=f"csv|{domain}|all"),
        ],
    ]
    await update.message.reply_text(
        f"Домен: {domain}\nОбери, що зробити:",
        reply_markup=InlineKeyboardMarkup(kb),
    )

async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    data = query.data.split("|")

    # Поповнення
    if data[0] == "topup":
        amount_uah = int(data[1])
        try:
            async with AsyncClient(timeout=20) as c:
                r = await c.post(
                    f"{BACKEND_BASE}/api/payments/create",
                    json={"user_id": uid, "amount_uah": amount_uah}
                )
                r.raise_for_status()
                resp = r.json()
        except ConnectError:
            return await query.edit_message_text(
                "❌ Бекенд недоступний. Перевір BACKEND_BASE і чи запущений payments_api (порт 8001)."
            )
        url = resp["invoiceUrl"]
        kb = [[InlineKeyboardButton("💳 Оплатити (WayForPay)", url=url)]]
        return await query.edit_message_text(
            f"Рахунок створено на {amount_uah}₴. Натисніть, щоб оплатити:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # Платні дії
    try:
        action, domain, scope = data
    except ValueError:
        return await query.edit_message_text("Невірний запит.")

    cost = PRICE_10 if scope == "10" else PRICE_ALL

    # Списання
    if not charge(uid, cost, domain, scope):
        rows = []
        for amount in TOPUP_OPTIONS:
            credits = int(amount // CREDIT_PRICE_UAH)
            rows.append([InlineKeyboardButton(f"💳 Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])
        return await query.edit_message_text(
            f"Недостатньо кредитів (потрібно {cost}). Поповніть баланс.",
            reply_markup=InlineKeyboardMarkup(rows)
        )

    # DataForSEO
    limit = 10 if scope == "10" else 1000
    try:
        data = await dfs.backlinks_live(domain, limit=limit, order_by="first_seen,desc")
        items = _extract_items(data)
        if not items:
            return await query.edit_message_text("Нічого не знайшов 😕")

        if action == "show":
            cap = 10 if scope == "10" else 50
            txt = _fmt_preview(items, cap)
            if scope == "all" and len(items) > cap:
                txt += f"\n\n…показано перші {cap} з {len(items)}. Обери CSV (всі), щоб завантажити повний список."
            await query.edit_message_text(txt)
        elif action == "csv":
            csv_bytes = _items_to_csv_bytes(items)
            await query.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_{scope}.csv"),
                caption=f"Експорт для {domain} ({'10' if scope=='10' else 'all'})"
            )
        else:
            await query.edit_message_text("Невідома дія.")
    except HTTPError as e:
        log.exception("HTTP error")
        await query.edit_message_text(f"DataForSEO HTTP error: {e}")
    except Exception as e:
        log.exception("Unexpected error")
        await query.edit_message_text(f"Помилка: {e}")

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
     
    app.add_handler(CommandHandler("buy", buy))
     

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("topup", topup))
    app.add_handler(CommandHandler("backlinks", backlinks))
    app.add_handler(CallbackQueryHandler(on_choice))

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s", DFS_BASE, BACKEND_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

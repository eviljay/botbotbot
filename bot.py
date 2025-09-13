import os
import logging
from dotenv import load_dotenv

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, InputFile
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

from httpx import AsyncClient

# ===== DAO =====
from dao import init_db, ensure_user, get_balance, charge, get_phone

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mybot")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Внутрішні URL до API (локальні)
PAY_API_BASE = os.getenv("PAY_API_BASE", "http://127.0.0.1:8001")

# Фіча-флаг WayForPay (для кнопки)
WAYFORPAY_ENABLED = os.getenv("WAYFORPAY_ENABLED", "0") == "1"

# ===== UI HELPERS =====
AMOUNTS = [50, 100, 200, 500]

def amounts_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, a in enumerate(AMOUNTS, start=1):
        row.append(InlineKeyboardButton(f"{a} UAH", callback_data=f"amt_{a}"))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="home")])
    return InlineKeyboardMarkup(rows)

def topup_methods_kb(amount: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("Оплатити LiqPay", callback_data=f"pay_liqpay_{amount}")]]
    if WAYFORPAY_ENABLED:
        rows.append([InlineKeyboardButton("Оплатити WayForPay", callback_data=f"pay_wfp_{amount}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="topup")])
    return InlineKeyboardMarkup(rows)

def home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Баланс", callback_data="balance")],
        [InlineKeyboardButton("💳 Поповнити", callback_data="topup")]
    ])

# ===== HANDLERS =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.first_name or "", user.last_name or "")
    await update.message.reply_text(
        "Вітаю! Це бот поповнення кредитів.\nОберіть дію нижче:",
        reply_markup=home_kb()
    )

async def home_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("Головне меню:", reply_markup=home_kb())

async def balance_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    bal = get_balance(update.effective_user.id)
    phone = get_phone(update.effective_user.id)
    await q.edit_message_text(
        f"📊 Баланс: {bal} кредитів\nРеєстрація: {'✅ телефон додано' if phone else '❌ телефону немає'}",
        reply_markup=home_kb()
    )

async def topup_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("Оберіть суму поповнення:", reply_markup=amounts_kb())

async def on_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    amt = int(q.data.split("_")[1])
    await q.edit_message_text(
        f"Сума поповнення: {amt} UAH\nОберіть спосіб оплати:",
        reply_markup=topup_methods_kb(amt)
    )

# ---- LiqPay flow ----
async def pay_liqpay_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    amt = int(q.data.split("_")[-1])
    user_id = update.effective_user.id

    payload = {
        "user_id": user_id,
        "amount": amt,
        "currency": "UAH",
        "description": f"Top-up {amt} credits"
    }
    try:
        async with AsyncClient(timeout=20.0) as client:
            r = await client.post(f"{PAY_API_BASE}/api/payments/create", json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                await q.edit_message_text("Помилка створення платежу (LiqPay).")
                return
            # LiqPay потребує data + signature — даємо кнопку на форму
            liqpay_form_url = "https://www.liqpay.ua/api/3/checkout"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Відкрити оплату LiqPay", url=liqpay_form_url)],
                [InlineKeyboardButton("Готово ✅ Перевірити баланс", callback_data="balance")],
            ])
            # Коротке пояснення — користувач відкриє форму і оплатить
            await q.edit_message_text(
                f"Сума: {amt} UAH\nНатисни кнопку нижче, щоб відкрити сторінку оплати LiqPay.\n"
                f"⚠️ Якщо не відкривається — скопіюй дані з повідомлення і оплати вручну.",
                reply_markup=kb
            )
            # Додаємо технічні дані окремим повідомленням (на випадок ручного внесення)
            await q.message.reply_text(
                f"Для LiqPay:\n"
                f"data: `{data['data']}`\n"
                f"signature: `{data['signature']}`",
                parse_mode="Markdown"
            )
    except Exception as e:
        await q.edit_message_text(f"Помилка LiqPay: {e}")

# ---- WayForPay flow ----
async def pay_wfp_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    amt = int(q.data.split("_")[-1])
    user_id = update.effective_user.id

    payload = {
        "user_id": user_id,
        "amount": amt,
        "description": f"Top-up {amt} credits",
    }
    try:
        async with AsyncClient(timeout=20.0) as client:
            r = await client.post(f"{PAY_API_BASE}/api/payments/wayforpay/create", json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                await q.edit_message_text("Помилка створення платежу (WayForPay).")
                return
            pay_url = data["url"]
    except Exception as e:
        await q.edit_message_text(f"Помилка WayForPay: {e}")
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Відкрити оплату WayForPay", url=pay_url)],
        [InlineKeyboardButton("Готово ✅ Перевірити баланс", callback_data="balance")],
    ])
    await q.edit_message_text(
        f"Сума: {amt} UAH\nНатисни кнопку нижче для переходу на WayForPay.",
        reply_markup=kb
    )

# ===== REGISTRATION (телефон) — опційно, якщо треба =====
async def ask_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    btn = KeyboardButton("📱 Надіслати номер", request_contact=True)
    kb = ReplyKeyboardMarkup([[btn]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Для верифікації надішли номер телефону:", reply_markup=kb)

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    contact = update.message.contact
    if contact and contact.user_id == update.effective_user.id:
        # Збережи у своїй dao.set_phone(...)
        await update.message.reply_text("Дякую! Телефон збережено ✅", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text("Це не твій контакт 🤔 Спробуй ще раз.", reply_markup=ReplyKeyboardRemove())

# ===== MAIN =====
def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("phone", ask_phone))
    app.add_handler(CallbackQueryHandler(home_cb, pattern="^home$"))
    app.add_handler(CallbackQueryHandler(balance_cb, pattern="^balance$"))
    app.add_handler(CallbackQueryHandler(topup_cb, pattern="^topup$"))
    app.add_handler(CallbackQueryHandler(on_amount, pattern=r"^amt_\d+$"))
    app.add_handler(CallbackQueryHandler(pay_liqpay_cb, pattern=r"^pay_liqpay_\d+$"))
    app.add_handler(CallbackQueryHandler(pay_wfp_cb, pattern=r"^pay_wfp_\d+$"))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))

    # fallback: якщо юзер напише "поповнити"
    app.add_handler(MessageHandler(filters.Regex(r"(?i)поповнити|topup"), lambda u,c: u.message.reply_text("Оберіть суму:", reply_markup=amounts_kb())))
    app.add_handler(MessageHandler(filters.COMMAND | filters.TEXT, lambda u,c: u.message.reply_text("Головне меню:", reply_markup=home_kb())))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

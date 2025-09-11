# bot.py
import os
import logging
import httpx
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# ---------- Config & logging ----------
load_dotenv()
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger(__name__)
logger.info("Bot starting. BACKEND_BASE=%s", BACKEND_BASE)


# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Привіт! Тут можна поповнити кредити.\n"
        "Обери пакет нижче 👇"
    )
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("100 кредитів — 99 UAH", callback_data="buy:99:100 кредитів")],
            [InlineKeyboardButton("220 кредитів — 199 UAH", callback_data="buy:199:220 кредитів")],
        ]
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=kb)
    else:
        # на випадок, якщо прийшло як callback або інший апдейт
        await update.effective_chat.send_message(text, reply_markup=kb)


async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # альтернативна команда /buy (те саме меню)
    await start(update, context)


async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()  # миттєвий фідбек Telegram (обов'язково)
    data = (q.data or "")
    logger.info("on_choice: %s", data)

    if not data.startswith("buy:"):
        await q.message.reply_text("Не впізнав дію кнопки 🤔")
        return

    try:
        # формат: buy:<amount>:<description>
        _, amount_str, description = data.split(":", 2)
        payload = {
            "user_id": q.from_user.id,    # int
            "amount": amount_str,         # дозволимо рядок, бекенд перетворить
            "description": description,
        }

        url = f"{BACKEND_BASE}/api/payments/create"
        logger.info("POST %s payload=%s", url, payload)

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json=payload)

        if r.status_code != 200:
            logger.error("Backend %s: %s", r.status_code, r.text)
            await q.message.reply_text("Бекенд недоступний або повернув помилку. Спробуй ще раз пізніше.")
            return

        resp = r.json()
        checkout_url = resp.get("checkout_url")
        if not checkout_url:
            logger.error("No checkout_url in response: %s", resp)
            await q.message.reply_text("Не отримав посилання на оплату 😕")
            return

        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Оплатити через LiqPay", url=checkout_url)]]
        )
        await q.message.reply_text(f"Сума: {amount_str} UAH\n{description}", reply_markup=kb)

    except Exception as e:
        logger.exception("on_choice error")
        await q.message.reply_text("Сталася помилка під час обробки. Спробуй ще раз.")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    # не падаємо на винятках — просто лог і тихий фейл
    logger.exception("Unhandled error: %s", context.error)


# ---------- App ----------
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy))

    # обробка кліку по кнопці поповнення
    app.add_handler(CallbackQueryHandler(on_choice, pattern=r"^buy:"))

    # error handler
    app.add_error_handler(on_error)

    logger.info("Application started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

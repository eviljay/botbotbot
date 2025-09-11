# bot.py
import os
import io
import csv
import uuid
import math
import logging
from typing import List

from dotenv import load_dotenv
from httpx import AsyncClient, ConnectError, HTTPError

from telegram import (
    Update,
    InputFile,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# Локальні модулі
from dao import init_db, ensure_user, get_balance, charge, get_phone, register_or_update_phone
from dataforseo import DataForSEO

# -----------------------------------------------------------------------------
# ЛОГИ
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# -----------------------------------------------------------------------------
# ENV
# -----------------------------------------------------------------------------
load_dotenv()

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DFS_LOGIN = os.environ["DATAFORSEO_LOGIN"]
DFS_PASS = os.environ["DATAFORSEO_PASSWORD"]
DFS_BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")

# Публічний бекенд (наш FastAPI з /api/payments/*)
BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8000").rstrip("/")

# Скільки коштує 1 кредит (в грн)
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))
# Списання за запит беклінків у гривнях (потім конвертуємо у кредити)
BACKLINKS_CHARGE_UAH = float(os.getenv("BACKLINKS_CHARGE_UAH", "5"))

# Початковий бонус за реєстрацію (кредитів)
INITIAL_BONUS = int(os.getenv("INITIAL_BONUS", "10"))

# Варіанти поповнення (в грн)
TOPUP_OPTIONS = [
    int(x.strip())
    for x in os.getenv("TOPUP_OPTIONS", "100,250,500").split(",")
    if x.strip().isdigit()
]

# Скільки показувати записів при "показати 10"
PREVIEW_COUNT = 10
# Скільки максимум віддавати у CSV при "всі"
CSV_MAX = 1000

# -----------------------------------------------------------------------------
# ІНІТ
# -----------------------------------------------------------------------------
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE)

# -----------------------------------------------------------------------------
# УТИЛІТИ
# -----------------------------------------------------------------------------
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("🔗 Backlinks"), KeyboardButton("💳 Поповнити")],
        [KeyboardButton("📊 Баланс"), KeyboardButton("📱 Реєстрація")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def _extract_items(resp: dict) -> List[dict]:
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

def _fmt_preview(items: List[dict], cap: int) -> str:
    lines = []
    for it in items[:cap]:
        url_from = (it.get("page_from") or {}).get("url_from") or it.get("url_from")
        anchor = (it.get("anchor") or "").strip()
        first_seen = it.get("first_seen")
        lines.append(f"• {url_from}\n  anchor: {anchor[:80]} | first_seen: {first_seen}")
    return "\n".join(lines)

def _items_to_csv_bytes(items: List[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["url_from", "url_to", "anchor", "dofollow", "first_seen", "last_seen", "domain_from"])
    for it in items:
        w.writerow([
            (it.get("page_from") or {}).get("url_from") or it.get("url_from"),
            it.get("url_to"),
            (it.get("anchor") or "").replace("\n", " ").strip(),
            it.get("dofollow"),
            it.get("first_seen"),
            it.get("last_visited"),
            it.get("domain_from")
        ])
    return buf.getvalue().encode()

def _uah_to_credits(amount_uah: float) -> int:
    # округляємо вгору, щоб не було “півкредиту”
    return max(1, math.ceil(amount_uah / CREDIT_PRICE_UAH))

# -----------------------------------------------------------------------------
# /start
# -----------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg = "✅ телефон додано" if phone else "❌ немає телефону (використайте Реєстрація)"

    text = (
        "Привіт! Я SEO-бот з балансом.\n\n"
        "Команди/меню:\n"
        "🔗 Backlinks — отримати останні або всі беклінки й CSV\n"
        "💳 Поповнити — оплата через LiqPay\n"
        "📊 Баланс — показати ваш баланс\n"
        "📱 Реєстрація — додати телефон (новим — бонус)\n\n"
        f"Статус реєстрації: {reg}\n"
        f"Ваш баланс: {bal} кредитів"
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())

# -----------------------------------------------------------------------------
# Реєстрація (ConversationHandler)
# -----------------------------------------------------------------------------
WAIT_PHONE = 10

def _normalize_phone(p: str) -> str:
    digits = "".join(ch for ch in p if ch.isdigit())
    return ("+" + digits) if digits and not p.strip().startswith("+") else (p if p.startswith("+") else "+" + digits)

async def register_cmd_or_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    kb = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
    await update.message.reply_text(
        "Натисніть кнопку, щоб поділитися **своїм** номером телефону:",
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True)
    )
    return WAIT_PHONE

async def on_contact_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    contact = update.message.contact
    if not contact:
        kb = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
        await update.message.reply_text(
            "Будь ласка, надішліть **контакт** кнопкою нижче.",
            reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True),
        )
        return WAIT_PHONE

    if contact.user_id and contact.user_id != uid:
        kb = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
        await update.message.reply_text(
            "Здається, це не ваш номер. Спробуйте ще раз.",
            reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True),
        )
        return WAIT_PHONE

    phone_norm = _normalize_phone(contact.phone_number or "")
    is_new, credited = register_or_update_phone(uid, phone_norm, initial_bonus=INITIAL_BONUS)
    bal = get_balance(uid)

    if is_new and credited > 0:
        msg = f"✅ Дякуємо за реєстрацію!\nНараховано бонус: +{credited} кредитів.\nВаш баланс: {bal}"
    else:
        msg = f"✅ Телефон збережено.\nВаш баланс: {bal}"

    await update.message.reply_text(msg, reply_markup=main_menu_keyboard())
    return ConversationHandler.END

async def cancel_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Скасовано.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END

# -----------------------------------------------------------------------------
# Баланс
# -----------------------------------------------------------------------------
async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg = "✅ телефон додано" if phone else "❌ немає телефону (використайте Реєстрація)"
    await update.message.reply_text(f"Баланс: {bal} кредитів\nРеєстрація: {reg}")

# -----------------------------------------------------------------------------
# Поповнення
# -----------------------------------------------------------------------------
async def topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)

    rows = []
    for amount in TOPUP_OPTIONS:
        credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([InlineKeyboardButton(f"💳 Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])

    await update.message.reply_text("Оберіть суму поповнення:", reply_markup=InlineKeyboardMarkup(rows))

# -----------------------------------------------------------------------------
# Backlinks
# -----------------------------------------------------------------------------
async def backlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args:
        return await update.message.reply_text("Приклад: `/backlinks yourdomain.com`", parse_mode="Markdown")
    domain = args[0].strip()

    kb = [
        [
            InlineKeyboardButton("👀 Показати 10 (5₴)", callback_data=f"show|{domain}|10"),
            InlineKeyboardButton("⬇️ CSV 10 (5₴)", callback_data=f"csv|{domain}|10"),
        ],
        [
            InlineKeyboardButton("👀 Показати всі (5₴)", callback_data=f"show|{domain}|all"),
            InlineKeyboardButton("⬇️ CSV всі (5₴)", callback_data=f"csv|{domain}|all"),
        ],
    ]
    await update.message.reply_text(
        f"Домен: *{domain}*\nОберіть дію (з кожної дії буде списано 5₴):",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )

# -----------------------------------------------------------------------------
# CALLBACKS
# -----------------------------------------------------------------------------
async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    data = (query.data or "").split("|")

    # --- Поповнення ---
    if data[0] == "topup":
        try:
            amount_uah = int(data[1])
        except Exception:
            return await query.edit_message_text("Невірна сума.")

        # Створюємо інвойс у бекенді
        try:
            async with AsyncClient(timeout=20) as c:
                r = await c.post(f"{BACKEND_BASE}/api/payments/create", json={"user_id": uid, "amount": amount_uah})
                r.raise_for_status()
                resp = r.json()
        except ConnectError:
            return await query.edit_message_text("❌ Бекенд недоступний. Перевір BACKEND_BASE і mybot-api (порт 8000).")
        except HTTPError as e:
            return await query.edit_message_text(f"Помилка створення платежу: {e}")

        url = resp.get("invoiceUrl")
        if not url:
            return await query.edit_message_text("Не отримав посилання на оплату.")
        kb = [[InlineKeyboardButton("💳 Оплатити (LiqPay)", url=url)]]
        return await query.edit_message_text(
            f"Рахунок створено на {amount_uah}₴. Натисніть, щоб оплатити:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # --- Платні дії (backlinks) ---
    if len(data) != 3:
        return await query.edit_message_text("Невірний запит.")
    action, domain, scope = data

    # Конвертуємо 5 грн у кредити
    need_credits = _uah_to_credits(BACKLINKS_CHARGE_UAH)

    # Списання
    if not charge(uid, need_credits, domain, scope):
        # Пропонуємо поповнення
        rows = []
        for amount in TOPUP_OPTIONS:
            credits = int(amount // CREDIT_PRICE_UAH)
            rows.append([InlineKeyboardButton(f"💳 Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])
        return await query.edit_message_text(
            f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
            reply_markup=InlineKeyboardMarkup(rows)
        )

    # Виконуємо запит до DataForSEO
    try:
        limit = PREVIEW_COUNT if scope == "10" else CSV_MAX
        data_resp = await dfs.backlinks_live(domain, limit=limit, order_by="first_seen,desc")
        items = _extract_items(data_resp)
        if not items:
            bal_now = get_balance(uid)
            return await query.edit_message_text(f"Нічого не знайшов 😕\nВаш новий баланс: {bal_now} кредитів")

        if action == "show":
            cap = PREVIEW_COUNT if scope == "10" else min(50, len(items))
            txt = _fmt_preview(items, cap)
            bal_now = get_balance(uid)
            if scope == "all" and len(items) > cap:
                txt += f"\n\n…показано перші {cap} з {len(items)}."
            txt += f"\n\n💰 Списано {need_credits} кредит(и). Новий баланс: {bal_now}"
            await query.edit_message_text(txt)
        elif action == "csv":
            csv_bytes = _items_to_csv_bytes(items)
            bal_now = get_balance(uid)
            await query.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_{scope}.csv"),
                caption=f"Експорт для {domain} ({'10' if scope=='10' else 'all'})\n💰 Списано {need_credits}. Новий баланс: {bal_now}"
            )
            await query.edit_message_text("Готово ✅")
        else:
            await query.edit_message_text("Невідома дія.")
    except HTTPError as e:
        log.exception("HTTP error")
        await query.edit_message_text(f"DataForSEO HTTP error: {e}")
    except Exception as e:
        log.exception("Unexpected error")
        await query.edit_message_text(f"Помилка: {e}")

# -----------------------------------------------------------------------------
# Обробка натискань по меню (reply keyboard)
# -----------------------------------------------------------------------------
async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "🔗 Backlinks":
        return await update.message.reply_text("Введіть команду у форматі: /backlinks yourdomain.com")
    if text == "💳 Поповнити":
        return await topup(update, context)
    if text == "📊 Баланс":
        return await balance(update, context)
    if text == "📱 Реєстрація":
        # запуск розмови /register через entry_point
        return await register_cmd_or_menu(update, context)

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("topup", topup))
    app.add_handler(CommandHandler("backlinks", backlinks))

    # Реєстрація як розмова — важливо додати РАНІШЕ за загальні text-хендлери
    reg_conv = ConversationHandler(
        entry_points=[
            CommandHandler("register", register_cmd_or_menu),
            MessageHandler(filters.Regex(r"^📱 Реєстрація$"), register_cmd_or_menu),
        ],
        states={
            WAIT_PHONE: [MessageHandler(filters.CONTACT, on_contact_register)],
        },
        fallbacks=[CommandHandler("cancel", cancel_register)],
        allow_reentry=True,
    )
    app.add_handler(reg_conv)

    # Callback’и
    app.add_handler(CallbackQueryHandler(on_choice))

    # Керування меню (reply keyboard)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_menu_text))

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s", DFS_BASE, BACKEND_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

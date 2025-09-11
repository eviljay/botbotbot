# bot.py
import os
import io
import csv
import uuid
import logging

from dotenv import load_dotenv
from httpx import AsyncClient, HTTPError, ConnectError
from telegram import (
    Update, InputFile,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

# ===== наші модулі =====
from dataforseo import DataForSEO
from dao import (
    init_db, ensure_user, get_balance, charge,
    get_phone, register_or_update_phone
)

# ----------------- ЛОГИ -----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ----------------- ENV ------------------
load_dotenv()

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

DFS_LOGIN = os.environ["DATAFORSEO_LOGIN"]
DFS_PASS  = os.environ["DATAFORSEO_PASSWORD"]
DFS_BASE  = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")

BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")

# 1 кредит = N грн
CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))

# Скільки грн списувати за беклінки (показ/CSV)
BACKLINKS_PRICE_UAH = float(os.getenv("BACKLINKS_PRICE_UAH", "5"))

INITIAL_BONUS = int(os.getenv("INITIAL_BONUS", "10"))

TOPUP_OPTIONS = [
    int(x.strip())
    for x in os.getenv("TOPUP_OPTIONS", "100,250,500").split(",")
    if x.strip().isdigit()
]

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # 0 = вимкнено

# ----------------- INIT -----------------
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE)

# =========================================================
#                     UI: КЛАВІАТУРИ
# =========================================================
def make_main_kb(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        ["🔗 Backlinks", "💳 Поповнити"],
        ["📊 Баланс", "📱 Реєстрація"],
    ]
    if ADMIN_ID and user_id == ADMIN_ID:
        rows.append(["🛠 Адмінка"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def topup_inline_kb() -> InlineKeyboardMarkup:
    rows = []
    for amount in TOPUP_OPTIONS:
        approx_credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([InlineKeyboardButton(
            f"💳 Поповнити {amount}₴ (~{approx_credits} кредитів)",
            callback_data=f"topup|{amount}"
        )])
    return InlineKeyboardMarkup(rows)

def backlinks_inline_kb(domain: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👀 Показати 10", callback_data=f"show|{domain}|10"),
            InlineKeyboardButton("⬇️ CSV (10)",    callback_data=f"csv|{domain}|10"),
        ],
        [
            InlineKeyboardButton("👀 Показати всі", callback_data=f"show|{domain}|all"),
            InlineKeyboardButton("⬇️ CSV (всі)",    callback_data=f"csv|{domain}|all"),
        ],
    ])

# =========================================================
#                     ХЕЛПЕРИ
# =========================================================
def _uah_to_credits(uah: float) -> int:
    # мінімум 1 кредит
    return max(1, int(round(uah / CREDIT_PRICE_UAH)))

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
        anchor = (it.get("anchor") or "").strip().replace("\n", " ")
        first_seen = it.get("first_seen")
        lines.append(f"• {url_from}\n  anchor: {anchor[:80]} | first_seen: {first_seen}")
    return "\n".join(lines)

def _items_to_csv_bytes(items: list[dict]) -> bytes:
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

# =========================================================
#                     ХЕНДЛЕРИ
# =========================================================
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
    await update.message.reply_text(text, reply_markup=make_main_kb(uid))

async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка натискання на кнопки з емодзі."""
    uid = update.effective_user.id
    text = (update.message.text or "").strip()

    if text.startswith("🔗"):
        await update.message.reply_text("Введіть команду у форматі: `backlinks ваш_домен` (без http…)", parse_mode="Markdown")
        return
    if text.startswith("💳"):
        return await topup(update, context)
    if text.startswith("📊"):
        return await balance(update, context)
    if text.startswith("📱"):
        return await register(update, context)
    if text.startswith("🛠") and ADMIN_ID and uid == ADMIN_ID:
        return await admin(update, context)

    # Підтримка швидкого вводу "backlinks domain.com"
    if text.lower().startswith("backlinks"):
        parts = text.split()
        if len(parts) >= 2:
            fake = Update.de_json(update.to_dict(), context.application.bot)
            update.message.text = "/backlinks " + parts[1]
            return await backlinks(update, context)

    await update.message.reply_text("Не впізнаю команду. Скористайтесь кнопками меню ⬇️", reply_markup=make_main_kb(uid))

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    kb = ReplyKeyboardMarkup([[ "📱 Надіслати номер (телеграм)" ]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(
        "Натисніть системну кнопку *прикріплення контакту* в Telegram (іконка скріпки ➜ Контакт), "
        "або просто натисніть нижче та відправте свій номер.",
        reply_markup=kb, parse_mode="Markdown"
    )

async def on_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    contact = update.message.contact
    # Якщо користувач не надіслав системний контакт — пробуємо взяти текст
    if not contact and update.message.text and update.message.text.startswith("+"):
        ph = update.message.text.strip()
        is_new, credited = register_or_update_phone(uid, ph, initial_bonus=INITIAL_BONUS)
    else:
        if not contact:
            return
        if contact.user_id and contact.user_id != uid:
            return await update.message.reply_text(
                "Здається, це не ваш номер. Поділіться саме своїм контактом.",
                reply_markup=ReplyKeyboardRemove()
            )
        is_new, credited = register_or_update_phone(uid, contact.phone_number, initial_bonus=INITIAL_BONUS)

    bal = get_balance(uid)
    if is_new and credited > 0:
        txt = f"✅ Дякую! Нараховано бонус +{credited} кредитів.\nВаш баланс: {bal}"
    else:
        txt = f"✅ Телефон збережено. Ваш баланс: {bal}"
    await update.message.reply_text(txt, reply_markup=make_main_kb(uid))

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg = "✅ телефон додано" if phone else "❌ немає телефону"
    await update.message.reply_text(f"Баланс: {bal} кредитів\n{reg}", reply_markup=make_main_kb(uid))

async def topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    await update.message.reply_text("Оберіть суму поповнення:", reply_markup=topup_inline_kb())

async def backlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:] if update.message.text else []
    if not args:
        return await update.message.reply_text("Приклад: /backlinks yourdomain.com або натисніть 🔗 Backlinks і введіть: `backlinks yourdomain.com`", parse_mode="Markdown")
    domain = args[0]
    await update.message.reply_text(
        f"Домен: *{domain}*\nОбери дію:\n"
        f"• Платна операція: {_uah_to_credits(BACKLINKS_PRICE_UAH)} кредит(ів) (~{BACKLINKS_PRICE_UAH:.2f}₴)",
        reply_markup=backlinks_inline_kb(domain),
        parse_mode="Markdown"
    )

async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    data = (query.data or "").split("|")

    # Поповнення LiqPay
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
            return await query.edit_message_text("❌ Бекенд недоступний. Перевір BACKEND_BASE та сервіс payments_api.")
        except HTTPError as e:
            return await query.edit_message_text(f"❌ Помилка бекенду: {e.response.status_code}")
        # очікуємо {"checkout_url": "..."}
        url = resp.get("checkout_url") or resp.get("invoiceUrl") or resp.get("url")
        if not url:
            return await query.edit_message_text("Не вдалося отримати посилання на оплату.")
        kb = [[InlineKeyboardButton("💳 Оплатити (LiqPay)", url=url)]]
        return await query.edit_message_text(
            f"Рахунок створено на {amount_uah}₴. Натисніть, щоб оплатити:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # Платні дії (беклінки)
    try:
        action, domain, scope = data
    except ValueError:
        return await query.edit_message_text("Невірний запит.")

    # списання в кредитах з ціни у грн
    cost_credits = _uah_to_credits(BACKLINKS_PRICE_UAH)
    if not charge(uid, cost_credits, domain, scope):
        # немає коштів — запропонувати поповнення
        return await query.edit_message_text(
            f"Недостатньо кредитів (потрібно {cost_credits}). Оберіть суму поповнення:",
            reply_markup=topup_inline_kb()
        )

    # якщо списали — виконуємо запит до DFS
    limit = 10 if scope == "10" else 1000
    try:
        data = await dfs.backlinks_live(domain, limit=limit, order_by="first_seen,desc")
        items = _extract_items(data)
        if not items:
            bal = get_balance(uid)
            return await query.edit_message_text(f"Нічого не знайшов 😕\nВаш новий баланс: {bal} кредитів")

        if action == "show":
            cap = 10 if scope == "10" else 50
            txt = _fmt_preview(items, cap)
            if scope == "all" and len(items) > cap:
                txt += f"\n\n…показано перші {cap} з {len(items)}."
            bal = get_balance(uid)
            txt += f"\n\n💼 Новий баланс: {bal} кредитів"
            await query.edit_message_text(txt)
        elif action == "csv":
            csv_bytes = _items_to_csv_bytes(items)
            bal = get_balance(uid)
            await query.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_{scope}.csv"),
                caption=f"Експорт для {domain} ({'10' if scope=='10' else 'all'})\n💼 Новий баланс: {bal} кредитів"
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

# ----------------- Адмінка (простий варіант) -----------------
async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not (ADMIN_ID and uid == ADMIN_ID):
        return await update.message.reply_text("⛔️ Доступ заборонено.")
    await update.message.reply_text(
        "🛠 Адмінка (мінімальна версія)\n"
        "• Надалі тут буде список юзерів, баланси та телефони.\n"
        "• Поки що доступні стандартні команди.\n",
        reply_markup=make_main_kb(uid)
    )

# =========================================================
#                      MAIN
# =========================================================
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("topup", topup))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("backlinks", backlinks))
    app.add_handler(CommandHandler("admin", admin))

    # Контакт/номер
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))

    # Кнопки inline (topup/backlinks actions)
    app.add_handler(CallbackQueryHandler(on_choice))

    # Обробник меню з емодзі та вільного тексту
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s", DFS_BASE, BACKEND_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

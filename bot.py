# /root/mybot/bot.py
import os
import io
import csv
import uuid
import logging

from dotenv import load_dotenv
from httpx import AsyncClient, ConnectError, HTTPError
from telegram import (
    Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters,
)

# --- локальні модулі вашого проєкту
from dao import init_db, ensure_user, get_balance, charge, get_phone, register_or_update_phone
from dataforseo import DataForSEO

# -------------------- ЛОГІНГ --------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# -------------------- ENV -----------------------
load_dotenv()

TELEGRAM_TOKEN      = os.environ["TELEGRAM_BOT_TOKEN"]
DFS_LOGIN           = os.environ["DATAFORSEO_LOGIN"]
DFS_PASS            = os.environ["DATAFORSEO_PASSWORD"]
DFS_BASE            = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")
BACKEND_BASE        = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")

PRICE_10            = int(os.getenv("PRICE_10", "5"))
PRICE_ALL           = int(os.getenv("PRICE_ALL", "20"))
INITIAL_BONUS       = int(os.getenv("INITIAL_BONUS", "10"))
CREDIT_PRICE_UAH    = float(os.getenv("CREDIT_PRICE_UAH", "5"))
TOPUP_OPTIONS       = [int(x.strip()) for x in os.getenv("TOPUP_OPTIONS", "100,250,500").split(",") if x.strip().isdigit()]

# -------------------- INIT ----------------------
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE)

# -------------------- HELPERS -------------------
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

def _topup_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for amount in TOPUP_OPTIONS:
        credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([
            InlineKeyboardButton(
                f"💳 Поповнити {amount}₴ (~{credits} кредитів)",
                callback_data=f"topup|{amount}"
            )
        ])
    return InlineKeyboardMarkup(rows)

def _main_menu_kb() -> ReplyKeyboardMarkup:
    # «фізична» клавіатура знизу
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("/backlinks"), KeyboardButton("/topup")],
            [KeyboardButton("/balance"),   KeyboardButton("/register")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
    )

# -------------------- HANDLERS ------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg_line = "Статус реєстрації: ✅ телефон додано" if phone else "Статус реєстрації: ❌ немає телефону (використайте /register)"
    text = (
        "Привіт! Я SEO-бот з балансом.\n"
        "Команди:\n"
        "/register — додати номер телефону (новим користувачам бонус)\n"
        "/backlinks <домен> — вибір 10/всі + перегляд/CSV (списання кредитів)\n"
        "/balance — показати баланс\n"
        "/topup — поповнити баланс через LiqPay\n\n"
        f"{reg_line}\nВаш баланс: {bal} кредитів"
    )
    # Показуємо лише «фізичні» кнопки-меню, без інлайн-пакетів на старті
    kb = _main_menu_kb()
    if update.message:
        await update.message.reply_text(text, reply_markup=kb)
    else:
        await update.effective_chat.send_message(text, reply_markup=kb)

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Меню:", reply_markup=_main_menu_kb())

async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Просто дубль /start (кнопки меню)
    await start(update, context)

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
    await update.message.reply_text(txt, reply_markup=_main_menu_kb())

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg_line = "✅ телефон додано" if phone else "❌ немає телефону (використайте /register)"
    await update.message.reply_text(f"Баланс: {bal} кредитів\nРеєстрація: {reg_line}", reply_markup=_main_menu_kb())

async def topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    await update.message.reply_text("Оберіть суму поповнення:", reply_markup=_topup_keyboard())

async def backlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args:
        return await update.message.reply_text("Приклад: /backlinks yourdomain.com", reply_markup=_main_menu_kb())
    domain = args[0]
    kb = [
        [
            InlineKeyboardButton("👀 Показати 10 останніх", callback_data=f"show|{domain}|10"),
            InlineKeyboardButton("⬇️ CSV (10)",            callback_data=f"csv|{domain}|10"),
        ],
        [
            InlineKeyboardButton("👀 Показати всі",         callback_data=f"show|{domain}|all"),
            InlineKeyboardButton("⬇️ CSV (всі)",            callback_data=f"csv|{domain}|all"),
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

    raw = query.data or ""
    parts = raw.split("|")
    if not parts:
        return await query.edit_message_text("Невірний запит.")

    # -------- Поповнення через LiqPay --------
    if parts[0] == "topup":
        try:
            amount_uah = int(parts[1])
        except Exception:
            return await query.edit_message_text("Невірна сума поповнення.")

        payload = {
            "user_id": str(uid),
            "amount": str(amount_uah),
            "description": f"{amount_uah} UAH topup"
        }

        try:
            async with AsyncClient(timeout=20) as c:
                r = await c.post(f"{BACKEND_BASE}/api/payments/create", json=payload)
            if r.status_code != 200:
                log.error("Payments API %s: %s", r.status_code, r.text)
                return await query.edit_message_text("❌ Бекенд недоступний або повернув помилку. Спробуйте пізніше.")
            resp = r.json()
            url = resp.get("checkout_url")
            if not url:
                log.error("Payments API no checkout_url: %s", resp)
                return await query.edit_message_text("❌ Не отримав посилання на оплату. Спробуйте пізніше.")
        except ConnectError:
            return await query.edit_message_text(
                "❌ Бекенд недоступний. Перевір BACKEND_BASE і чи запущений payments_api (порт 8001)."
            )
        except Exception as e:
            log.exception("Create payment failed")
            return await query.edit_message_text(f"❌ Помилка створення оплати: {e}")

        kb = [[InlineKeyboardButton("💳 Оплатити (LiqPay)", url=url)]]
        return await query.edit_message_text(
            f"Рахунок створено на {amount_uah}₴. Натисніть, щоб оплатити:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # -------- Платні дії (DataForSEO) --------
    try:
        action, domain, scope = parts
    except ValueError:
        return await query.edit_message_text("Невірний запит.")

    cost = PRICE_10 if scope == "10" else PRICE_ALL
    if not charge(uid, cost, domain, scope):
        # не вистачає кредитів — показуємо кнопки поповнення
        return await query.edit_message_text(
            f"Недостатньо кредитів (потрібно {cost}). Поповніть баланс.",
            reply_markup=_topup_keyboard()
        )

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

# -------------------- MAIN ----------------------
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu_cmd))      # повернути «фізичні» кнопки
    app.add_handler(CommandHandler("buy", buy))            # дубль меню
    app.add_handler(CommandHandler("register", register))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("topup", topup))
    app.add_handler(CommandHandler("backlinks", backlinks))
    app.add_handler(CallbackQueryHandler(on_choice))       # ловимо всі натискання

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s", DFS_BASE, BACKEND_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

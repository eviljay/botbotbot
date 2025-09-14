# bot.py
import os
import io
import csv
import math
import logging
import sqlite3
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

# ====== Локальні модулі ======
from dao import init_db, ensure_user, get_balance, charge, get_phone, register_or_update_phone
from dataforseo import DataForSEO

# ====== Логи ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bot")

# ====== ENV ======
load_dotenv()
TELEGRAM_BOT_URL     = os.getenv("TELEGRAM_BOT_URL", "")        # наприклад: https://t.me/YourBotName
TELEGRAM_START_PARAM = os.getenv("TELEGRAM_START_PARAM", "paid") # опціонально

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DFS_LOGIN = os.environ["DATAFORSEO_LOGIN"]
DFS_PASS = os.environ["DATAFORSEO_PASSWORD"]
DFS_BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")

# внутрішній бекенд (локальний API)
BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")
# публічний домен (на випадок, якщо колись доведеться дати /pay/{order_id})
PUBLIC_BASE  = os.getenv("PUBLIC_BASE", "https://server1.seoswiss.online").rstrip("/")

CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))
BACKLINKS_CHARGE_UAH = float(os.getenv("BACKLINKS_CHARGE_UAH", "5"))
INITIAL_BONUS = int(os.getenv("INITIAL_BONUS", "10"))
TOPUP_OPTIONS = [int(x.strip()) for x in os.getenv("TOPUP_OPTIONS", "100,250,500").split(",") if x.strip().isdigit()]

# для адмінки
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH = os.getenv("DB_PATH", "bot.db")  # очікувана БД, яку використовує dao.py

PREVIEW_COUNT = 10
CSV_MAX = 1000
PAGE_SIZE = 20
WAIT_PHONE = 10

# ====== INIT ======
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE)

# ====== Утиліти ======
def main_menu_keyboard(registered: bool) -> ReplyKeyboardMarkup:
    if registered:
        rows = [
            [KeyboardButton("🔗 Backlinks"), KeyboardButton("💳 Поповнити")],
            [KeyboardButton("📊 Баланс")],
        ]
    else:
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
    return max(1, math.ceil(amount_uah / CREDIT_PRICE_UAH))

def _registered(uid: int) -> bool:
    return bool(get_phone(uid))

# ====== Хелпер: клавіатура сум поповнення (LiqPay) ======
def _build_topup_amounts_kb() -> InlineKeyboardMarkup:
    rows = []
    for amount in TOPUP_OPTIONS:
        credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([InlineKeyboardButton(f"💳 Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="topup_providers")])
    return InlineKeyboardMarkup(rows)

# ====== /start ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    reg = _registered(uid)
    reg_text = "✅ телефон додано" if reg else "❌ немає телефону (використайте Реєстрація)"

    text = (
        "Привіт! Я SEO-бот з балансом.\n\n"
        "Команди/меню:\n"
        "🔗 Backlinks — отримати останні або всі беклінки й CSV\n"
        "💳 Поповнити — оплата через LiqPay (інші провайдери — скоро)\n"
        "📊 Баланс — показати ваш баланс\n"
        "📱 Реєстрація — додати телефон (новим — бонус)\n\n"
        f"Статус реєстрації: {reg_text}\n"
        f"Ваш баланс: {bal} кредитів"
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard(reg))

# ====== Реєстрація ======
def _normalize_phone(p: str) -> str:
    digits = "".join(ch for ch in p if ch.isdigit())
    return ("+" + digits) if digits and not p.strip().startswith("+") else (p if p.startswith("+") else "+" + digits)

async def register_cmd_or_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)

    if _registered(uid):
        return await update.message.reply_text("Ви вже зареєстровані ✅", reply_markup=main_menu_keyboard(True))

    kb = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
    await update.message.reply_text(
        "Натисніть кнопку, щоб поділитися **своїм** номером телефону:",
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True)
    )
    return WAIT_PHONE

async def on_contact_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    contact = update.message.contact
    if not contact or (contact.user_id and contact.user_id != uid):
        kb = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
        await update.message.reply_text(
            "Будь ласка, поділіться **власним** контактом.",
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

    await update.message.reply_text(msg, reply_markup=main_menu_keyboard(True))
    return ConversationHandler.END

async def cancel_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Скасовано.", reply_markup=main_menu_keyboard(_registered(update.effective_user.id)))
    return ConversationHandler.END

# ====== Баланс ======
async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    reg_text = "✅ телефон додано" if _registered(uid) else "❌ немає телефону (використайте Реєстрація)"
    await update.message.reply_text(f"Баланс: {bal} кредитів\nРеєстрація: {reg_text}")

# ====== Поповнення: вибір провайдера ======
async def topup_providers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 LiqPay", callback_data="open_liqpay_amounts")],
        [InlineKeyboardButton("🏦 WayForPay", callback_data="pay_wfp_soon")],
        [InlineKeyboardButton("🧾 Portmone", callback_data="pay_portmone_soon")],
    ])
    text = (
        "💰 *Поповнення балансу*\n\n"
        "Оберіть провайдера. WayForPay та Portmone поки що не підключені."
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")

# ====== Поповнення: вибір суми (LiqPay) ======
async def topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # показуємо вибір сум; якщо прийшли з callback — редагуємо поточне повідомлення
    if update.message:
        await update.message.reply_text("Оберіть суму поповнення (LiqPay):", reply_markup=_build_topup_amounts_kb())
    else:
        await update.callback_query.edit_message_text("Оберіть суму поповнення (LiqPay):", reply_markup=_build_topup_amounts_kb())

# ====== Backlinks ======
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

# ====== CALLBACKS (topup & backlinks) ======
async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    data = (query.data or "").split("|")

    # --- Екран вибору провайдера / повернення назад ---
    if data[0] == "topup_providers":
        return await topup_providers(update, context)

    # --- Відкрити суми LiqPay ---
    if data[0] == "open_liqpay_amounts":
        return await topup(update, context)

    # --- Пусті кнопки провайдерів ---
    if data[0] in ("pay_wfp_soon", "pay_portmone_soon"):
        return await query.answer("Ще не підключено", show_alert=False)

    # --- Поповнення через LiqPay (існуюча логіка створення платежу) ---
    if data[0] == "topup":
        try:
            amount_uah = int(data[1])
        except Exception:
            return await query.edit_message_text("Невірна сума.")

        try:
            async with AsyncClient(timeout=20) as c:
                r = await c.post(
                    f"{BACKEND_BASE}/api/payments/create",
                    json={"user_id": uid, "amount": amount_uah}
                )
                r.raise_for_status()
                resp = r.json()
                log.info("payments.create resp: %s", resp)
        except ConnectError:
            return await query.edit_message_text(
                f"❌ Бекенд недоступний ({BACKEND_BASE}). Перевір API/порт."
            )
        except HTTPError as e:
            body = getattr(e.response, "text", "")[:400]
            return await query.edit_message_text(f"Помилка створення платежу: {e}\n{body}")

        # ===== Формування pay_url =====
        pay_url = resp.get("pay_url") or resp.get("invoiceUrl")
        order_id = resp.get("order_id")

        # Якщо API віддав data+signature — самі збираємо checkout URL
        if not pay_url and resp.get("data") and resp.get("signature"):
            pay_url = f"https://www.liqpay.ua/api/3/checkout?data={resp['data']}&signature={resp['signature']}"

        # Резервний варіант: /pay/{order_id} на публічному домені
        if not pay_url and order_id:
            pay_url = f"{PUBLIC_BASE}/pay/{order_id}"

        if not pay_url:
            preview = (str(resp)[:400]).replace("\n", " ")
            log.error("No pay_url returned. Resp=%s", resp)
            return await query.edit_message_text(
                "Не отримав посилання на оплату. "
                f"Відповідь бекенду: {preview}"
            )

        log.info("Sending pay_url to user %s: %s (order_id=%s)", uid, pay_url, order_id)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("💳 Оплатити (LiqPay)", url=pay_url)]])
        await context.bot.send_message(
            chat_id=uid,
            text=(f"Рахунок створено на {amount_uah}₴.\n"
                  f"Тисни кнопку нижче або відкрий лінк вручну:\n{pay_url}"),
            reply_markup=kb
        )
        try:
            await query.edit_message_text("Рахунок створено, дивись наступне повідомлення з кнопкою.")
        except Exception:
            pass
        return

    # --- Платні дії (backlinks) ---
    if len(data) != 3:
        return await query.edit_message_text("Невірний запит.")
    action, domain, scope = data

    need_credits = _uah_to_credits(BACKLINKS_CHARGE_UAH)

    if not charge(uid, need_credits, domain, scope):
        rows = []
        for amount in TOPUP_OPTIONS:
            credits = int(amount // CREDIT_PRICE_UAH)
            rows.append([InlineKeyboardButton(f"💳 Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])
        return await query.edit_message_text(
            f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
            reply_markup=InlineKeyboardMarkup(rows)
        )

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

# ====== Обробка меню ======
async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    if text == "🔗 Backlinks":
        return await update.message.reply_text("Введіть команду у форматі: /backlinks yourdomain.com")
    if text == "💳 Поповнити":
        return await topup_providers(update, context)
    if text == "📊 Баланс":
        return await balance(update, context)
    if text == "📱 Реєстрація":
        if _registered(uid):
            return await update.message.reply_text("Ви вже зареєстровані ✅", reply_markup=main_menu_keyboard(True))
        return await register_cmd_or_menu(update, context)

# ====== АДМІНКА ======
def _db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def _admin_check(uid: int) -> bool:
    return uid in ADMIN_IDS

def _render_users_page(page: int) -> str:
    offset = (page - 1) * PAGE_SIZE
    with _db() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM users")
        total = cur.fetchone()[0]
        cur = conn.execute(
            "SELECT user_id, balance, COALESCE(phone,'') FROM users ORDER BY user_id LIMIT ? OFFSET ?",
            (PAGE_SIZE, offset),
        )
        rows = cur.fetchall()

    if total == 0:
        return "Користувачів ще немає."

    import math as _math
    lines = [f"👤 Користувачі (всього: {total}) | сторінка {page}/{max(1, _math.ceil(total / PAGE_SIZE))}"]
    for uid, bal, phone in rows:
        phone_disp = phone if phone else "—"
        lines.append(f"• {uid}: баланс {bal}, телефон {phone_disp}")
    return "\n".join(lines)

def _admin_kb(page: int) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton("⬅️ Назад", callback_data=f"admin|page|{page-1}")] if page > 1 else []
    buttons += [
        InlineKeyboardButton("↻ Оновити", callback_data=f"admin|page|{page}"),
        InlineKeyboardButton("Вперед ➡️", callback_data=f"admin|page|{page+1}")
    ]
    return InlineKeyboardMarkup([buttons])

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not _admin_check(uid):
        return await update.message.reply_text("⛔️ Доступ заборонено.")
    text = _render_users_page(1)
    await update.message.reply_text(text, reply_markup=_admin_kb(1))

async def on_admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    if not _admin_check(uid):
        return await query.edit_message_text("⛔️ Доступ заборонено.")
    parts = (query.data or "").split("|")
    if len(parts) == 3 and parts[0] == "admin" and parts[1] == "page":
        try:
            page = max(1, int(parts[2]))
        except Exception:
            page = 1
        text = _render_users_page(page)
        return await query.edit_message_text(text, reply_markup=_admin_kb(page))

# ====== MAIN ======
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("topup", topup_providers))  # /topup тепер відкриває вибір провайдера
    app.add_handler(CommandHandler("backlinks", backlinks))

    # Реєстрація — розмова
    reg_conv = ConversationHandler(
        entry_points=[
            CommandHandler("register", register_cmd_or_menu),
            MessageHandler(filters.Regex(r"^📱 Реєстрація$"), register_cmd_or_menu),
        ],
        states={WAIT_PHONE: [MessageHandler(filters.CONTACT, on_contact_register)]},
        fallbacks=[CommandHandler("cancel", cancel_register)],
        allow_reentry=True,
    )
    app.add_handler(reg_conv)

    # Адмінка
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(on_admin_cb, pattern=r"^admin\|"))

    # Callback’и (providers / liqpay amounts / liqpay topup / backlinks)
    app.add_handler(CallbackQueryHandler(on_choice))

    # Меню-тексти
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_menu_text))

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s PUBLIC_BASE=%s", DFS_BASE, BACKEND_BASE, PUBLIC_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

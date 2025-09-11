# bot.py
import os
import csv
import io
import math
import uuid
import logging

from dotenv import load_dotenv
from telegram import (
    Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    CallbackQueryHandler, MessageHandler, filters
)
from httpx import HTTPError, AsyncClient, ConnectError

from dataforseo import DataForSEO
from dao import (
    init_db, ensure_user, get_balance, charge, get_phone, register_or_update_phone,
    list_users, count_users, get_user, add_credits
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# === ENV ===
load_dotenv()
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

DFS_LOGIN = os.environ["DATAFORSEO_LOGIN"]
DFS_PASS = os.environ["DATAFORSEO_PASSWORD"]
DFS_BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")

BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")

CREDIT_PRICE_UAH = float(os.getenv("CREDIT_PRICE_UAH", "5"))
CHARGE_BACKLINKS_UAH = float(os.getenv("CHARGE_BACKLINKS_UAH", "5"))
CHARGE_BACKLINKS_CREDITS = max(1, math.ceil(CHARGE_BACKLINKS_UAH / CREDIT_PRICE_UAH))

TOPUP_OPTIONS = [int(x.strip()) for x in os.getenv("TOPUP_OPTIONS", "100,250,500").split(",") if x.strip().isdigit()]

# адміни (через кому): ADMIN_IDS=123,456
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(";", ",").split(",") if x.strip().isdigit()}

# === INIT ===
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE)

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

def _main_menu_kb() -> ReplyKeyboardMarkup:
    rows = [
        ["🔎 Backlinks", "💳 Поповнити"],
        ["📱 Реєстрація", "💼 Баланс"],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# === PUBLIC COMMANDS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    phone = get_phone(uid)
    reg_line = "Статус реєстрації: ✅ телефон додано" if phone else "Статус реєстрації: ❌ немає телефону (використайте /register)"
    extra = "\n/admin — адмін-меню" if uid in ADMIN_IDS else ""
    await update.message.reply_text(
        "Привіт! Я SEO-бот з балансом.\n"
        "/register — додати номер телефону (новим користувачам бонус)\n"
        "/backlinks <домен> — отримати дані беклінків (списання ~"
        f"{CHARGE_BACKLINKS_CREDITS} кредит/ів ≈ {CHARGE_BACKLINKS_UAH:.0f} грн)\n"
        "/balance — показати баланс\n"
        "/topup — поповнити баланс через LiqPay"
        f"{extra}\n\n"
        f"{reg_line}\nВаш баланс: {bal} кредитів",
        reply_markup=_main_menu_kb()
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
    is_new, credited = register_or_update_phone(uid, contact.phone_number, initial_bonus=int(os.getenv("INITIAL_BONUS", "10")))
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
    rows = []
    for amount in TOPUP_OPTIONS:
        credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([InlineKeyboardButton(f"Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}")])
    await update.message.reply_text("Оберіть суму поповнення:", reply_markup=InlineKeyboardMarkup(rows))

async def backlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.strip().lower() in ("🔎 backlinks", "backlinks"):
        return await update.message.reply_text("Введіть: /backlinks yourdomain.com")

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
        f"Домен: {domain}\nКожна дія списує ~{CHARGE_BACKLINKS_CREDITS} кредит/ів (≈ {CHARGE_BACKLINKS_UAH:.0f} грн). Оберіть:",
        reply_markup=InlineKeyboardMarkup(kb),
    )

# === CALLBACKS (покупки + backlinks) ===
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
        except Exception as e:
            log.exception("Create invoice error")
            return await query.edit_message_text(f"Помилка створення рахунку: {e}")

        url = resp["invoiceUrl"]
        kb = [[InlineKeyboardButton("💳 Оплатити (LiqPay)", url=url)]]
        return await query.edit_message_text(
            f"Рахунок створено на {amount_uah}₴. Натисніть, щоб оплатити:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # Backlinks — фіксоване списання
    try:
        action, domain, scope = data
    except ValueError:
        return await query.edit_message_text("Невірний запит.")

    cost = CHARGE_BACKLINKS_CREDITS
    if not charge(uid, cost, domain, f"{action}:{scope}"):
        rows = []
        for amount in TOPUP_OPTIONS:
            credits = int(amount // CREDIT_PRICE_UAH)
            rows.append([InlineKeyboardButton(
                f"💳 Поповнити {amount}₴ (~{credits} кредитів)", callback_data=f"topup|{amount}"
            )])
        return await query.edit_message_text(
            f"Недостатньо кредитів (потрібно {cost}). Поповніть баланс.",
            reply_markup=InlineKeyboardMarkup(rows)
        )

    limit = 10 if scope == "10" else 1000
    try:
        data = await dfs.backlinks_live(domain, limit=limit, order_by="first_seen,desc")
        items = _extract_items(data)
        if not items:
            new_bal = get_balance(uid)
            await query.edit_message_text("Нічого не знайшов 😕")
            return await query.message.reply_text(f"Ваш новий баланс: {new_bal} кредитів")

        if action == "show":
            cap = 10 if scope == "10" else 50
            txt = _fmt_preview(items, cap)
            if scope == "all" and len(items) > cap:
                txt += f"\n\n…показано перші {cap} з {len(items)}. Оберіть CSV (всі), щоб завантажити повний список."
            await query.edit_message_text(txt)
            await query.message.reply_text(f"✅ Операція виконана. Новий баланс: {get_balance(uid)} кредитів")
        elif action == "csv":
            csv_bytes = _items_to_csv_bytes(items)
            await query.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_{scope}.csv"),
                caption=f"Експорт для {domain} ({'10' if scope=='10' else 'all'})"
            )
            await query.message.reply_text(f"✅ Файл надіслано. Новий баланс: {get_balance(uid)} кредитів")
        else:
            await query.edit_message_text("Невідома дія.")
            await query.message.reply_text(f"Новий баланс: {get_balance(uid)} кредитів")

    except HTTPError as e:
        log.exception("HTTP error")
        await query.edit_message_text(f"DataForSEO HTTP error: {e}")
        await query.message.reply_text(f"Новий баланс: {get_balance(uid)} кредитів")
    except Exception as e:
        log.exception("Unexpected error")
        await query.edit_message_text(f"Помилка: {e}")
        await query.message.reply_text(f"Новий баланс: {get_balance(uid)} кредитів")

# === АДМІНКА ===
def _admin_kb(page: int = 0, total: int = 0, page_size: int = 10):
    buttons = [[InlineKeyboardButton("👥 Користувачі", callback_data=f"admin|list|{page}")],
               [InlineKeyboardButton("📤 Експорт CSV", callback_data="admin|export")]]
    # пагінація тільки якщо ми в списку
    pages = max(1, math.ceil(total / page_size)) if total else 1
    if total:
        prev_page = (page - 1) % pages
        next_page = (page + 1) % pages
        buttons.append([
            InlineKeyboardButton("⬅️", callback_data=f"admin|list|{prev_page}"),
            InlineKeyboardButton(f"{page+1}/{pages}", callback_data=f"noop"),
            InlineKeyboardButton("➡️", callback_data=f"admin|list|{next_page}")
        ])
    return InlineKeyboardMarkup(buttons)

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        return await update.message.reply_text("⛔ Нема доступу.")
    total = count_users()
    await update.message.reply_text(
        f"Адмін-меню. Користувачів: {total}",
        reply_markup=_admin_kb(page=0, total=total)
    )

async def on_admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        return await update.callback_query.answer("⛔ Нема доступу.", show_alert=True)

    q = update.callback_query
    parts = q.data.split("|")

    if parts[0] == "noop":
        return await q.answer()

    if parts[0] != "admin":
        return

    action = parts[1]

    if action == "list":
        page = int(parts[2]) if len(parts) > 2 else 0
        page_size = 10
        offset = page * page_size
        users = list_users(offset=offset, limit=page_size)
        total = count_users()

        if not users:
            return await q.edit_message_text(
                "Користувачів поки немає.",
                reply_markup=_admin_kb(page=page, total=total, page_size=page_size)
            )

        lines = []
        for u in users:
            uid2 = u["user_id"]
            bal = u["balance"]
            phone = u["phone"] or "—"
            lines.append(f"• <b>{uid2}</b> | баланс: <b>{bal}</b> | 📱 {phone}")
        text = "Список користувачів (останні):\n" + "\n".join(lines) + "\n\nНатисніть на користувача, щоб відкрити картку."
        # додамо ряд кнопок з user_id (по 5 у ряд)
        btns = []
        row = []
        for idx, u in enumerate(users, start=1):
            row.append(InlineKeyboardButton(str(u["user_id"]), callback_data=f"admin|user|{u['user_id']}"))
            if idx % 5 == 0:
                btns.append(row)
                row = []
        if row:
            btns.append(row)
        # + пагінація/меню
        base = _admin_kb(page=page, total=total, page_size=page_size).inline_keyboard
        kb = InlineKeyboardMarkup(btns + base)
        return await q.edit_message_text(text, reply_markup=kb, parse_mode="HTML")

    if action == "user":
        uid2 = int(parts[2])
        u = get_user(uid2)
        if not u:
            return await q.answer("Користувача не знайдено", show_alert=True)
        txt = (
            f"👤 <b>{u['user_id']}</b>\n"
            f"Баланс: <b>{u['balance']}</b>\n"
            f"Телефон: <b>{u['phone'] or '—'}</b>\n"
            f"Створено: {u['created_at']}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ +10 кредитів", callback_data=f"admin|give|{uid2}|10"),
             InlineKeyboardButton("➖ -10 кредитів", callback_data=f"admin|give|{uid2}|-10")],
            [InlineKeyboardButton("⬅️ Назад до списку", callback_data="admin|list|0")]
        ])
        return await q.edit_message_text(txt, reply_markup=kb, parse_mode="HTML")

    if action == "give":
        uid2 = int(parts[2])
        delta = int(parts[3])
        ok = add_credits(uid2, delta, reason="admin_panel")
        if not ok:
            return await q.answer("Не вдалось змінити баланс", show_alert=True)
        u = get_user(uid2)
        txt = (
            f"✅ Баланс змінено.\n\n"
            f"👤 <b>{u['user_id']}</b>\n"
            f"Новий баланс: <b>{u['balance']}</b>\n"
            f"Телефон: <b>{u['phone'] or '—'}</b>"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ +10", callback_data=f"admin|give|{uid2}|10"),
             InlineKeyboardButton("➖ -10", callback_data=f"admin|give|{uid2}|-10")],
            [InlineKeyboardButton("⬅️ Назад до списку", callback_data="admin|list|0")]
        ])
        return await q.edit_message_text(txt, reply_markup=kb, parse_mode="HTML")

    if action == "export":
        # зливаємо всіх у CSV
        total = count_users()
        page_size = 500
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["user_id", "balance", "phone", "created_at"])
        fetched = 0
        page = 0
        while fetched < total:
            users = list_users(offset=page * page_size, limit=page_size)
            for u in users:
                w.writerow([u["user_id"], u["balance"], u["phone"] or "", u["created_at"]])
            fetched += len(users)
            page += 1

        data = out.getvalue().encode()
        await q.message.reply_document(
            document=InputFile(io.BytesIO(data), filename="users_export.csv"),
            caption=f"Експорт {total} користувачів"
        )
        return await q.answer("Готово!")

# === TEXT MENU ===
async def _on_text_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip().lower()
    if txt in ("💼 баланс", "баланс", "/balance"):
        return await balance(update, context)
    if txt in ("📱 реєстрація", "реєстрація", "/register"):
        return await register(update, context)
    if txt in ("💳 поповнити", "поповнити", "/topup"):
        return await topup(update, context)
    if txt in ("🔎 backlinks", "backlinks", "/backlinks"):
        return await backlinks(update, context)
    if txt in ("/admin", "admin") and update.effective_user.id in ADMIN_IDS:
        # зручність: якщо набрали "admin" текстом
        return await admin_cmd(update, context)

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # публічні
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(MessageHandler(filters.CONTACT, on_contact))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("topup", topup))
    app.add_handler(CommandHandler("backlinks", backlinks))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _on_text_menu))
    app.add_handler(CallbackQueryHandler(on_choice))

    # адмін
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(on_admin_cb, pattern="^(admin\\||noop$)"))

    log.info(
        "Bot started. DFS_BASE=%s BACKEND_BASE=%s CREDIT_PRICE_UAH=%.2f CHARGE_BACKLINKS_UAH=%.2f (= %d credits) ADMIN_IDS=%s",
        DFS_BASE, BACKEND_BASE, CREDIT_PRICE_UAH, CHARGE_BACKLINKS_UAH, CHARGE_BACKLINKS_CREDITS, ADMIN_IDS
    )
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

import os
import io
import re
import csv
import math
import logging
import sqlite3
import zipfile
import asyncio
from telegram.error import TelegramError, BadRequest
from typing import List, Optional, Tuple

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

# ====== ENV / допоміжні парсери ======
def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    m = re.search(r"[-+]?\d*\.?\d+", raw)
    try:
        return float(m.group(0)) if m else float(default)
    except Exception:
        return float(default)

def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    m = re.search(r"[-+]?\d+", raw)
    try:
        return int(m.group(0)) if m else int(default)
    except Exception:
        return int(default)

def _parse_int_list_env(name: str, fallback: str = "100,250,500") -> List[int]:
    raw = os.getenv(name, fallback)
    nums = re.findall(r"\d+", raw)
    res = []
    for n in nums:
        try:
            v = int(n)
            if v > 0:
                res.append(v)
        except Exception:
            continue
    return res or [100, 250, 500]

# ====== ENV ======
load_dotenv()
TELEGRAM_BOT_URL     = os.getenv("TELEGRAM_BOT_URL", "")
TELEGRAM_START_PARAM = os.getenv("TELEGRAM_START_PARAM", "paid")

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DFS_LOGIN = os.environ.get("DATAFORSEO_LOGIN", "")
DFS_PASS  = os.environ.get("DATAFORSEO_PASSWORD", "")
DFS_BASE  = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")

BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")
PUBLIC_BASE  = os.getenv("PUBLIC_BASE", "https://server1.seoswiss.online").rstrip("/")

CREDIT_PRICE_UAH = _parse_float_env("CREDIT_PRICE_UAH", 5.0)
INITIAL_BONUS    = _parse_int_env("INITIAL_BONUS", 10)
TOPUP_OPTIONS    = _parse_int_list_env("TOPUP_OPTIONS", "100,250,500")

# ціни інструментів (грн → кредити)
SERP_CHARGE_UAH                   = _parse_float_env("SERP_CHARGE_UAH", 5.0)
KW_IDEAS_CHARGE_UAH               = _parse_float_env("KW_IDEAS_CHARGE_UAH", 5.0)
GAP_CHARGE_UAH                    = _parse_float_env("GAP_CHARGE_UAH", 10.0)
BACKLINKS_CHARGE_UAH              = _parse_float_env("BACKLINKS_CHARGE_UAH", 5.0)
BACKLINKS_FULL_EXPORT_CHARGE_UAH  = _parse_float_env("BACKLINKS_FULL_EXPORT_CHARGE_UAH", 5.0)
AUDIT_CHARGE_UAH                  = _parse_float_env("AUDIT_CHARGE_UAH", 5.0)

# налаштування експорту
CSV_MAX                 = _parse_int_env("CSV_MAX", 1000)
BACKLINKS_PAGE_SIZE     = _parse_int_env("BACKLINKS_PAGE_SIZE", 1000)
MAX_BACKLINKS_EXPORT    = _parse_int_env("MAX_BACKLINKS_EXPORT", 200000)
BACKLINKS_PART_ROWS     = _parse_int_env("BACKLINKS_CSV_PART_ROWS", 50000)

# для адмінки
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH   = os.getenv("DB_PATH", "bot.db")

PREVIEW_COUNT = 10
PAGE_SIZE     = 20
WAIT_PHONE    = 10

# ====== INIT ======
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE) if DFS_LOGIN and DFS_PASS else None

# ====== Утиліти ======
def main_menu_keyboard(registered: bool) -> ReplyKeyboardMarkup:
    if registered:
        rows = [
            [KeyboardButton("🧰 Сервіси"), KeyboardButton("💳 Поповнити")],
            [KeyboardButton("📊 Баланс")],
        ]
    else:
        rows = [
            [KeyboardButton("🧰 Сервіси"), KeyboardButton("💳 Поповнити")],
            [KeyboardButton("📊 Баланс"), KeyboardButton("📱 Реєстрація")],
        ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def services_menu_keyboard() -> ReplyKeyboardMarkup:
    """
    Нижнє меню сервісів: без Поповнити/Баланс, з кнопкою Назад.
    """
    rows = [
        [KeyboardButton("🔍 SERP"), KeyboardButton("🧠 Keyword Ideas")],
        [KeyboardButton("⚔️ Gap"), KeyboardButton("🔗 Backlinks")],
        [KeyboardButton("🛠️ Аудит"), KeyboardButton("⬅️ Назад")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

async def _set_menu_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE, kb: ReplyKeyboardMarkup):
    """
    Перемикає нижнє (reply) меню:
    1) надсилаємо непомітне повідомлення з потрібною ReplyKeyboardMarkup
    2) через мить видаляємо це повідомлення — клавіатура залишиться активною
    """
    chat_id = update.effective_chat.id

    # Надсилаємо плейсхолдер з новою reply-клавою
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text="·",
        reply_markup=kb,
        disable_notification=True,
        allow_sending_without_reply=True,
    )

    # Акуратно видаляємо плейсхолдер (клава залишиться)
    try:
        await asyncio.sleep(0.25)
        await context.bot.delete_message(chat_id=chat_id, message_id=msg.message_id)
    except TelegramError:
        pass
    # --- Повернення з інлайн-меню сервісів ---
    if cmd == "services_back":
        # Просто підкажемо і повернемо нижнє головне меню
        await query.edit_message_text("Повернувся до головного меню. Користуйся нижніми кнопками ⬇️")
        await _set_menu_keyboard(update, context, main_menu_keyboard(_registered(uid)))
        return

def _extract_first_items(resp: dict) -> List[dict]:
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

def _extract_result(resp: dict) -> dict:
    tasks = resp.get("tasks") or []
    if not tasks:
        return {}
    t = tasks[0] or {}
    if t.get("status_code") and t["status_code"] != 20000:
        raise RuntimeError(t.get("status_message") or f"Task error: {t.get('status_code')}")
    res = t.get("result") or []
    return res[0] if res else {}

def _uah_to_credits(amount_uah: float) -> int:
    return max(1, math.ceil(amount_uah / CREDIT_PRICE_UAH))

def _registered(uid: int) -> bool:
    return bool(get_phone(uid))

def _provider_label(provider: str) -> str:
    return "LiqPay" if provider == "liqpay" else ("WayForPay" if provider in ("wayforpay", "wfp") else provider)

def _topup_cta() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("💳 LiqPay", callback_data="open_amounts|liqpay")],
        [InlineKeyboardButton("🏦 WayForPay", callback_data="open_amounts|wayforpay")],
    ]
    return InlineKeyboardMarkup(rows)

def _parse_opts(line: str) -> Tuple[str, dict]:
    parts = [p.strip() for p in line.split("|")]
    main = parts[0] if parts else ""
    opts = {}
    for p in parts[1:]:
        m = re.match(r"([a-zA-Z_]+)\s*=\s*(.+)", p)
        if m:
            opts[m.group(1).lower()] = m.group(2).strip()
    return main, opts

def _write_backlink_rows(writer: csv.writer, items: List[dict]):
    for it in items:
        writer.writerow([
            (it.get("page_from") or {}).get("url_from") or it.get("url_from"),
            it.get("url_to"),
            (it.get("anchor") or "").replace("\n", " ").strip(),
            it.get("dofollow"),
            it.get("first_seen"),
            it.get("last_visited"),
            it.get("domain_from"),
        ])

# ====== Клавіатури оплати ======
def _build_topup_amounts_kb(provider: str) -> InlineKeyboardMarkup:
    rows = []
    for amount in TOPUP_OPTIONS:
        credits = int(amount // CREDIT_PRICE_UAH)
        rows.append([
            InlineKeyboardButton(
                f"💳 Поповнити {amount}₴ (~{credits} кредитів)",
                callback_data=f"topup|{provider}|{amount}"
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="topup_providers")])
    return InlineKeyboardMarkup(rows)

def _providers_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 LiqPay", callback_data="open_amounts|liqpay")],
        [InlineKeyboardButton("🏦 WayForPay", callback_data="open_amounts|wayforpay")],
    ])

# ====== Сервіси (інлайн-меню) ======
def _services_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Топ-10 Google (SERP)", callback_data="svc|serp")], 
        [InlineKeyboardButton("🧠 Ідеї ключових + обсяг/CPC", callback_data="svc|keywords")],
        [InlineKeyboardButton("⚔️ Keyword Gap", callback_data="svc|gap")],
        [InlineKeyboardButton("🔗 Backlinks огляд", callback_data="svc|backlinks_ov")],
        [InlineKeyboardButton("🛠️ Аудит URL (On-Page)", callback_data="svc|audit")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="services_back")],
    ])

async def services_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🧰 *Сервіси*\n\nОбери інструмент. "
        "Після кліку надішліть дані в одному рядку з опціями через `|`.\n\n"
        "Приклади:\n"
        "• SERP: `iphone 13 | country=Ukraine | lang=Ukrainian | depth=10`\n"
        "• Ідеї ключових: `seo tools | country=Ukraine | lang=Ukrainian | limit=20`\n"
        "• Gap: `mydomain.com | comps=site1.com,site2.com | country=Ukraine | lang=Ukrainian | limit=50`\n"
        "• Backlinks огляд: `mydomain.com`\n"
        "• Аудит: `https://example.com/page`"
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=_services_kb(), disable_web_page_preview=True, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(text, reply_markup=_services_kb(), disable_web_page_preview=True, parse_mode="Markdown")

# ====== /start ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    reg = _registered(uid)
    reg_text = "✅ телефон додано" if reg else "❌ немає телефону (використайте Реєстрація)"

    raw = (update.message.text or "").strip()
    param: Optional[str] = None
    if raw.startswith("/start"):
        parts = raw.split(maxsplit=1)
        if len(parts) == 2:
            param = parts[1].strip()

    if param == TELEGRAM_START_PARAM:
        msg = await update.message.reply_text(
            "Дякуємо! Якщо платіж пройшов, баланс оновиться протягом хвилини.\n"
            "Перевірте /balance або натисніть «📊 Баланс».",
            reply_markup=main_menu_keyboard(reg)
        )
        context.chat_data["menu_msg_id"] = msg.message_id
        context.chat_data["in_services"] = False
        return

    text = (
        "Привіт! Я SEO-бот з балансом.\n\n"
        "Меню:\n"
        "🧰 Сервіси — SERP, Keywords, Gap, Backlinks, Audit\n"
        "💳 Поповнити — LiqPay або WayForPay\n"
        "📊 Баланс — ваші кредити\n"
        "📱 Реєстрація — додати телефон (новим — бонус)\n\n"
        f"Статус реєстрації: {reg_text}\n"
        f"Ваш баланс: {bal} кредитів"
    )
    msg = await update.message.reply_text(text, reply_markup=main_menu_keyboard(reg))
    context.chat_data["menu_msg_id"] = msg.message_id
    context.chat_data["in_services"] = False

# ====== Реєстрація ======
def _normalize_phone(p: str) -> str:
    digits = "".join(ch for ch in p if ch.isdigit())
    if not digits:
        return p
    if p.strip().startswith("+"):
        return p
    return "+" + digits

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

# ====== Поповнення ======
async def topup_providers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = _providers_kb()
    text = "💰 *Поповнення балансу*\n\nОберіть провайдера оплати."
    if update.message:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")

async def open_amounts(update: Update, context: ContextTypes.DEFAULT_TYPE, provider: str):
    label = _provider_label(provider)
    msg = f"Оберіть суму поповнення ({label}):"
    kb = _build_topup_amounts_kb(provider)
    if update.message:
        await update.message.reply_text(msg, reply_markup=kb)
    else:
        await update.callback_query.edit_message_text(msg, reply_markup=kb)

# ====== Backlinks (команда з кнопками/експортом) ======
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

# ====== CALLBACKS (services entry, topup, backlinks) ======
async def on_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = update.effective_user.id
    raw = (query.data or "").strip()
    log.info("CB <- %s", raw)

    parts = raw.split("|")
    if not parts:
        try:
            return await query.edit_message_text("Кнопка застаріла. Відкрийте меню ще раз.")
        except Exception:
            return

    cmd = parts[0]

    # --- Сервіси (вхід у wizard) ---
    if cmd == "svc":
        tool = parts[1] if len(parts) > 1 else ""
        context.user_data["await_tool"] = tool
        prompts = {
            "serp": "🔍 SERP: введіть запит. Опційно: `| country=Ukraine | lang=Ukrainian | depth=10`",
            "keywords": "🧠 Ідеї ключових: введіть seed. Опційно: `| country=Ukraine | lang=Ukrainian | limit=20`",
            "gap": "⚔️ Gap: `mydomain.com | comps=site1.com,site2.com | country=Ukraine | lang=Ukrainian | limit=50`",
            "backlinks_ov": "🔗 Backlinks огляд: введіть домен: `mydomain.com`",
            "audit": "🛠️ Аудит: введіть URL: `https://example.com/page`",
        }
        text = prompts.get(tool, "Надішліть параметри в одному рядку.")
        return await query.edit_message_text(text, disable_web_page_preview=True, parse_mode="Markdown")

    # --- Екран вибору провайдера / повернення ---
    if cmd == "topup_providers":
        return await topup_providers(update, context)

    # --- Вибір сум для провайдера ---
    if cmd == "open_amounts":
        provider = (parts[1] if len(parts) > 1 else "liqpay").lower()
        return await open_amounts(update, context, provider)

    # --- Поповнення (створення інвойсу) ---
    if cmd == "topup":
        provider = (parts[1] if len(parts) > 1 else "liqpay").lower()
        amount_raw = parts[2] if len(parts) > 2 else ""
        amount_clean = re.sub(r"[^\d.]", "", str(amount_raw))
        try:
            amount_uah = int(float(amount_clean))
            if amount_uah <= 0:
                raise ValueError
        except Exception:
            try:
                return await query.edit_message_text("Невірна сума. Оберіть її заново через «💳 Поповнити».")
            except Exception:
                return

        try:
            async with AsyncClient(timeout=20) as c:
                r = await c.post(
                    f"{BACKEND_BASE}/api/payments/create",
                    json={"user_id": uid, "amount": amount_uah, "provider": provider}
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

        pay_url = resp.get("pay_url") or resp.get("invoiceUrl")
        order_id = resp.get("order_id")

        if not pay_url and resp.get("data") and resp.get("signature"):
            pay_url = f"https://www.liqpay.ua/api/3/checkout?data={resp['data']}&signature={resp['signature']}"

        if not pay_url and order_id:
            pay_url = f"{PUBLIC_BASE}/pay/{order_id}"

        if not pay_url:
            preview = (str(resp)[:400]).replace("\n", " ")
            log.error("No pay_url returned. Resp=%s", resp)
            return await query.edit_message_text(
                "Не отримав посилання на оплату. "
                f"Відповідь бекенду: {preview}"
            )

        label = _provider_label(provider)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"💳 Оплатити ({label})", url=pay_url)]])
        await context.bot.send_message(
            chat_id=uid,
            text=(f"Рахунок створено на {amount_uah}₴ ({label}).\n"
                  f"Натисніть кнопку нижче або відкрийте лінк:\n{pay_url}"),
            reply_markup=kb
        )
        try:
            await query.edit_message_text("Рахунок створено, дивись повідомлення з кнопкою нижче ⬇️")
        except Exception:
            pass
        return

    # --- Старі платні дії (backlinks list/CSV через /backlinks) ---
    if cmd in ("show", "csv") and len(parts) == 3:
        if not dfs:
            return await query.edit_message_text("DataForSEO не сконфігуровано. Додайте логін/пароль у .env")

        _, domain, scope = parts
        uah_cost = BACKLINKS_FULL_EXPORT_CHARGE_UAH if scope == "all" and cmd == "csv" else BACKLINKS_CHARGE_UAH
        need_credits = _uah_to_credits(uah_cost)

        if not charge(uid, need_credits, domain, f"{cmd}:{scope}"):
            return await query.edit_message_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )

        try:
            if scope != "all":
                limit = PREVIEW_COUNT if scope == "10" else CSV_MAX
                data_resp = await dfs.backlinks_live(domain, limit=limit, order_by="first_seen,desc")
                items = _extract_first_items(data_resp)
                if not items:
                    bal_now = get_balance(uid)
                    return await query.edit_message_text(f"Нічого не знайшов 😕\nВаш новий баланс: {bal_now} кредитів")

                if cmd == "show":
                    cap = PREVIEW_COUNT if scope == "10" else min(50, len(items))
                    lines = []
                    for it in items[:cap]:
                        url_from = (it.get("page_from") or {}).get("url_from") or it.get("url_from")
                        anchor = (it.get("anchor") or "").strip()
                        first_seen = it.get("first_seen")
                        lines.append(f"• {url_from}\n  anchor: {anchor[:80]} | first_seen: {first_seen}")
                    txt = "\n".join(lines)
                    bal_now = get_balance(uid)
                    if scope != "10" and len(items) > cap:
                        txt += f"\n\n…показано перші {cap} з {len(items)}."
                    txt += f"\n\n💰 Списано {need_credits} кредит(и). Новий баланс: {bal_now}"
                    await query.edit_message_text(txt)
                else:
                    buf = io.StringIO()
                    w = csv.writer(buf)
                    w.writerow(["url_from", "url_to", "anchor", "dofollow", "first_seen", "last_seen", "domain_from"])
                    _write_backlink_rows(w, items)
                    csv_bytes = buf.getvalue().encode()
                    bal_now = get_balance(uid)
                    await query.message.reply_document(
                        document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_{scope}.csv"),
                        caption=f"Експорт для {domain} ({scope})\n💰 Списано {need_credits}. Новий баланс: {bal_now}"
                    )
                    await query.edit_message_text("Готово ✅")
                return

            # повний експорт
            items_all, total = await dfs.backlinks_all(
                domain, order_by="first_seen,desc", page_size=BACKLINKS_PAGE_SIZE, max_total=MAX_BACKLINKS_EXPORT
            )
            count = len(items_all)
            if count == 0:
                bal_now = get_balance(uid)
                return await query.edit_message_text(f"Нічого не знайшов 😕\nВаш новий баланс: {bal_now} кредитів")

            if count > BACKLINKS_PART_ROWS:
                zip_buf = io.BytesIO()
                with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    start = 0
                    part = 1
                    while start < count:
                        chunk = items_all[start:start + BACKLINKS_PART_ROWS]
                        tmp_io = io.StringIO()
                        w = csv.writer(tmp_io)
                        w.writerow(["url_from", "url_to", "anchor", "dofollow", "first_seen", "last_seen", "domain_from"])
                        _write_backlink_rows(w, chunk)
                        zf.writestr(f"{domain}_backlinks_part{part}.csv", tmp_io.getvalue())
                        start += BACKLINKS_PART_ROWS
                        part += 1
                zip_bytes = zip_buf.getvalue()
                bal_now = get_balance(uid)
                await query.message.reply_document(
                    document=InputFile(io.BytesIO(zip_bytes), filename=f"{domain}_backlinks_full.zip"),
                    caption=(f"Повний експорт для {domain}: {count} рядків (із ~{total}). "
                             f"ZIP з частинами по {BACKLINKS_PART_ROWS}.\n"
                             f"💰 Списано {need_credits}. Баланс: {bal_now}")
                )
                await query.edit_message_text("Готово ✅")
                return
            else:
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["url_from", "url_to", "anchor", "dofollow", "first_seen", "last_seen", "domain_from"])
                _write_backlink_rows(w, items_all)
                csv_bytes = buf.getvalue().encode()
                bal_now = get_balance(uid)
                await query.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_full.csv"),
                    caption=(f"Повний експорт для {domain}: {count} рядків (із ~{total}).\n"
                             f"💰 Списано {need_credits}. Баланс: {bal_now}")
                )
                await query.edit_message_text("Готово ✅")
                return

        except HTTPError as e:
            log.exception("HTTP error")
            await query.edit_message_text(f"DataForSEO HTTP error: {e}")
        except Exception as e:
            log.exception("Unexpected error")
            await query.edit_message_text(f"Помилка: {e}")
        return

    # --- Невідома кнопка ---
    try:
        return await query.edit_message_text("Кнопка застаріла або формат невірний. Відкрийте меню ще раз.")
    except Exception:
        return

# ====== Обробка меню (reply-клавіатура) + логіка Сервісів ======
async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    # Перемикач нижніх меню
    if text in ("🧰 Сервіси", "Сервіси"):
        context.chat_data["in_services"] = True
        await _set_menu_keyboard(update, context, services_menu_keyboard())
        return

    if text == "⬅️ Назад":
        context.chat_data["in_services"] = False
        await _set_menu_keyboard(update, context, main_menu_keyboard(_registered(uid)))
        return

    # Швидкий вибір сервісу (reply-кнопки)
    if text in ("🔍 SERP", "🧠 Keyword Ideas", "⚔️ Gap", "🔗 Backlinks", "🛠️ Аудит"):
        mapping = {
            "🔍 SERP": ("serp", "SERP: `iphone 13 | country=Ukraine | lang=Ukrainian | depth=10`"),
            "🧠 Keyword Ideas": ("keywords", "Keywords: `seo tools | country=Ukraine | lang=Ukrainian | limit=20`"),
            "⚔️ Gap": ("gap", "Gap: `mydomain.com | comps=site1.com,site2.com | country=Ukraine | lang=Ukrainian | limit=50`"),
            "🔗 Backlinks": ("backlinks_ov", "Backlinks: `mydomain.com`"),
            "🛠️ Аудит": ("audit", "Audit: `https://example.com/page`"),
        }
        tool, hint = mapping[text]
        context.user_data["await_tool"] = tool
        await update.message.reply_text(
            f"Окей, надішли параметри в одному рядку.\n\nПриклад:\n{hint}",
            parse_mode="Markdown",
            reply_markup=services_menu_keyboard()
        )
        return

    # Wizard для сервісів
    aw = context.user_data.get("await_tool")
    if aw:
        # зберігаємо, але очищаємо прапор (щоб кожен запит був одноразовим)
        context.user_data.pop("await_tool", None)

        if not dfs:
            return await update.message.reply_text("DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env")

        main, opts = _parse_opts(text)
        country = opts.get("country", "Ukraine")
        lang    = opts.get("lang", "Ukrainian")
        limit   = int(re.findall(r"\d+", opts.get("limit", "20"))[0]) if opts.get("limit") else 20
        prices  = {
            "serp": SERP_CHARGE_UAH,
            "keywords": KW_IDEAS_CHARGE_UAH,
            "gap": GAP_CHARGE_UAH,
            "backlinks_ov": BACKLINKS_CHARGE_UAH,
            "audit": AUDIT_CHARGE_UAH,
        }
        need_credits = _uah_to_credits(prices.get(aw, 5.0))

        if not charge(uid, need_credits, f"svc:{aw}", main or "-"):
            return await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )

        try:
            # ===== SERP =====
            if aw == "serp":
                depth = int(re.findall(r"\d+", opts.get("depth", "10"))[0]) if opts.get("depth") else 10
                resp = await dfs.serp_google_organic(main, location_name=country, language_name=lang, depth=depth)
                items = _extract_first_items(resp)
                if not items:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")

                lines = []
                for it in items[:10]:
                    rank = it.get("rank_group") or it.get("rank_absolute") or it.get("rank")
                    title = it.get("title") or it.get("domain") or "—"
                    url = it.get("url") or it.get("link") or "—"
                    itype = it.get("type") or it.get("serp_item_type")
                    suf = f" [{itype}]" if itype and str(itype).lower() != "organic" else ""
                    lines.append(f"{rank}. {title}\n{url}{suf}")
                preview = "🔍 *Топ-10 Google*\n" + "\n\n".join(lines)

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["rank", "title", "url", "type"])
                for it in items:
                    w.writerow([
                        it.get("rank_group") or it.get("rank_absolute") or it.get("rank"),
                        it.get("title") or it.get("domain") or "",
                        it.get("url") or it.get("link") or "",
                        it.get("type") or it.get("serp_item_type") or "",
                    ])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                await update.message.reply_text(preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}", parse_mode="Markdown")
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="serp_top.csv"),
                    caption="CSV із результатами SERP"
                )
                return

            # ===== Keywords Ideas =====
            if aw == "keywords":
                resp = await dfs.keywords_for_keywords(main, location_name=country, language_name=lang, limit=limit)
                items = _extract_first_items(resp)
                if not items:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")

                lines = []
                for it in items[:10]:
                    kw  = it.get("keyword") or it.get("keyword_text") or "—"
                    vol = it.get("search_volume") or it.get("avg_monthly_searches") or it.get("search_volume_avg") or "-"
                    cpc = it.get("cpc") or it.get("cost_per_click") or "-"
                    lines.append(f"• {kw} — vol: {vol}, CPC: {cpc}")
                preview = "🧠 *Ідеї ключових*\n" + "\n".join(lines)

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "cpc"])
                for it in items:
                    w.writerow([
                        it.get("keyword") or it.get("keyword_text") or "",
                        it.get("search_volume") or it.get("avg_monthly_searches") or it.get("search_volume_avg") or "",
                        it.get("cpc") or it.get("cost_per_click") or "",
                    ])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                await update.message.reply_text(preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}", parse_mode="Markdown")
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="keyword_ideas.csv"),
                    caption="CSV з ідеями ключових"
                )
                return

            # ===== Keyword Gap =====
            if aw == "gap":
                comps_raw = opts.get("comps") or opts.get("competitors") or ""
                competitors = [x.strip() for x in comps_raw.split(",") if x.strip()]
                if not main or not competitors:
                    return await update.message.reply_text("Формат: `mydomain.com | comps=site1.com,site2.com`", parse_mode="Markdown")

                # Виконуємо запит попарно до кожного конкурента
                rows = []
                for comp in competitors:
                    try:
                        resp = await dfs.domain_intersection_gap(main, comp, location_name=country, language_name=lang, limit=limit)
                    except AttributeError:
                        # якщо у твоєму клієнті інша назва — можна замінити на правильний метод
                        resp = await dfs.keywords_gap(main, [comp], location_name=country, language_name=lang, limit=limit)
                    items = _extract_first_items(resp)
                    for it in items:
                        kw  = it.get("keyword") or it.get("keyword_text") or ""
                        vol = it.get("search_volume") or it.get("avg_monthly_searches") or ""
                        my  = it.get("target_rank") or it.get("rank") or ""
                        comp_ranks = it.get("competitor_ranks") or it.get("ranks") or {}
                        rows.append((kw, vol, my, comp, comp_ranks))

                if not rows:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")

                lines = []
                for kw, vol, my, comp, comp_ranks in rows[:10]:
                    if isinstance(comp_ranks, dict):
                        comp_str = ", ".join(f"{k}:{v}" for k, v in list(comp_ranks.items())[:3])
                    elif isinstance(comp_ranks, list):
                        comp_str = ", ".join(str(x) for x in comp_ranks[:3])
                    else:
                        comp_str = "-"
                    lines.append(f"• {kw} — vol:{vol}, ми:{my}, vs {comp}: {comp_str}")
                preview = "⚔️ *Keyword Gap*\n" + "\n".join(lines)

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "our_rank", "competitor", "competitor_ranks"])
                for kw, vol, my, comp, comp_ranks in rows:
                    if isinstance(comp_ranks, dict):
                        comp_str = "; ".join(f"{k}:{v}" for k, v in comp_ranks.items())
                    elif isinstance(comp_ranks, list):
                        comp_str = "; ".join(str(x) for x in comp_ranks)
                    else:
                        comp_str = ""
                    w.writerow([kw, vol, my, comp, comp_str])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                await update.message.reply_text(preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}", parse_mode="Markdown")
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="keyword_gap.csv"),
                    caption="CSV з результатами Keyword Gap"
                )
                return

            # ===== Backlinks Overview =====
            if aw == "backlinks_ov":
                target = main
                summary = await dfs.backlinks_summary(target)
                s = _extract_result(summary)  # один блок result

                totals = s.get("totals") or {}
                backlinks = totals.get("backlinks") or s.get("backlinks") or "-"
                refdomains = totals.get("referring_domains") or s.get("referring_domains") or "-"
                dofollow = totals.get("dofollow") or s.get("dofollow") or "-"
                nofollow = totals.get("nofollow") or s.get("nofollow") or "-"

                rdom = await dfs.refdomains_live(target, limit=10, order_by="backlinks,desc")
                r_items = _extract_first_items(rdom)
                rd_lines = []
                for it in r_items[:10]:
                    d = it.get("domain") or it.get("referring_domain") or "-"
                    b = it.get("backlinks") or "-"
                    rd_lines.append(f"• {d} — {b} backlinks")

                anch = await dfs.anchors_live(target, limit=10, order_by="backlinks,desc")
                a_items = _extract_first_items(anch)
                a_lines = []
                for it in a_items[:10]:
                    a = it.get("anchor") or "-"
                    b = it.get("backlinks") or "-"
                    a_lines.append(f"• {a[:60]} — {b}")

                bal_now = get_balance(uid)
                txt = (
                    f"🔗 *Backlinks огляд для* **{target}**\n"
                    f"• Backlinks: {backlinks}\n"
                    f"• Referring domains: {refdomains}\n"
                    f"• Dofollow: {dofollow} | Nofollow: {nofollow}\n\n"
                    f"Топ реф.доменів:\n" + ("\n".join(rd_lines) or "—") + "\n\n"
                    f"Топ анкорів:\n" + ("\n".join(a_lines) or "—") + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}"
                )
                await update.message.reply_text(txt, parse_mode="Markdown")
                return

            # ===== Audit URL =====
            if aw == "audit":
                url = main
                res = await dfs.onpage_instant(url)
                r = _extract_result(res)
                items = r.get("items") or []
                it0 = items[0] if items else {}

                status = it0.get("status_code") or it0.get("status") or "-"
                meta = it0.get("meta") or {}
                content = it0.get("content") or {}

                title = meta.get("title") or ""
                desc  = meta.get("description") or ""
                canon = meta.get("canonical") or meta.get("canonical_url") or ""

                def _norm_h(x):
                    if isinstance(x, list):
                        return [str(i)[:120] for i in x if i]
                    if isinstance(x, str):
                        return [x[:120]]
                    return []
                h1 = _norm_h(meta.get("h1") or content.get("h1"))
                h2 = _norm_h(meta.get("h2") or content.get("h2"))

                lines = [
                    f"🛠️ *Аудит URL*",
                    f"URL: {url}",
                    f"Статус: {status}",
                    f"Title: {title[:160]}",
                    f"Description: {desc[:200]}",
                    f"Canonical: {canon or '—'}",
                    f"H1: {('; '.join(h1) if h1 else '—')}",
                    f"H2: {('; '.join(h2[:5]) if h2 else '—')}",
                ]
                bal_now = get_balance(uid)
                lines.append(f"\n💰 Списано {need_credits}. Баланс: {bal_now}")
                await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
                return

            # fallback
            bal_now = get_balance(uid)
            return await update.message.reply_text(f"Інструмент поки не реалізовано. Баланс: {bal_now}")

        except HTTPError as e:
            log.exception("DataForSEO HTTP error")
            return await update.message.reply_text(f"DataForSEO HTTP error: {e}")
        except Exception as e:
            log.exception("Unexpected error")
            return await update.message.reply_text(f"Помилка: {e}")

    # Стандартні пункти меню
    if text == "🧰 Сервіси":
        return await services_menu(update, context)
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
    app.add_handler(CommandHandler("topup", topup_providers))
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

    # Сервіси + поповнення + беклінки
    app.add_handler(CallbackQueryHandler(
        on_choice,
        pattern=r"^(svc\|.*|services_back|topup.*|open_amounts\|.*|topup_providers|show\|.*|csv\|.*)$"
    ))

    # Меню-тексти / ввід для сервісів
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_menu_text))

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s PUBLIC_BASE=%s", DFS_BASE, BACKEND_BASE, PUBLIC_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

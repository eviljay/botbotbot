# bot.py
import os
import io
import re
import csv
import math
import logging
import sqlite3
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

# ціни інструментів (грн → конвертуємо в кредити)
SERP_CHARGE_UAH       = _parse_float_env("SERP_CHARGE_UAH", 5.0)
KW_IDEAS_CHARGE_UAH   = _parse_float_env("KW_IDEAS_CHARGE_UAH", 5.0)
GAP_CHARGE_UAH        = _parse_float_env("GAP_CHARGE_UAH", 10.0)
BACKLINKS_OV_CHARGE_UAH = _parse_float_env("BACKLINKS_OVERVIEW_CHARGE_UAH", 5.0)
AUDIT_CHARGE_UAH      = _parse_float_env("AUDIT_CHARGE_UAH", 5.0)

# для адмінки
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH   = os.getenv("DB_PATH", "bot.db")

PREVIEW_COUNT = 10
CSV_MAX       = 1000
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

def _ensure_credits(uid: int, need_uah: float, feature_name: str) -> Tuple[bool, int]:
    need_credits = _uah_to_credits(need_uah)
    ok = charge(uid, need_credits, feature_name, "run")
    return ok, need_credits

def _parse_opts(line: str) -> Tuple[str, dict]:
    """
    Розбір формату:
    "something | country=Ukraine | lang=Ukrainian | limit=20"
    повертає ("something", {"country":"Ukraine", "lang":"Ukrainian", "limit":"20"})
    """
    parts = [p.strip() for p in line.split("|")]
    main = parts[0] if parts else ""
    opts = {}
    for p in parts[1:]:
        m = re.match(r"([a-zA-Z_]+)\s*=\s*(.+)", p)
        if m:
            opts[m.group(1).lower()] = m.group(2).strip()
    return main, opts

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
        "Після кліку бот попросить ввести дані в одному рядку з опціями через `|`.\n"
        "Приклади:\n"
        "• SERP: `iphone 13 | country=Ukraine | lang=Ukrainian`\n"
        "• Ідеї ключових: `seo tools | country=Ukraine | lang=Ukrainian | limit=20`\n"
        "• Gap: `mydomain.com | comps=site1.com,site2.com | country=Ukraine | lang=Ukrainian`\n"
        "• Backlinks огляд: `mydomain.com`\n"
        "• Аудит: `https://example.com/page`"
    )
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=_services_kb())
    else:
        await update.callback_query.edit_message_text(text, parse_mode="Markdown", reply_markup=_services_kb())

# ====== /start ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ensure_user(uid)
    bal = get_balance(uid)
    reg = _registered(uid)
    reg_text = "✅ телефон додано" if reg else "❌ немає телефону (використайте Реєстрація)"

    # Deep-link /start <param>
    raw = (update.message.text or "").strip()
    param: Optional[str] = None
    if raw.startswith("/start"):
        parts = raw.split(maxsplit=1)
        if len(parts) == 2:
            param = parts[1].strip()

    if param == TELEGRAM_START_PARAM:
        await update.message.reply_text(
            "Дякуємо! Якщо платіж пройшов, баланс оновиться протягом хвилини.\n"
            "Перевірте /balance або натисніть «📊 Баланс».",
            reply_markup=main_menu_keyboard(reg)
        )
        return

    text = (
        "Привіт! Я SEO-бот з балансом.\n\n"
        "Меню:\n"
        "🧰 Сервіси — інструменти (SERP, Keywords, Gap, Backlinks, Audit)\n"
        "💳 Поповнити — LiqPay або WayForPay\n"
        "📊 Баланс — ваші кредити\n"
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

# ====== Backlinks (старий тул через команду — залишимо) ======
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

# ====== CALLBACKS (topup & backlinks & services entry) ======
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
            "serp": "🔍 SERP: введіть запит. Опційно: `| country=Ukraine | lang=Ukrainian`",
            "keywords": "🧠 Ідеї ключових: введіть seed. Опційно: `| country=Ukraine | lang=Ukrainian | limit=20`",
            "gap": "⚔️ Gap: `mydomain.com | comps=site1.com,site2.com | country=Ukraine | lang=Ukrainian`",
            "backlinks_ov": "🔗 Backlinks огляд: введіть домен: `mydomain.com`",
            "audit": "🛠️ Аудит: введіть URL: `https://example.com/page`",
        }
        text = prompts.get(tool, "Надішліть параметри в одному рядку.")
        return await query.edit_message_text(text, parse_mode="Markdown")

    # --- Екран вибору провайдера / повернення ---
    if cmd == "topup_providers":
        return await topup_providers(update, context)

    # --- Відкрити вибір сум для провайдера ---
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

    # --- Старі платні дії (backlinks list/CSV через команду) ---
    if cmd in ("show", "csv") and len(parts) == 3:
        if not dfs:
            return await query.edit_message_text("DataForSEO не сконфігуровано. Додайте логін/пароль у .env")

        _, domain, scope = parts
        need_credits = _uah_to_credits(5.0)

        if not charge(uid, need_credits, domain, scope):
            return await query.edit_message_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )

        try:
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
                if scope == "all" and len(items) > cap:
                    txt += f"\n\n…показано перші {cap} з {len(items)}."
                txt += f"\n\n💰 Списано {need_credits} кредит(и). Новий баланс: {bal_now}"
                await query.edit_message_text(txt)
            else:
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
                        it.get("domain_from"),
                    ])
                csv_bytes = buf.getvalue().encode()
                bal_now = get_balance(uid)
                await query.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename=f"{domain}_backlinks_{scope}.csv"),
                    caption=f"Експорт для {domain} ({'10' if scope=='10' else 'all'})\n💰 Списано {need_credits}. Новий баланс: {bal_now}"
                )
                await query.edit_message_text("Готово ✅")
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

# ====== Обробка меню (reply-клавіатура) ======
async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    # Якщо очікуємо ввід параметрів для сервісу — обробляємо тут
    aw = context.user_data.get("await_tool")
    if aw:
        # гасимо чекання відразу, щоб не зациклитись при помилці
        context.user_data.pop("await_tool", None)
        if not dfs:
            return await update.message.reply_text("DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env")

        # Розбір опцій
        main, opts = _parse_opts(text)
        country = opts.get("country", "Ukraine")
        lang = opts.get("lang", "Ukrainian")
        limit = int(re.findall(r"\d+", opts.get("limit", "20"))[0]) if opts.get("limit") else 20
        comps_raw = opts.get("comps") or opts.get("competitors") or ""

        try:
            if aw == "serp":
                ok, spent = _ensure_credits(uid, SERP_CHARGE_UAH, "SERP")
                if not ok:
                    return await update.message.reply_text(
                        f"Недостатньо кредитів (потрібно {_uah_to_credits(SERP_CHARGE_UAH)}). Поповніть баланс.",
                        reply_markup=_topup_cta(),
                    )
                resp = await dfs.serp_google_organic(main, country, lang, depth=10)
                items = _extract_first_items(resp)
                # top-10
                lines = []
                for it in items[:10]:
                    rank = it.get("rank_group") or it.get("rank_absolute") or "—"
                    title = it.get("title") or it.get("rich_snippet", {}).get("top", {}).get("title") or ""
                    url = it.get("url") or it.get("domain")
                    lines.append(f"{rank}. {title}\n{url}")
                out = "🔍 *SERP — Топ-10*\n" + ("\n\n".join(lines) if lines else "Нічого не знайшов.")
                bal_now = get_balance(uid)
                out += f"\n\n💰 Списано {_uah_to_credits(SERP_CHARGE_UAH)}. Новий баланс: {bal_now}"
                return await update.message.reply_text(out, parse_mode="Markdown")

            if aw == "keywords":
                ok, spent = _ensure_credits(uid, KW_IDEAS_CHARGE_UAH, "Keywords Ideas")
                if not ok:
                    return await update.message.reply_text(
                        f"Недостатньо кредитів (потрібно {_uah_to_credits(KW_IDEAS_CHARGE_UAH)}). Поповніть баланс.",
                        reply_markup=_topup_cta(),
                    )
                resp = await dfs.keywords_for_keywords(main, country, lang, limit=min(100, max(10, limit)))
                items = _extract_first_items(resp)
                # сорт за volume (якщо є)
                items.sort(key=lambda x: (x.get("search_volume") or 0), reverse=True)
                top = items[:min(20, len(items))]
                lines = []
                for it in top:
                    kw = it.get("keyword")
                    vol = it.get("search_volume")
                    cpc = (it.get("cpc") or {}).get("usd")
                    comp = it.get("competition")
                    lines.append(f"• {kw} — vol: {vol}, CPC: {cpc}, comp: {comp}")
                out = "🧠 *Ідеї ключових*\n" + ("\n".join(lines) if lines else "Нічого не знайшов.")
                # CSV
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "cpc_usd", "competition"])
                for it in items:
                    w.writerow([it.get("keyword"), it.get("search_volume"), (it.get("cpc") or {}).get("usd"), it.get("competition")])
                csv_bytes = buf.getvalue().encode()
                bal_now = get_balance(uid)
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename=f"keywords_{re.sub(r'\\W+','_',main)[:30]}.csv"),
                    caption=f"Вивантаження ({len(items)} рядків). 💰 Списано {_uah_to_credits(KW_IDEAS_CHARGE_UAH)}. Баланс: {bal_now}"
                )
                return await update.message.reply_text(out, parse_mode="Markdown")

            if aw == "gap":
                ok, spent = _ensure_credits(uid, GAP_CHARGE_UAH, "Keyword Gap")
                if not ok:
                    return await update.message.reply_text(
                        f"Недостатньо кредитів (потрібно {_uah_to_credits(GAP_CHARGE_UAH)}). Поповніть баланс.",
                        reply_markup=_topup_cta(),
                    )
                competitors = [c.strip() for c in comps_raw.split(",") if c.strip()]
                resp = await dfs.keywords_gap(main, competitors, country, lang, limit=min(200, max(20, limit)))
                items = _extract_first_items(resp)
                # Виведемо топ відсутніх (missing) для мого домену
                missing = [it for it in items if (it.get("intersection_status") or "").startswith("missing")]
                lines = []
                for it in missing[:30]:
                    kw = it.get("keyword")
                    vol = it.get("search_volume")
                    lines.append(f"• {kw} — vol: {vol}")
                out = "⚔️ *Keyword Gap — відсутні ключі*\n" + ("\n".join(lines) if lines else "Відсутніх не знайдено.")
                # CSV
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "status", "search_volume"])
                for it in items:
                    w.writerow([it.get("keyword"), it.get("intersection_status"), it.get("search_volume")])
                csv_bytes = buf.getvalue().encode()
                bal_now = get_balance(uid)
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename=f"gap_{re.sub(r'\\W+','_',main)[:30]}.csv"),
                    caption=f"Повний список ({len(items)}). 💰 Списано {_uah_to_credits(GAP_CHARGE_UAH)}. Баланс: {bal_now}"
                )
                return await update.message.reply_text(out, parse_mode="Markdown")

            if aw == "backlinks_ov":
                ok, spent = _ensure_credits(uid, BACKLINKS_OV_CHARGE_UAH, "Backlinks Overview")
                if not ok:
                    return await update.message.reply_text(
                        f"Недостатньо кредитів (потрібно {_uah_to_credits(BACKLINKS_OV_CHARGE_UAH)}). Поповніть баланс.",
                        reply_markup=_topup_cta(),
                    )
                domain = main
                # summary
                sum_resp = await dfs.backlinks_summary(domain)
                sum_items = _extract_first_items(sum_resp)
                summary = sum_items[0] if sum_items else {}
                total_backlinks = summary.get("backlinks") or summary.get("total_backlinks")
                ref_domains = summary.get("referring_domains") or summary.get("ref_domains")
                dofollow = summary.get("dofollow")
                nofollow = summary.get("nofollow")
                # топ реф.домени
                rd_resp = await dfs.refdomains_live(domain, limit=10)
                top_rd = _extract_first_items(rd_resp)
                # топ анкори
                an_resp = await dfs.anchors_live(domain, limit=10)
                top_anchors = _extract_first_items(an_resp)

                lines = [f"🔗 *Backlinks — огляд для* `{domain}`"]
                lines.append(f"• Backlinks: {total_backlinks}")
                lines.append(f"• Referring domains: {ref_domains}")
                if dofollow is not None or nofollow is not None:
                    lines.append(f"• dofollow/nofollow: {dofollow}/{nofollow}")

                if top_rd:
                    lines.append("\nТоп реф.доменів:")
                    for it in top_rd[:10]:
                        d = it.get("domain") or it.get("referring_domain")
                        bl = it.get("backlinks")
                        lines.append(f"  • {d} — {bl} посилань")

                if top_anchors:
                    lines.append("\nТоп анкорів:")
                    for it in top_anchors[:10]:
                        a = (it.get("anchor") or "").strip()
                        bl = it.get("backlinks")
                        lines.append(f"  • {a[:60]} — {bl}")

                bal_now = get_balance(uid)
                lines.append(f"\n💰 Списано {_uah_to_credits(BACKLINKS_OV_CHARGE_UAH)}. Новий баланс: {bal_now}")
                return await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

            if aw == "audit":
                ok, spent = _ensure_credits(uid, AUDIT_CHARGE_UAH, "On-Page Audit")
                if not ok:
                    return await update.message.reply_text(
                        f"Недостатньо кредитів (потрібно {_uah_to_credits(AUDIT_CHARGE_UAH)}). Поповніть баланс.",
                        reply_markup=_topup_cta(),
                    )
                url = main
                resp = await dfs.onpage_instant(url)
                items = _extract_first_items(resp)
                it = items[0] if items else {}
                status = it.get("status_code")
                meta = it.get("meta") or {}
                title = meta.get("title")
                descr = meta.get("description")
                h1 = (it.get("page_headers") or {}).get("h1") or it.get("h1")
                if isinstance(h1, list):
                    h1 = "; ".join([str(x) for x in h1][:3])
                h2 = (it.get("page_headers") or {}).get("h2") or it.get("h2")
                if isinstance(h2, list):
                    h2 = "; ".join([str(x) for x in h2][:5])
                canonical = it.get("canonical")
                issues = it.get("onpage_score")  # placeholder, різні поля можливі

                lines = [f"🛠️ *Аудит URL*\n{url}"]
                lines.append(f"• Статус: {status}")
                lines.append(f"• Title: {title}")
                lines.append(f"• Description: {descr}")
                lines.append(f"• H1: {h1}")
                lines.append(f"• H2: {h2}")
                lines.append(f"• Canonical: {canonical}")
                if issues is not None:
                    lines.append(f"• On-page score: {issues}")

                bal_now = get_balance(uid)
                lines.append(f"\n💰 Списано {_uah_to_credits(AUDIT_CHARGE_UAH)}. Новий баланс: {bal_now}")
                return await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

            # якщо щось інше
            return await update.message.reply_text("Прийшло, але інструмент ще не підв’язаний.")

        except HTTPError as e:
            log.exception("DataForSEO HTTP error")
            return await update.message.reply_text(f"DataForSEO HTTP error: {e}")
        except Exception as e:
            log.exception("Unexpected error")
            return await update.message.reply_text(f"Помилка: {e}")

    # --- звичайне меню ---
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

    # Сервіси (інлайн)
    app.add_handler(CallbackQueryHandler(on_choice, pattern=r"^(svc\|.*|services_back|topup.*|show\|.*|csv\|.*|open_amounts\|.*|topup_providers)$"))

    # Меню-тексти / ввід для сервісів
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_menu_text))

    log.info("Bot started. DFS_BASE=%s BACKEND_BASE=%s PUBLIC_BASE=%s", DFS_BASE, BACKEND_BASE, PUBLIC_BASE)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()

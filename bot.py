import os
import io
import re
import csv
import math
import logging
import sqlite3
import zipfile
import asyncio
from typing import List, Optional, Tuple
from urllib.parse import urlparse
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv
from httpx import AsyncClient, ConnectError, HTTPError
from html import escape


from telegram.error import TelegramError
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
BACK_TEXT = "⬅️ Назад"
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
TELEGRAM_BOT_URL = os.getenv("TELEGRAM_BOT_URL", "")
TELEGRAM_START_PARAM = os.getenv("TELEGRAM_START_PARAM", "paid")

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DFS_LOGIN = os.environ.get("DATAFORSEO_LOGIN", "")
DFS_PASS = os.environ.get("DATAFORSEO_PASSWORD", "")
DFS_BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")

BACKEND_BASE = os.getenv("BACKEND_BASE", "http://127.0.0.1:8001").rstrip("/")
PUBLIC_BASE = os.getenv("PUBLIC_BASE", "https://server1.seoswiss.online").rstrip("/")

# 1 кредит = скільки грн
CREDIT_PRICE_UAH = _parse_float_env("CREDIT_PRICE_UAH", 5.0)

INITIAL_BONUS = _parse_int_env("INITIAL_BONUS", 10)
TOPUP_OPTIONS = _parse_int_list_env("TOPUP_OPTIONS", "100,250,500")

# === Ціни інструментів (в грн за 1 реквест, уже з націнкою) ===
# Можна налаштовувати через .env:
# SERP_CHARGE_UAH=0.04
# KW_IDEAS_CHARGE_UAH=1.2
# SITE_KW_CHARGE_UAH=1.2
# GAP_CHARGE_UAH=0.3
# BACKLINKS_CHARGE_UAH=0.18
# BACKLINKS_FULL_EXPORT_CHARGE_UAH=0.18
# AUDIT_CHARGE_UAH=0.6
#
# Якщо в .env є ще й PRICE_* (наприклад, PRICE_SERP_ORGANIC),
# то вручну просто виставляй їх у *_CHARGE_UAH — бот бере саме їх.

SERP_CHARGE_UAH = _parse_float_env("SERP_CHARGE_UAH", 0.04)                 # Google Organic SERP
KW_IDEAS_CHARGE_UAH = _parse_float_env("KW_IDEAS_CHARGE_UAH", 1.20)         # keywords_for_keywords
SITE_KW_CHARGE_UAH = _parse_float_env("SITE_KW_CHARGE_UAH", 1.20)           # keywords_for_site
GAP_CHARGE_UAH = _parse_float_env("GAP_CHARGE_UAH", 0.30)                   # domain_intersection (keyword gap)
BACKLINKS_CHARGE_UAH = _parse_float_env("BACKLINKS_CHARGE_UAH", 0.18)       # backlinks/backlinks live (огляд)
BACKLINKS_FULL_EXPORT_CHARGE_UAH = _parse_float_env(
    "BACKLINKS_FULL_EXPORT_CHARGE_UAH",
    0.18
)
AUDIT_CHARGE_UAH = _parse_float_env("AUDIT_CHARGE_UAH", 0.60)               # on_page/instant_pages
SITE_OVERVIEW_CHARGE_UAH = _parse_float_env("SITE_OVERVIEW_CHARGE_UAH", 0.80)  # огляд сайту (relevant_pages + ranked_keywords)

# налаштування експорту
CSV_MAX = _parse_int_env("CSV_MAX", 1000)
BACKLINKS_PAGE_SIZE = _parse_int_env("BACKLINKS_PAGE_SIZE", 1000)
MAX_BACKLINKS_EXPORT = _parse_int_env("MAX_BACKLINKS_EXPORT", 200000)
BACKLINKS_PART_ROWS = _parse_int_env("BACKLINKS_CSV_PART_ROWS", 50000)

# для адмінки
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
DB_PATH = os.getenv("DB_PATH", "bot.db")

PREVIEW_COUNT = 10
PAGE_SIZE = 20
WAIT_PHONE = 10

# ====== INIT ======
init_db()
dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE) if DFS_LOGIN and DFS_PASS else None


# ====== Списки країн / мов ======
SERP_LOCATIONS = [
    "Ukraine",
    "Poland",
    "Germany",
    "Sweden",
    "Norway",
    "Finland",
    "Denmark",
    "Netherlands",
    "Czech Republic",
    "Spain",
    "France",
    "Italy",
    "United States",
    "United Kingdom",
    "Canada",
    "Australia",
    "New Zealand",
]

SERP_LANGUAGES = [
    "Ukrainian",
    "Polish",
    "German",
    "Swedish",
    "Norwegian",
    "Danish",
    "Dutch",
    "Czech",
    "Spanish",
    "French",
    "Italian",
    "English",
]

LOCATION_CODES = {
    "Ukraine": 2804,
    "Poland": 2616,
    "Germany": 2276,
    "Sweden": 2752,
    "Norway": 2578,
    "Finland": 2246,
    "Denmark": 2208,
    "Netherlands": 2528,
    "Czech Republic": 2203,
    "Spain": 2724,
    "France": 2250,
    "Italy": 2380,
    "United States": 2840,
    "United Kingdom": 2826,
    "Canada": 2124,
    "Australia": 2036,
    "New Zealand": 2554,
}

LANGUAGE_CODES = {
    "Ukrainian": "uk",
    "Polish": "pl",
    "German": "de",
    "Swedish": "sv",
    "Norwegian": "no",
    "Danish": "da",
    "Dutch": "nl",
    "Czech": "cs",
    "Spanish": "es",
    "French": "fr",
    "Italian": "it",
    "English": "en",
}

def build_keyword_gap_message(
    gap_response: Dict[str, Any],
    target: str,
    limit: int = 10,
) -> str:
    """
    Формує текст для Keyword Gap з спрощеної структури:
    {
        "items": [
            {
                "competitor": "...",
                "keyword": "...",
                "search_volume": ...,
                "position": ...
            },
            ...
        ]
    }
    """
    items = gap_response.get("items") or []
    if not items:
        return "Нічого не знайшов 😕"

    # топ-ключі за пошуковим об’ємом
    items_sorted = sorted(
        items,
        key=lambda x: x.get("search_volume") or 0,
        reverse=True,
    )[:limit]

    lines: List[str] = [
        f"⚔️ Keyword Gap\n(ключі є в конкурентів, але не в {target}):"
    ]

    for it in items_sorted:
        kw = it.get("keyword", "")
        vol = it.get("search_volume") or 0
        pos = it.get("position") or "—"
        comp = it.get("competitor") or "конкурент"

        lines.append(
            f"• {kw} — vol: {vol}, {comp}: {pos}, {target}: —"
        )

    return "\n".join(lines)


    def safe_keyword(item: Dict[str, Any]) -> Optional[str]:
        kd = item.get("keyword_data") or {}
        kw = kd.get("keyword")
        if isinstance(kw, str) and kw:
            return kw
        # fallback: інколи ключ лежить прямо в item['keyword']
        kw = item.get("keyword")
        if isinstance(kw, str) and kw:
            return kw
        # крайній варіант — пошук по вкладених dict
        for v in kd.values():
            if isinstance(v, dict) and isinstance(v.get("keyword"), str):
                return v["keyword"]
        return None

    def safe_volume(item: Dict[str, Any]) -> Optional[int]:
        kd = item.get("keyword_data") or {}
        ki = kd.get("keyword_info") or {}
        vol = ki.get("search_volume")
        if isinstance(vol, (int, float)):
            return int(vol)
        # клікстрім-варіант, якщо раптом такий
        cki = kd.get("clickstream_keyword_info") or {}
        vol = cki.get("search_volume")
        if isinstance(vol, (int, float)):
            return int(vol)
        # запасний варіант
        vol = item.get("search_volume")
        if isinstance(vol, (int, float)):
            return int(vol)
        return None

    def safe_position_for_target(item: Dict[str, Any]) -> Optional[int]:
        # 1) пробуємо наш домен у first_domain_serp_element / target1_serp_element
        fd = item.get("first_domain_serp_element") or item.get("target1_serp_element") or {}
        if isinstance(fd, dict):
            pos = fd.get("rank_group")
            if isinstance(pos, int):
                return pos
        # 2) fallback — second_domain_serp_element / target2_serp_element
        sd = item.get("second_domain_serp_element") or item.get("target2_serp_element") or {}
        if isinstance(sd, dict):
            pos = sd.get("rank_group")
            if isinstance(pos, int):
                return pos
        return None

    def competitor_domains(item: Dict[str, Any]) -> str:
        domains: List[str] = []
        for key in (
            "second_domain_serp_element",
            "third_domain_serp_element",
            "fourth_domain_serp_element",
            "target2_serp_element",
            "target3_serp_element",
            "target4_serp_element",
        ):
            el = item.get(key)
            if isinstance(el, dict):
                d = el.get("domain")
                if isinstance(d, str):
                    domains.append(d)
        domains = sorted(set(domains))
        return ", ".join(domains) if domains else "конкуренти"

    lines: List[str] = ["⚔️ Keyword Gap"]
    for idx, item in enumerate(items[:limit], start=1):
        kw = safe_keyword(item)
        vol = safe_volume(item)
        pos = safe_position_for_target(item)
        comps = competitor_domains(item)

        # Якщо взагалі нічого корисного немає – пропускаємо
        if not kw and vol is None and pos is None:
            continue

        vol_str = str(vol) if vol is not None else "—"
        pos_str = str(pos) if pos is not None else "—"
        kw_str = kw or "—"

        lines.append(
            f"• {kw_str} — vol: {vol_str}, місце: {pos_str}, vs {target}: ({comps})"
        )

    if len(lines) == 1:
        lines.append("Немає ключових слів для відображення.")

    return "\n".join(lines)


def countries_keyboard() -> ReplyKeyboardMarkup:
    rows = []
    row: List[KeyboardButton] = []
    for i, name in enumerate(SERP_LOCATIONS, start=1):
        row.append(KeyboardButton(name))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def languages_keyboard() -> ReplyKeyboardMarkup:
    rows = []
    row: List[KeyboardButton] = []
    for i, name in enumerate(SERP_LANGUAGES, start=1):
        row.append(KeyboardButton(name))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


# ====== Утиліти меню ======
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
    rows = [
        [KeyboardButton("🔍 SERP"), KeyboardButton("🧠 Keyword Ideas")],
        [KeyboardButton("🌐 Ключі для сайту"), KeyboardButton("⚔️ Gap")],
        [KeyboardButton("🔗 Backlinks"), KeyboardButton("🛠️ Аудит"), KeyboardButton("📈 Огляд сайту")],
        [KeyboardButton("⬅️ Назад")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def _set_menu_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE, kb: ReplyKeyboardMarkup):
    """
    Оновлення нижнього меню через «невидиме» повідомлення.
    """
    chat_id = update.effective_chat.id
    old_id = context.chat_data.get("menu_holder_id")

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text="\u2063",
        reply_markup=kb,
        disable_notification=True,
        allow_sending_without_reply=True,
    )
    context.chat_data["menu_holder_id"] = msg.message_id

    if old_id and old_id != msg.message_id:
        try:
            await asyncio.sleep(0.15)
            await context.bot.delete_message(chat_id=chat_id, message_id=old_id)
        except TelegramError:
            pass


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
        #[InlineKeyboardButton("💳 LiqPay", callback_data="open_amounts|liqpay")],
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


def find_keyword_items(node):
    """
    Рекурсивно шукаємо список dict'ів з ключем 'keyword' в будь-якій вкладеності.
    """
    if isinstance(node, list):
        if node and all(isinstance(x, dict) and "keyword" in x for x in node):
            return node
        for x in node:
            found = find_keyword_items(x)
            if found:
                return found
    elif isinstance(node, dict):
        for v in node.values():
            found = find_keyword_items(v)
            if found:
                return found
    return []


def filter_keywords(items, min_search_volume: int = 1):
    """
    Фільтруємо keywords з пошуковим обсягом >= min_search_volume.
    """
    out = []
    for it in items:
        vol = (
            it.get("search_volume")
            or it.get("avg_monthly_searches")
            or it.get("search_volume_avg")
        )
        try:
            vol_val = int(vol)
        except Exception:
            vol_val = 0
        if vol_val >= min_search_volume:
            out.append(it)
    return out


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
        #[InlineKeyboardButton("💳 LiqPay", callback_data="open_amounts|liqpay")],
        [InlineKeyboardButton("🏦 WayForPay", callback_data="open_amounts|wayforpay")],
    ])


# ====== Сервіси (інлайн-меню) ======
def _services_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Топ-10 Google (SERP)", callback_data="svc|serp")],
        [InlineKeyboardButton("🧠 Ідеї ключових + обсяг/CPC", callback_data="svc|keywords")],
        [InlineKeyboardButton("⚔️ Keyword Gap", callback_data="svc|gap")],
        [InlineKeyboardButton("📈 Огляд сайту", callback_data="svc|site_overview")],
        [
            InlineKeyboardButton("🔗 Backlinks огляд", callback_data="svc|backlinks_ov"),
            InlineKeyboardButton("🛠️ Аудит URL (On-Page)", callback_data="svc|audit"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="services_back")],
    ])



async def services_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🧰 *Сервіси*\n\nОбери інструмент. "
        "Можна або діалоговий режим через кнопки (SERP/Ideas/GAP/Ключі для сайту), "
        "або разовий запуск, надіславши параметри в одному рядку з опціями через `|`.\n\n"
        "Приклади one-line:\n"
        "• SERP: `iphone 13 | country=Ukraine | lang=Ukrainian | depth=10`\n"
        "• Ідеї ключових: `seo tools | country=Ukraine | lang=Ukrainian | limit=20`\n"
        "• Gap: `mydomain.com | comps=site1.com,site2.com | country=Ukraine | lang=Ukrainian | limit=50`\n"
        "• Backlinks огляд: `mydomain.com`\n"
        "• Аудит: `https://example.com/page`"
    )
    if update.message:
        await update.message.reply_text(
            text,
            reply_markup=_services_kb(),
            disable_web_page_preview=True,
            parse_mode="Markdown",
        )
    else:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=_services_kb(),
            disable_web_page_preview=True,
            parse_mode="Markdown",
        )


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
        "Привіт! Я SEO-бот який вміє багато корисних речей які стануть у нагоді SEO спеціалісту.\n\n Всі побажання і пропозиціі висилайте на пошту info@seoswiss.online \n\n"
        "Меню:\n"
        "🧰 Сервіси — SERP, Keywords, Gap, Backlinks, Audit, Ключі для сайту\n"
        "💳 Поповнити —  WayForPay\n"
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


# ====== Backlinks (/backlinks команда) ======
async def backlinks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args:
        return await update.message.reply_text("Приклад: `/backlinks yourdomain.com`", parse_mode="Markdown")
    domain = args[0].strip()

    kb = [
        [
            InlineKeyboardButton("👀 Показати 10", callback_data=f"show|{domain}|10"),
            InlineKeyboardButton("⬇️ CSV 10", callback_data=f"csv|{domain}|10"),
        ],
        [
            InlineKeyboardButton("👀 Показати всі", callback_data=f"show|{domain}|all"),
            InlineKeyboardButton("⬇️ CSV всі", callback_data=f"csv|{domain}|all"),
        ],
    ]
    await update.message.reply_text(
        f"Домен: *{domain}*\nОберіть дію (з кожної дії буде списано {BACKLINKS_CHARGE_UAH}₴ / перераховано в кредити):",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


# ====== CALLBACKS (services, topup, backlinks) ======
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

    # --- Сервіси (інлайн) ---
    if cmd == "svc":
        tool = parts[1] if len(parts) > 1 else ""
        if tool in ("backlinks_ov", "audit", "site_overview"):
            context.user_data["await_tool"] = tool
            prompts = {
                "backlinks_ov": "🔗 Backlinks огляд: введіть домен: `mydomain.com`",
                "audit": "🛠️ Аудит: введіть URL: `https://example.com/page`",
                "site_overview": "📈 Огляд сайту: `domain.net | country=United States | lang=English | pages=5 | limit=10`",
            }
            text = prompts.get(tool, "Надішліть параметри в одному рядку.")
            return await query.edit_message_text(
                text,
                disable_web_page_preview=True,
                parse_mode="Markdown",
            )
        else:
            txt = (
                "Для цього інструменту краще скористайтесь нижніми кнопками меню:\n"
                "🔍 SERP / 🧠 Keyword Ideas / ⚔️ Gap / 🌐 Ключі для сайту.\n\n"
                "Або надішліть параметри в одному рядку через `|`."
            )
            return await query.edit_message_text(txt, parse_mode="Markdown")

    # --- Екран вибору провайдера / назад ---
    if cmd == "topup_providers":
        return await topup_providers(update, context)

    # --- Вибір сум ---
    if cmd == "open_amounts":
        provider = (parts[1] if len(parts) > 1 else "liqpay").lower()
        return await open_amounts(update, context, provider)

    # --- Створення інвойсу ---
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

    # --- Кнопки старого /backlinks ---
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
                data_resp = await dfs.backlinks_live(domain, limit=limit, offset=0)
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


# ============ ФЛОУ: SERP / KW IDEAS / SITE KW / GAP ============

async def _start_serp_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["serp_state"] = "keyword"
    context.user_data["serp_params"] = {}
    context.user_data.pop("await_tool", None)

    # Кнопка назад у Reply Keyboard
    kb = [[KeyboardButton("⬅️ Назад")]]

    await update.message.reply_text(
        "🔍 SERP трекінг\n\nВведи keyword, який хочеш перевірити в Google:",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True)
    )


async def _handle_serp_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    uid = update.effective_user.id
    state = context.user_data.get("serp_state")
    params = context.user_data.get("serp_params") or {}

    if text == "⬅️ Назад":
        context.user_data.pop("serp_state", None)
        context.user_data.pop("serp_params", None)
        await update.message.reply_text(
            "Повертаю в меню сервісів.",
            reply_markup=services_menu_keyboard(),
        )
        return

    # keyword
    if state == "keyword":
        kw = text.strip()
        if not kw:
            await update.message.reply_text("Будь ласка, введи непорожній keyword:")
            return
        params["keyword"] = kw
        context.user_data["serp_params"] = params
        context.user_data["serp_state"] = "country"
        await update.message.reply_text(
            "Оберіть країну для пошуку:",
            reply_markup=countries_keyboard(),
        )
        return

    # country
    if state == "country":
        if text not in SERP_LOCATIONS:
            await update.message.reply_text(
                "Оберіть країну з кнопок нижче:",
                reply_markup=countries_keyboard(),
            )
            return
        params["country"] = text
        context.user_data["serp_params"] = params
        context.user_data["serp_state"] = "language"
        await update.message.reply_text(
            "Тепер оберіть мову пошуку:",
            reply_markup=languages_keyboard(),
        )
        return

    # language
    if state == "language":
        if text not in SERP_LANGUAGES:
            await update.message.reply_text(
                "Оберіть мову з кнопок нижче:",
                reply_markup=languages_keyboard(),
            )
            return
        params["language"] = text
        context.user_data["serp_params"] = params
        context.user_data["serp_state"] = "depth"
        await update.message.reply_text(
            "Глибина SERP: обери 10, 20 або 30 або 100.",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("10"), KeyboardButton("20"), KeyboardButton("30"), KeyboardButton("100")]],
                resize_keyboard=True,
            ),
        )
        return

    # depth + запуск
    if state == "depth":
        try:
            depth = int(text)
        except ValueError:
            await update.message.reply_text("Напиши 10, 20 або 30 або 100 як глибину:")
            return
        if depth not in (10, 20, 30, 100):
            await update.message.reply_text("Підтримуються значення 10, 20 або 30 або 100.")
            return

        keyword = (params.get("keyword") or "").strip()
        country_name = params.get("country") or "Ukraine"
        language_name = params.get("language") or "Ukrainian"
        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")

        context.user_data.pop("serp_state", None)
        context.user_data.pop("serp_params", None)

        if not dfs:
            await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )
            return

        need_credits = _uah_to_credits(SERP_CHARGE_UAH)
        if not charge(uid, need_credits, "svc:serp", keyword or "-"):
            await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )
            return

        try:
            resp = await dfs.serp_google_organic(
                keyword,
                location_code=location_code,
                language_code=language_code,
                depth=depth,
            )
        except Exception as e:
            log.exception("SERP request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        items = _extract_first_items(resp)
        if not items:
            bal_now = get_balance(uid)
            await update.message.reply_text(
                f"Нічого не знайшов 😕\nБаланс: {bal_now}",
                reply_markup=services_menu_keyboard(),
            )
            return

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
        await update.message.reply_text(
            preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
            parse_mode="Markdown",
            reply_markup=services_menu_keyboard(),
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="serp_top.csv"),
            caption="CSV із результатами SERP",
        )
        return


async def start_kwideas_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["kwideas_state"] = "keyword"
    context.user_data["kwideas"] = {}
    context.user_data.pop("await_tool", None)
    # Кнопка назад у Reply Keyboard
    kb = [[KeyboardButton("⬅️ Назад")]]
    await update.message.reply_text(
        "🧠 Keyword Ideas\n\nВведи seed keyword:",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True),
    )


async def handle_keyword_gap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    # 0) Беремо вибрані країну і мову з тих самих параметрів, що й для SERP
    params = context.user_data.get("serp_params", {})

    country_name = params.get("country", "Ukraine")      # дефолт як у SERP
    language_name = params.get("language", "Ukrainian")  # дефолт як у SERP

    location_code = LOCATION_CODES.get(country_name, 2840)   # 2840 — Ukraine
    language_code = LANGUAGE_CODES.get(language_name, "uk")  # "uk" — українська

    # 1) Парсимо аргументи з команди
    # /gap fotoklok.se onskefoto.se smartphoto.se cewe.se
    args = context.args if hasattr(context, "args") else []

    if not args:
        await update.message.reply_text(
            "⚔️ Keyword Gap\n\n"
            "Будь ласка, введи домени так:\n"
            "`/gap ваш-сайт.se конкурент1.se конкурент2.se конкурент3.se`\n"
            "або:\n"
            "`/gap ваш-сайт.se конкурент1.se, конкурент2.se, конкурент3.se`",
            parse_mode="Markdown",
        )
        return

    # перший аргумент — наш сайт (target)
    target = args[0].strip().lower()

    # все, що після першого аргументу — конкуренти (можуть бути через пробіли або коми)
    raw_competitors = " ".join(args[1:]).strip()

    competitors: list[str] = []
    if raw_competitors:
        # Міняємо ; на , про всяк випадок
        raw_competitors = raw_competitors.replace(";", ",")
        for chunk in raw_competitors.split(","):
            dom = chunk.strip().lower()
            if dom and dom != target:
                competitors.append(dom)

    # прибираємо дублікати, але зберігаємо порядок
    competitors = list(dict.fromkeys(competitors))

    if not competitors:
        await update.message.reply_text(
            "⚔️ Keyword Gap\n\n"
            "Потрібен хоча б один конкурент.\n"
            "Приклад:\n"
            "`/gap fotoklok.se onskefoto.se smartphoto.se cewe.se`",
            parse_mode="Markdown",
        )
        return

    # 2) тягнемо gap з dataforseo — вже з обраними location_code та language_code
    gap = await dfs.keywords_gap(
        target=target,
        competitors=competitors,
        location_code=location_code,
        language_code=language_code,
        limit=50,
    )

    # 3) будуємо текст через нашу функцію
    text = build_keyword_gap_message(gap, target=target, limit=10)

    # 4) додаємо інформацію про баланс
    balance = await get_balance(chat_id)  # твоя існуюча функція
    text += f"\n\n💰 Списано 1. Баланс: {balance}"

    # 5) шлемо в Telegram
    await update.message.reply_text(text)






async def handle_kwideas_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    state = context.user_data.get("kwideas_state")
    data = context.user_data.get("kwideas") or {}
    uid = update.effective_user.id

    if text == "⬅️ Назад":
        context.user_data.pop("kwideas_state", None)
        context.user_data.pop("kwideas", None)
        await update.message.reply_text(
            "Повертаю в меню сервісів.",
            reply_markup=services_menu_keyboard(),
        )
        return

    # keyword
    if state == "keyword":
        kw = text.strip()
        if not kw:
            await update.message.reply_text("Порожній keyword. Введи ще раз:")
            return
        data["keyword"] = kw
        context.user_data["kwideas"] = data
        context.user_data["kwideas_state"] = "country"
        await update.message.reply_text(
            "Оберіть країну:",
            reply_markup=countries_keyboard(),
        )
        return

    # country
    if state == "country":
        if text not in SERP_LOCATIONS:
            await update.message.reply_text(
                "Оберіть країну з кнопок нижче:",
                reply_markup=countries_keyboard(),
            )
            return
        data["country"] = text
        context.user_data["kwideas"] = data
        context.user_data["kwideas_state"] = "language"
        await update.message.reply_text(
            "Оберіть мову:",
            reply_markup=languages_keyboard(),
        )
        return

    # language
    if state == "language":
        if text not in SERP_LANGUAGES:
            await update.message.reply_text(
                "Оберіть мову з кнопок нижче:",
                reply_markup=languages_keyboard(),
            )
            return
        data["language"] = text
        context.user_data["kwideas"] = data
        context.user_data["kwideas_state"] = "limit"
        await update.message.reply_text(
            "Кількість ідей: обери 10, 20 або 50.",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("10"), KeyboardButton("20"), KeyboardButton("50")]],
                resize_keyboard=True,
            ),
        )
        return

    # limit + запуск
    if state == "limit":
        try:
            limit = int(text)
        except ValueError:
            await update.message.reply_text("Напиши 10, 20 або 50:")
            return
        if limit not in (10, 20, 50):
            await update.message.reply_text("Підтримуються значення 10, 20 або 50.")
            return

        kw = data.get("keyword") or ""
        country_name = data.get("country") or "Ukraine"
        language_name = data.get("language") or "Ukrainian"
        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")

        context.user_data.pop("kwideas_state", None)
        context.user_data.pop("kwideas", None)

        if not dfs:
            await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )
            return

        need_credits = _uah_to_credits(KW_IDEAS_CHARGE_UAH)
        if not charge(uid, need_credits, "svc:keywords", kw or "-"):
            await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )
            return

        await update.message.reply_text(
            f"Шукаю keyword ideas для *{kw}* ({country_name}, {language_name}, {limit})…",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )

        try:
            resp = await dfs.keywords_for_keywords(
                kw,
                location_code=location_code,
                language_code=language_code,
            )
        except Exception as e:
            log.exception("KW ideas request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        items_all = find_keyword_items(resp)
        items = filter_keywords(items_all, min_search_volume=1)

        if not items:
            bal_now = get_balance(uid)
            await update.message.reply_text(
                f"Нічого не знайшов 😕 (або всі з 0 пошуку)\nБаланс: {bal_now}",
                reply_markup=services_menu_keyboard(),
            )
            return

        items_limited = items[:limit]

        lines = []
        for it in items_limited[:10]:
            kw_i = it.get("keyword") or it.get("keyword_text") or "—"
            vol = (
                it.get("search_volume")
                or it.get("avg_monthly_searches")
                or it.get("search_volume_avg")
                or "-"
            )
            cpc = it.get("cpc") or it.get("cost_per_click") or "-"
            lines.append(f"• {kw_i} — vol: {vol}, CPC: {cpc}")
        preview = "🧠 *Ідеї ключових*\n" + "\n".join(lines)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["keyword", "search_volume", "cpc"])
        for it in items_limited:
            w.writerow([
                it.get("keyword") or it.get("keyword_text") or "",
                it.get("search_volume") or it.get("avg_monthly_searches") or it.get("search_volume_avg") or "",
                it.get("cpc") or it.get("cost_per_click") or "",
            ])
        csv_bytes = buf.getvalue().encode()

        bal_now = get_balance(uid)
        await update.message.reply_text(
            preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
            parse_mode="Markdown",
            reply_markup=services_menu_keyboard(),
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="keyword_ideas.csv"),
            caption="CSV з ідеями ключових",
        )
        return


async def start_site_kw_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["sitekw_state"] = "target"
    context.user_data["sitekw"] = {}
    context.user_data.pop("await_tool", None)
    kb = [[KeyboardButton("⬅️ Назад")]]
    await update.message.reply_text(
        "🌐 Ключі для сайту\n\nЦей тул виконує автоматичний підбрір ключів для сайта. Якщо треба підібрати семантику, то  введи домен або URL сайту, напр. `google.com`:",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True),
        parse_mode="Markdown",
    )


async def handle_site_kw_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    state = context.user_data.get("sitekw_state")
    data = context.user_data.get("sitekw") or {}
    uid = update.effective_user.id

    if text == "⬅️ Назад":
        context.user_data.pop("sitekw_state", None)
        context.user_data.pop("sitekw", None)
        await update.message.reply_text(
            "Повертаю в меню сервісів.",
            reply_markup=services_menu_keyboard(),
        )
        return

    # target
    if state == "target":
        target = text.strip()
        if not target:
            await update.message.reply_text("Введи домен або URL сайту, напр. `domain.net`:", parse_mode="Markdown")
            return
        data["target"] = target
        context.user_data["sitekw"] = data
        context.user_data["sitekw_state"] = "country"
        await update.message.reply_text(
            "Оберіть країну:",
            reply_markup=countries_keyboard(),
        )
        return

    # country
    if state == "country":
        if text not in SERP_LOCATIONS:
            await update.message.reply_text(
                "Оберіть країну з кнопок нижче:",
                reply_markup=countries_keyboard(),
            )
            return
        data["country"] = text
        context.user_data["sitekw"] = data
        context.user_data["sitekw_state"] = "language"
        await update.message.reply_text(
            "Оберіть мову:",
            reply_markup=languages_keyboard(),
        )
        return

    # language
    if state == "language":
        if text not in SERP_LANGUAGES:
            await update.message.reply_text(
                "Оберіть мову з кнопок нижче:",
                reply_markup=languages_keyboard(),
            )
            return
        data["language"] = text
        context.user_data["sitekw"] = data
        context.user_data["sitekw_state"] = "limit"
        await update.message.reply_text(
            "Скільки ключів зібрати? Обери 20, 50 або 100.",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("20"), KeyboardButton("50"), KeyboardButton("100")]],
                resize_keyboard=True,
            ),
        )
        return

    # limit + запуск
    if state == "limit":
        try:
            limit = int(text)
        except ValueError:
            await update.message.reply_text("Напиши 20, 50 або 100:")
            return
        if limit not in (20, 50, 100):
            await update.message.reply_text("Підтримуються значення 20, 50 або 100.")
            return

        target = data.get("target") or ""
        country_name = data.get("country") or "Ukraine"
        language_name = data.get("language") or "Ukrainian"
        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")

        context.user_data.pop("sitekw_state", None)
        context.user_data.pop("sitekw", None)

        if not dfs:
            await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )
            return

        need_credits = _uah_to_credits(SITE_KW_CHARGE_UAH)
        if not charge(uid, need_credits, "svc:sitekw", target or "-"):
            await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )
            return

        await update.message.reply_text(
            f"Шукаю автоматичні ключі для сайту *{target}* "
            f"({country_name}, {language_name}, до {limit} ключів)…",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )

        try:
            resp = await dfs.keywords_for_site(
                target,
                location_code=location_code,
                language_code=language_code,
            )
        except Exception as e:
            log.exception("Site KW request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        items_all = find_keyword_items(resp)
        items = filter_keywords(items_all, min_search_volume=1)

        if not items:
            bal_now = get_balance(uid)
            await update.message.reply_text(
                f"Нічого не знайшов 😕 (або всі з 0 пошуку)\nБаланс: {bal_now}",
                reply_markup=services_menu_keyboard(),
            )
            return

        items_limited = items[:limit]

        lines = []
        for it in items_limited[:10]:
            kw_i = it.get("keyword") or it.get("keyword_text") or "—"
            vol = (
                it.get("search_volume")
                or it.get("avg_monthly_searches")
                or it.get("search_volume_avg")
                or "-"
            )
            cpc = it.get("cpc") or it.get("cost_per_click") or "-"
            lines.append(f"• {kw_i} — vol: {vol}, CPC: {cpc}")
        preview = "🌐 *Ключі для сайту*\n" + "\n".join(lines)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["keyword", "search_volume", "cpc"])
        for it in items_limited:
            w.writerow([
                it.get("keyword") or it.get("keyword_text") or "",
                it.get("search_volume") or it.get("avg_monthly_searches") or it.get("search_volume_avg") or "",
                it.get("cpc") or it.get("cost_per_click") or "",
            ])
        csv_bytes = buf.getvalue().encode()

        bal_now = get_balance(uid)
        await update.message.reply_text(
            preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
            parse_mode="Markdown",
            reply_markup=services_menu_keyboard(),
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="site_keywords.csv"),
            caption="CSV з автоматичними ключами для сайту",
        )
        return


async def start_gap_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["gap_state"] = "target"
    context.user_data["gap"] = {}
    context.user_data.pop("await_tool", None)
    kb = [[KeyboardButton("⬅️ Назад")]]
    await update.message.reply_text(
        "⚔️ GAP\n\nВведи свій сайт (target), напр. `mydomain.com`:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True),
    )

async def start_site_overview_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Старт wizard'а «Огляд сайту».
    """
    context.user_data["siteov_state"] = "target"
    context.user_data["siteov"] = {}
    context.user_data.pop("await_tool", None)
    kb = [[KeyboardButton("⬅️ Назад")]]
    await update.message.reply_text(
        "📈 Огляд сайту\n\nВведи домен або URL сайту, напр. `domain.net`:",
        reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True),
        parse_mode="Markdown",
    )


async def handle_site_overview_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """
    Кроки: target -> country -> language -> pages -> limit -> запуск.
    """
    state = context.user_data.get("siteov_state")
    data = context.user_data.get("siteov") or {}
    uid = update.effective_user.id

    if text == "⬅️ Назад":
        context.user_data.pop("siteov_state", None)
        context.user_data.pop("siteov", None)
        await update.message.reply_text(
            "Повертаю в меню сервісів.",
            reply_markup=services_menu_keyboard(),
        )
        return

    # 1) target
    if state == "target":
        target = text.strip()
        if not target:
            await update.message.reply_text(
                "Введи домен або URL сайту, напр. `wildfortune.net`:",
                parse_mode="Markdown",
            )
            return
        data["target"] = target
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "country"
        await update.message.reply_text(
            "Оберіть країну:",
            reply_markup=countries_keyboard(),
        )
        return

    # 2) country
    if state == "country":
        if text not in SERP_LOCATIONS:
            await update.message.reply_text(
                "Оберіть країну з кнопок нижче:",
                reply_markup=countries_keyboard(),
            )
            return
        data["country"] = text
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "language"
        await update.message.reply_text(
            "Тепер оберіть мову:",
            reply_markup=languages_keyboard(),
        )
        return

    # 3) language
    if state == "language":
        if text not in SERP_LANGUAGES:
            await update.message.reply_text(
                "Оберіть мову з кнопок нижче:",
                reply_markup=languages_keyboard(),
            )
            return
        data["language"] = text
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "pages"

        page_options = [["5", "10", "15"], ["20", "25"], ["50", "100"]]
        await update.message.reply_text(
            "Скільки топ-сторінок взяти з сайту?",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(x) for x in row] for row in page_options],
                resize_keyboard=True,
            ),
        )
        return

    # 4) pages
    if state == "pages":
        try:
            pages = int(text)
        except ValueError:
            await update.message.reply_text("Напиши число: 5, 10, 15, 20, 25, 50 або 100.")
            return

        allowed = {5, 10, 15, 20, 25, 50, 100}
        if pages not in allowed:
            await update.message.reply_text("Підтримуються значення: 5, 10, 15, 20, 25, 50, 100.")
            return

        data["pages"] = pages
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "limit"

        limit_options = [["5", "10", "15"], ["20", "25"], ["50", "100"]]
        await update.message.reply_text(
            "Скільки ключів на сторінку збирати?",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(x) for x in row] for row in limit_options],
                resize_keyboard=True,
            ),
        )
        return

    # 5) limit -> запуск
    if state == "limit":
        try:
            kw_limit = int(text)
        except ValueError:
            await update.message.reply_text("Напиши число: 5, 10, 15, 20, 25, 50 або 100.")
            return

        allowed = {5, 10, 15, 20, 25, 50, 100}
        if kw_limit not in allowed:
            await update.message.reply_text("Підтримуються значення: 5, 10, 15, 20, 25, 50, 100.")
            return

        data["limit"] = kw_limit

        target = data.get("target") or ""
        country_name = data.get("country") or "Ukraine"
        language_name = data.get("language") or "Ukrainian"
        pages_limit = data.get("pages") or 5

        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")

        # очищаємо state
        context.user_data.pop("siteov_state", None)
        context.user_data.pop("siteov", None)

        if not dfs:
            await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )
            return

        need_credits = _uah_to_credits(SITE_OVERVIEW_CHARGE_UAH)
        if not charge(uid, need_credits, "svc:site_overview", target or "-"):
            await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )
            return

        await update.message.reply_text(
            f"Готую огляд сайту {target} ({country_name}, {language_name})…",
            reply_markup=ReplyKeyboardRemove(),
        )

        try:
            site_resp = await dfs.relevant_pages(
                target,
                location_code=location_code,
                language_code=language_code,
                limit=pages_limit,
            )
            site_res = _extract_result(site_resp)
            pages = site_res.get("items") or []

            if not pages:
                bal_now = get_balance(uid)
                await update.message.reply_text(
                    f"Нічого не знайшов по сайту 😕\nБаланс: {bal_now}",
                    reply_markup=services_menu_keyboard(),
                )
                return

            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow([
                "URL сторінки",
                "Кількість ключів у ТОП-100",
                "Оціночний органічний трафік",
                "Вартість трафіку",
                "Ключове слово",
                "Пошуковий об’єм",
                "Позиція в Google",
                "etv",
            ])

            preview_lines = [f"📈 Огляд сайту {target} ({country_name}, {language_name})\n"]
            page_idx = 1

            for p in pages[:pages_limit]:
                page_url = p.get("page_address") or ""
                metrics = (p.get("metrics") or {}).get("organic") or {}
                kw_count = metrics.get("count") or 0
                etv_val = metrics.get("etv") or 0
                paid_cost = metrics.get("estimated_paid_traffic_cost") or 0

                rel = urlparse(page_url).path or "/"

                try:
                    kw_resp = await dfs.ranked_keywords_for_url(
                        target,
                        location_code=location_code,
                        language_code=language_code,
                        relative_url=rel,
                        limit=kw_limit,
                    )
                    kw_res = _extract_result(kw_resp)
                    kw_items = kw_res.get("items") or []
                except Exception:
                    kw_items = []

                preview_lines.append(
                    f"{page_idx}. {page_url}\n"
                    f"   keywords: {kw_count}, ETV: {etv_val:.2f}, paid_est: {paid_cost:.2f}"
                )

                for kw_item in kw_items[:3]:
                    kd = kw_item.get("keyword_data") or {}
                    ki = kd.get("keyword_info") or {}
                    se = (kw_item.get("ranked_serp_element") or {}).get("serp_item") or {}

                    kw_str = kd.get("keyword") or "—"
                    sv = ki.get("search_volume") or 0
                    rank = se.get("rank_group") or se.get("rank_absolute") or "-"
                    kw_etv = se.get("etv") or 0
                    preview_lines.append(f"      • {kw_str} — vol:{sv}, pos:{rank}, etv:{kw_etv:.2f}")

                for kw_item in kw_items:
                    kd = kw_item.get("keyword_data") or {}
                    ki = kd.get("keyword_info") or {}
                    se = (kw_item.get("ranked_serp_element") or {}).get("serp_item") or {}

                    kw_str = kd.get("keyword") or ""
                    sv = ki.get("search_volume") or ""
                    rank = se.get("rank_group") or se.get("rank_absolute") or ""
                    kw_etv = se.get("etv") or ""
                    w.writerow([
                        page_url,
                        kw_count,
                        etv_val,
                        paid_cost,
                        kw_str,
                        sv,
                        rank,
                        kw_etv,
                    ])

                preview_lines.append("")
                page_idx += 1

            csv_bytes = buf.getvalue().encode()
            bal_now = get_balance(uid)

            preview_text = "\n".join(preview_lines) + f"\n💰 Списано {need_credits}. Баланс: {bal_now}"
            await update.message.reply_text(
                preview_text,
                reply_markup=services_menu_keyboard(),
            )
            await update.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=f"{target}_overview.csv"),
                caption="CSV: сторінки сайту + ключі, по яких вони ранжуються"
            )
        except Exception as e:
            await update.message.reply_text(f"Помилка: {e}")
        return




async def handle_site_overview_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """
    Кроки: target -> country -> language -> pages -> limit -> запуск.
    """
    state = context.user_data.get("siteov_state")
    data = context.user_data.get("siteov") or {}
    uid = update.effective_user.id

    if text == "⬅️ Назад":
        context.user_data.pop("siteov_state", None)
        context.user_data.pop("siteov", None)
        await update.message.reply_text(
            "Повертаю в меню сервісів.",
            reply_markup=services_menu_keyboard(),
        )
        return

    # 1) target
    if state == "target":
        target = text.strip()
        if not target:
            await update.message.reply_text(
                "Введи домен або URL сайту, напр. `wildfortune.net`:",
                parse_mode="Markdown",
            )
            return
        data["target"] = target
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "country"
        await update.message.reply_text(
            "Оберіть країну:",
            reply_markup=countries_keyboard(),
        )
        return

    # 2) country
    if state == "country":
        if text not in SERP_LOCATIONS:
            await update.message.reply_text(
                "Оберіть країну з кнопок нижче:",
                reply_markup=countries_keyboard(),
            )
            return
        data["country"] = text
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "language"
        await update.message.reply_text(
            "Тепер оберіть мову:",
            reply_markup=languages_keyboard(),
        )
        return

    # 3) language
    if state == "language":
        if text not in SERP_LANGUAGES:
            await update.message.reply_text(
                "Оберіть мову з кнопок нижче:",
                reply_markup=languages_keyboard(),
            )
            return
        data["language"] = text
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "pages"

        page_options = [["5", "10", "15"], ["20", "25"], ["50", "100"]]
        await update.message.reply_text(
            "Скільки топ-сторінок взяти з сайту?",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(x) for x in row] for row in page_options],
                resize_keyboard=True,
            ),
        )
        return

    # 4) pages
    if state == "pages":
        try:
            pages = int(text)
        except ValueError:
            await update.message.reply_text("Напиши число: 5, 10, 15, 20, 25, 50 або 100.")
            return

        allowed = {5, 10, 15, 20, 25, 50, 100}
        if pages not in allowed:
            await update.message.reply_text("Підтримуються значення: 5, 10, 15, 20, 25, 50, 100.")
            return

        data["pages"] = pages
        context.user_data["siteov"] = data
        context.user_data["siteov_state"] = "limit"

        limit_options = [["5", "10", "15"], ["20", "25"], ["50", "100"]]
        await update.message.reply_text(
            "Скільки ключів на сторінку збирати?",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(x) for x in row] for row in limit_options],
                resize_keyboard=True,
            ),
        )
        return

    # 5) limit -> запуск інструменту
    if state == "limit":
        try:
            kw_limit = int(text)
        except ValueError:
            await update.message.reply_text("Напиши число: 5, 10, 15, 20, 25, 50 або 100.")
            return

        allowed = {5, 10, 15, 20, 25, 50, 100}
        if kw_limit not in allowed:
            await update.message.reply_text("Підтримуються значення: 5, 10, 15, 20, 25, 50, 100.")
            return

        data["limit"] = kw_limit

        target = data.get("target") or ""
        country_name = data.get("country") or "Ukraine"
        language_name = data.get("language") or "Ukrainian"
        pages_limit = data.get("pages") or 5

        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")

        # очищаємо стейт
        context.user_data.pop("siteov_state", None)
        context.user_data.pop("siteov", None)

        if not dfs:
            await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )
            return

        need_credits = _uah_to_credits(SITE_OVERVIEW_CHARGE_UAH)
        if not charge(uid, need_credits, "svc:site_overview", target or "-"):
            await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )
            return

        await update.message.reply_text(
            f"Готую огляд сайту {target} ({country_name}, {language_name})…",
            reply_markup=ReplyKeyboardRemove(),
        )

        try:
            # використовуємо ту саму логіку, що й в one-line site_overview
            site_resp = await dfs.relevant_pages(
                target,
                location_code=location_code,
                language_code=language_code,
                limit=pages_limit,
            )
            site_res = _extract_result(site_resp)
            pages = site_res.get("items") or []

            if not pages:
                bal_now = get_balance(uid)
                await update.message.reply_text(
                    f"Нічого не знайшов по сайту 😕\nБаланс: {bal_now}",
                    reply_markup=services_menu_keyboard(),
                )
                return

            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow([
                "page_url",
                "organic_keywords_count",
                "organic_etv",
                "organic_estimated_paid_traffic_cost",
                "keyword",
                "search_volume",
                "rank",
                "etv",
            ])

            preview_lines = [f"📈 Огляд сайту {target} ({country_name}, {language_name})\n"]
            page_idx = 1

            for p in pages[:pages_limit]:
                page_url = p.get("page_address") or ""
                metrics = (p.get("metrics") or {}).get("organic") or {}
                kw_count = metrics.get("count") or 0
                etv_val = metrics.get("etv") or 0
                paid_cost = metrics.get("estimated_paid_traffic_cost") or 0

                rel = urlparse(page_url).path or "/"

                try:
                    kw_resp = await dfs.ranked_keywords_for_url(
                        target,
                        location_code=location_code,
                        language_code=language_code,
                        relative_url=rel,
                        limit=kw_limit,
                    )
                    kw_res = _extract_result(kw_resp)
                    kw_items = kw_res.get("items") or []
                except Exception:
                    kw_items = []

                preview_lines.append(
                    f"{page_idx}. {page_url}\n"
                    f"   keywords: {kw_count}, ETV: {etv_val:.2f}, paid_est: {paid_cost:.2f}"
                )

                for kw_item in kw_items[:3]:
                    kd = kw_item.get("keyword_data") or {}
                    ki = kd.get("keyword_info") or {}
                    se = (kw_item.get("ranked_serp_element") or {}).get("serp_item") or {}

                    kw_str = kd.get("keyword") or "—"
                    sv = ki.get("search_volume") or 0
                    rank = se.get("rank_group") or se.get("rank_absolute") or "-"
                    kw_etv = se.get("etv") or 0
                    preview_lines.append(f"      • {kw_str} — vol:{sv}, pos:{rank}, etv:{kw_etv:.2f}")

                for kw_item in kw_items:
                    kd = kw_item.get("keyword_data") or {}
                    ki = kd.get("keyword_info") or {}
                    se = (kw_item.get("ranked_serp_element") or {}).get("serp_item") or {}

                    kw_str = kd.get("keyword") or ""
                    sv = ki.get("search_volume") or ""
                    rank = se.get("rank_group") or se.get("rank_absolute") or ""
                    kw_etv = se.get("etv") or ""
                    w.writerow([
                        page_url,
                        kw_count,
                        etv_val,
                        paid_cost,
                        kw_str,
                        sv,
                        rank,
                        kw_etv,
                    ])

                preview_lines.append("")
                page_idx += 1

            csv_bytes = buf.getvalue().encode()
            bal_now = get_balance(uid)

            preview_text = "\n".join(preview_lines) + f"\n💰 Списано {need_credits}. Баланс: {bal_now}"
            await update.message.reply_text(
                preview_text,
                reply_markup=services_menu_keyboard(),
            )
            await update.message.reply_document(
                document=InputFile(io.BytesIO(csv_bytes), filename=f"{target}_overview.csv"),
                caption="CSV: сторінки сайту + ключі, по яких вони ранжуються"
            )
        except Exception as e:
            await update.message.reply_text(f"Помилка: {e}")
        return


async def handle_gap_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    state = context.user_data.get("gap_state")
    data = context.user_data.get("gap") or {}
    uid = update.effective_user.id

    if text == "⬅️ Назад":
        context.user_data.pop("gap_state", None)
        context.user_data.pop("gap", None)
        await update.message.reply_text(
            "Повертаю в меню сервісів.",
            reply_markup=services_menu_keyboard(),
        )
        return

    # target
    if state == "target":
        target = text.strip()
        if not target:
            await update.message.reply_text(
                "Введи свій домен, напр. `mydomain.com`:",
                parse_mode="Markdown",
            )
            return
        data["target"] = target
        context.user_data["gap"] = data
        context.user_data["gap_state"] = "competitors"
        await update.message.reply_text(
            "Тепер введи конкурентів через кому, напр.: `site1.com`",
            parse_mode="Markdown",
        )
        return

    # competitors
    if state == "competitors":
        raw = text.strip()
        if not raw:
            await update.message.reply_text(
                "Введи хоча б одного конкурента через кому, напр.: `site1.com`",
                parse_mode="Markdown",
            )
            return
        comps = [c.strip() for c in raw.split(",") if c.strip()]
        if not comps:
            await update.message.reply_text("Не зрозумів конкурентів. Спробуй ще раз:")
            return
        data["competitors"] = comps
        context.user_data["gap"] = data
        context.user_data["gap_state"] = "country"
        await update.message.reply_text(
            "Оберіть країну:",
            reply_markup=countries_keyboard(),
        )
        return

    # country
    if state == "country":
        if text not in SERP_LOCATIONS:
            await update.message.reply_text(
                "Оберіть країну з кнопок нижче:",
                reply_markup=countries_keyboard(),
            )
            return
        data["country"] = text
        context.user_data["gap"] = data
        context.user_data["gap_state"] = "language"
        await update.message.reply_text(
            "Оберіть мову:",
            reply_markup=languages_keyboard(),
        )
        return

    # language + запуск
    if state == "language":
        if text not in SERP_LANGUAGES:
            await update.message.reply_text(
                "Оберіть мову з кнопок нижче:",
                reply_markup=languages_keyboard(),
            )
            return

        data["language"] = text
        target = data.get("target")
        competitors = data.get("competitors") or []
        country_name = data.get("country") or "Ukraine"
        language_name = data.get("language") or "Ukrainian"
        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")

        context.user_data.pop("gap_state", None)
        context.user_data.pop("gap", None)

        if not dfs:
            await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )
            return

        need_credits = _uah_to_credits(GAP_CHARGE_UAH)
        if not charge(uid, need_credits, "svc:gap", target or "-"):
            await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )
            return

        await update.message.reply_text(
            f"Шукаю keyword GAP для *{target}* vs {', '.join(competitors)}…",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )

        try:
            resp = await dfs.keywords_gap(
                target=target,
                competitors=competitors,
                location_code=location_code,
                language_code=language_code,
                limit=50,
            )
        except Exception as e:
            log.exception("GAP request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        # === НОВИЙ парсер відповіді domain_intersection ===
        tasks = resp.get("tasks") or []
        rows: List[Tuple[str, int | str, str | int | str, str, str | int | str]] = []

        for t in tasks:
            result_list = t.get("result") or []
            if not result_list:
                continue

            r0 = result_list[0] or {}
            items = r0.get("items") or []
            if not items:
                continue

            data_block = t.get("data") or {}
            # ми відправляємо target1 = конкурент, target2 = наш сайт
            comp_name = data_block.get("target1") or "competitor"
            # our_domain = data_block.get("target2") or target  # якщо раптом треба

            for it in items:
                kd = it.get("keyword_data") or {}
                kw = kd.get("keyword") or it.get("keyword") or ""
                if not kw:
                    continue

                ki = kd.get("keyword_info") or {}
                vol = (
                    ki.get("search_volume")
                    or it.get("search_volume")
                    or 0
                )

                first = it.get("first_domain_serp_element") or {}
                second = it.get("second_domain_serp_element") or {}

                # target1 = конкурент → його позиція в first_domain_serp_element
                comp_rank = (
                    first.get("rank_group")
                    or first.get("rank_absolute")
                    or ""
                )

                # при intersections=false target2 часто взагалі не ранжується
                our_rank = (
                    second.get("rank_group")
                    or second.get("rank_absolute")
                    or ""
                )

                rows.append((kw, vol, our_rank, comp_name, comp_rank))


        if not rows:
            bal_now = get_balance(uid)
            await update.message.reply_text(
                f"Нічого не знайшов 😕\nБаланс: {bal_now}",
                reply_markup=services_menu_keyboard(),
            )
            return

        lines = []
        for kw, vol, my, comp_name, comp_rank in rows[:10]:
            lines.append(f"• {kw} — vol:{vol}, ми:{my}, vs {comp_name}: {comp_rank}")
        preview = "⚔️ *Keyword Gap*\n" + "\n".join(lines)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["keyword", "search_volume", "our_rank", "competitor", "competitor_rank"])
        for kw, vol, my, comp_name, comp_rank in rows:
            w.writerow([kw, vol, my, comp_name, comp_rank])
        csv_bytes = buf.getvalue().encode()

        bal_now = get_balance(uid)
        await update.message.reply_text(
            preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
            parse_mode="Markdown",
            reply_markup=services_menu_keyboard(),
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="keyword_gap.csv"),
            caption="CSV з результатами Keyword Gap",
        )
        return


# ============ on_menu_text ============

async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    # Якщо активний SERP / Ideas / GAP / SITE KW — обробляємо state-machine
    if context.user_data.get("serp_state"):
        await _handle_serp_flow(update, context, text)
        return

    if context.user_data.get("kwideas_state"):
        await handle_kwideas_flow(update, context, text)
        return

    if context.user_data.get("sitekw_state"):
        await handle_site_kw_flow(update, context, text)
        return

    if context.user_data.get("gap_state"):
        await handle_gap_flow(update, context, text)
        return

    # Перемикач нижніх меню
    if text in ("🧰 Сервіси", "Сервіси"):
        context.chat_data["in_services"] = True
        await _set_menu_keyboard(update, context, services_menu_keyboard())
        return

    if text == "⬅️ Назад" and context.chat_data.get("in_services"):
        context.chat_data["in_services"] = False
        await _set_menu_keyboard(update, context, main_menu_keyboard(_registered(uid)))
        return
  # 👇 оце ОБОВʼЯЗКОВО має бути
    if context.user_data.get("siteov_state"):
        await handle_site_overview_flow(update, context, text)
        return

    # Швидкий вибір сервісу (reply-кнопки)
    # Швидкий вибір сервісу (reply-кнопки)
    if text in ("🔍 SERP", "🧠 Keyword Ideas", "🌐 Ключі для сайту", "⚔️ Gap", "🔗 Backlinks", "🛠️ Аудит", "📈 Огляд сайту"):
        if text == "🔍 SERP":
            await _start_serp_flow(update, context)
            return
        if text == "🧠 Keyword Ideas":
            await start_kwideas_flow(update, context)
            return
        if text == "🌐 Ключі для сайту":
            await start_site_kw_flow(update, context)
            return
        if text == "⚔️ Gap":
            await start_gap_flow(update, context)
            return
        if text == "📈 Огляд сайту":
            await start_site_overview_flow(update, context)
            return

        # нижче лишаємо тільки ті, що працюють через one-line
        mapping = {
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


        mapping = {
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



    # One-line wizard
    aw = context.user_data.get("await_tool")
    if aw:
        context.user_data.pop("await_tool", None)

        if not dfs:
            return await update.message.reply_text(
                "DataForSEO не сконфігуровано. Додайте DATAFORSEO_LOGIN/PASSWORD у .env"
            )

        main, opts = _parse_opts(text)
        country_name = opts.get("country", "Ukraine")
        language_name = opts.get("lang", "Ukrainian")
        location_code = LOCATION_CODES.get(country_name, 2840)
        language_code = LANGUAGE_CODES.get(language_name, "en")
        limit = int(re.findall(r"\d+", opts.get("limit", "20"))[0]) if opts.get("limit") else 20

        prices = {
            "serp": SERP_CHARGE_UAH,
            "keywords": KW_IDEAS_CHARGE_UAH,
            "gap": GAP_CHARGE_UAH,
            "backlinks_ov": BACKLINKS_CHARGE_UAH,
            "audit": AUDIT_CHARGE_UAH,
            "site_overview": SITE_OVERVIEW_CHARGE_UAH,
        }

        need_credits = _uah_to_credits(prices.get(aw, 5.0))

        if not charge(uid, need_credits, f"svc:{aw}", main or "-"):
            return await update.message.reply_text(
                f"Недостатньо кредитів (потрібно {need_credits}). Поповніть баланс.",
                reply_markup=_topup_cta(),
            )

        try:
            # --- SERP (one-line) ---
            if aw == "serp":
                depth = int(re.findall(r"\d+", opts.get("depth", "10"))[0]) if opts.get("depth") else 10
                resp = await dfs.serp_google_organic(
                    main,
                    location_code=location_code,
                    language_code=language_code,
                    depth=depth,
                )
                items = _extract_first_items(resp)
                if not items:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(
                        f"Нічого не знайшов 😕\nБаланс: {bal_now}",
                        reply_markup=services_menu_keyboard(),
                    )

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
                await update.message.reply_text(
                    preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
                    parse_mode="Markdown",
                    reply_markup=services_menu_keyboard(),
                )
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="serp_top.csv"),
                    caption="CSV із результатами SERP"
                )
                return

            # --- Keyword Ideas (one-line) ---
            if aw == "keywords":
                resp = await dfs.keywords_for_keywords(
                    main,
                    location_code=location_code,
                    language_code=language_code,
                )

                items_all = find_keyword_items(resp)
                items = filter_keywords(items_all, min_search_volume=1)

                if not items:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(
                        f"Нічого не знайшов 😕 (або всі з 0 пошуку)\nБаланс: {bal_now}",
                        reply_markup=services_menu_keyboard(),
                    )

                items_limited = items[:limit]

                lines = []
                for it in items_limited[:10]:
                    kw = it.get("keyword") or it.get("keyword_text") or "—"
                    vol = (
                        it.get("search_volume")
                        or it.get("avg_monthly_searches")
                        or it.get("search_volume_avg")
                        or "-"
                    )
                    cpc = it.get("cpc") or it.get("cost_per_click") or "-"
                    lines.append(f"• {kw} — vol: {vol}, CPC: {cpc}")
                preview = "🧠 *Ідеї ключових*\n" + "\n".join(lines)

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "cpc"])
                for it in items_limited:
                    w.writerow([
                        it.get("keyword") or it.get("keyword_text") or "",
                        it.get("search_volume") or it.get("avg_monthly_searches") or it.get("search_volume_avg") or "",
                        it.get("cpc") or it.get("cost_per_click") or "",
                    ])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                await update.message.reply_text(
                    preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
                    parse_mode="Markdown",
                    reply_markup=services_menu_keyboard(),
                )
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="keyword_ideas.csv"),
                    caption="CSV з ідеями ключових"
                )
                return

            # --- GAP (one-line) ---
            if aw == "gap":
                comps_raw = opts.get("comps") or opts.get("competitors") or ""
                competitors = [x.strip() for x in comps_raw.split(",") if x.strip()]
                if not main or not competitors:
                    return await update.message.reply_text(
                        "Формат: `mydomain.com | comps=site1.com,site2.com`",
                        parse_mode="Markdown",
                    )

                resp = await dfs.keywords_gap(
                    main,
                    competitors,
                    location_code=location_code,
                    language_code=language_code,
                    limit=limit,
                )

                tasks = resp.get("tasks") or []
                rows = []
                for t in tasks:
                    result = t.get("result") or []
                    if not result:
                        continue
                    r0 = result[0]
                    items = r0.get("items") or []
                    data_block = t.get("data") or {}
                    intersections = data_block.get("intersections") or []
                    comp_label = intersections[0] if intersections else "target2"
                    comp_name = data_block.get(comp_label) or "competitor"
                    for it in items:
                        kw = it.get("keyword") or it.get("keyword_text") or ""
                        vol = it.get("search_volume") or it.get("avg_monthly_searches") or ""
                        my_rank = it.get("rank1") or it.get("target_rank") or it.get("rank") or ""
                        comp_rank = it.get("rank2") or ""
                        rows.append((kw, vol, my_rank, comp_name, comp_rank))

                if not rows:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(
                        f"Нічого не знайшов 😕\nБаланс: {bal_now}",
                        reply_markup=services_menu_keyboard(),
                    )

                lines = []
                for kw, vol, my, comp_name, comp_rank in rows[:10]:
                    lines.append(f"• {kw} — vol:{vol}, ми:{my}, vs {comp_name}: {comp_rank}")
                preview = "⚔️ *Keyword Gap*\n" + "\n".join(lines)

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "our_rank", "competitor", "competitor_rank"])
                for kw, vol, my, comp_name, comp_rank in rows:
                    w.writerow([kw, vol, my, comp_name, comp_rank])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                await update.message.reply_text(
                    preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
                    parse_mode="Markdown",
                    reply_markup=services_menu_keyboard(),
                )
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="keyword_gap.csv"),
                    caption="CSV з результатами Keyword Gap"
                )
                return
 # --- Site Overview (relevant_pages + ranked_keywords) ---
            if aw == "site_overview":
                target = main.strip()
                if not target:
                    return await update.message.reply_text(
                        "Формат: mydomain.com | country=Ukraine | lang=Ukrainian | pages=5 | limit=10"
                    )

                pages_param = opts.get("pages") or "5"
                try:
                    pages_limit = int(re.findall(r"\d+", pages_param)[0])
                except Exception:
                    pages_limit = 5
                if pages_limit <= 0:
                    pages_limit = 5

                kw_limit = limit if limit > 0 else 10

                site_resp = await dfs.relevant_pages(
                    target,
                    location_code=location_code,
                    language_code=language_code,
                    limit=pages_limit,
                )
                site_res = _extract_result(site_resp)
                pages = site_res.get("items") or []

                if not pages:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(
                        f"Нічого не знайшов по сайту 😕\nБаланс: {bal_now}",
                        reply_markup=services_menu_keyboard(),
                    )

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow([
                    "page_url",
                    "organic_keywords_count",
                    "organic_etv",
                    "organic_estimated_paid_traffic_cost",
                    "keyword",
                    "search_volume",
                    "rank",
                    "etv",
                ])

                preview_lines = [f"📈 Огляд сайту {target} ({country_name}, {language_name})\n"]
                page_idx = 1

                for p in pages[:pages_limit]:
                    page_url = p.get("page_address") or ""
                    metrics = (p.get("metrics") or {}).get("organic") or {}
                    kw_count = metrics.get("count") or 0
                    etv_val = metrics.get("etv") or 0
                    paid_cost = metrics.get("estimated_paid_traffic_cost") or 0

                    rel = urlparse(page_url).path or "/"

                    try:
                        kw_resp = await dfs.ranked_keywords_for_url(
                            target,
                            location_code=location_code,
                            language_code=language_code,
                            relative_url=rel,
                            limit=kw_limit,
                        )
                        kw_res = _extract_result(kw_resp)
                        kw_items = kw_res.get("items") or []
                    except Exception:
                        kw_items = []

                    preview_lines.append(
                        f"{page_idx}. {page_url}\n"
                        f"   keywords: {kw_count}, ETV: {etv_val:.2f}, paid_est: {paid_cost:.2f}"
                    )

                    for kw_item in kw_items[:3]:
                        kd = kw_item.get("keyword_data") or {}
                        ki = kd.get("keyword_info") or {}
                        se = (kw_item.get("ranked_serp_element") or {}).get("serp_item") or {}

                        kw_str = kd.get("keyword") or "—"
                        sv = ki.get("search_volume") or 0
                        rank = se.get("rank_group") or se.get("rank_absolute") or "-"
                        kw_etv = se.get("etv") or 0
                        preview_lines.append(f"      • {kw_str} — vol:{sv}, pos:{rank}, etv:{kw_etv:.2f}")

                    for kw_item in kw_items:
                        kd = kw_item.get("keyword_data") or {}
                        ki = kd.get("keyword_info") or {}
                        se = (kw_item.get("ranked_serp_element") or {}).get("serp_item") or {}

                        kw_str = kd.get("keyword") or ""
                        sv = ki.get("search_volume") or ""
                        rank = se.get("rank_group") or se.get("rank_absolute") or ""
                        kw_etv = se.get("etv") or ""
                        w.writerow([
                            page_url,
                            kw_count,
                            etv_val,
                            paid_cost,
                            kw_str,
                            sv,
                            rank,
                            kw_etv,
                        ])

                    preview_lines.append("")
                    page_idx += 1

                csv_bytes = buf.getvalue().encode()
                bal_now = get_balance(uid)

                preview_text = "\n".join(preview_lines) + f"\n💰 Списано {need_credits}. Баланс: {bal_now}"
                await update.message.reply_text(
                    preview_text,
                    reply_markup=services_menu_keyboard(),
                )
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename=f"{target}_overview.csv"),
                    caption="CSV: сторінки сайту + ключі, по яких вони ранжуються"
                )
                return

            # --- Backlinks Overview (one-line) ---
            if aw == "backlinks_ov":
                target = main

                # 1) Summary
                summary = await dfs.backlinks_summary(target)
                s = _extract_result(summary)

                totals = s.get("totals") or {}
                backlinks_total = totals.get("backlinks") or s.get("backlinks") or "-"
                refdomains_total = totals.get("referring_domains") or s.get("referring_domains") or "-"
                dofollow_total = totals.get("dofollow") or s.get("dofollow") or "-"
                nofollow_total = totals.get("nofollow") or s.get("nofollow") or "-"

                # 2) Детальні backlinks (як у твоєму JSON-прикладі)
                bl_resp = await dfs.backlinks_live(target, limit=100)
                bl_items = _extract_first_items(bl_resp)

                # Порахувати dofollow / nofollow у цьому зрізі (top 100)
                dof_sample = sum(1 for it in bl_items if it.get("dofollow") is True)
                nof_sample = sum(1 for it in bl_items if it.get("dofollow") is False)

                # 3) Топ referring domains
                rdom = await dfs.refdomains_live(target, limit=20)
                r_items = _extract_first_items(rdom)
                rd_lines = []
                for it in r_items[:10]:
                    d = it.get("domain") or it.get("referring_domain") or "-"
                    b = it.get("backlinks") or "-"
                    rd_lines.append(f"• {escape(str(d))} — {escape(str(b))} backlinks")

                # 4) Топ anchors
                anch_resp = await dfs.anchors_live(target, limit=50)
                a_items = _extract_first_items(anch_resp)
                a_lines = []
                for it in a_items[:10]:
                    a = it.get("anchor") or "-"
                    b = it.get("backlinks") or "-"
                    a_lines.append(f"• {escape(str(a))[:60]} — {escape(str(b))}")

                # 5) CSV з "повною" інфою по backlinks/live
                buf = io.StringIO()
                w = csv.writer(buf)

                # Шапка CSV — основні поля
                w.writerow([
                    "Ref Domain",
                    "Ref URL",
                    "URL",
                    "Is dofollow?",
                    "Spam Score",
                    "Rank",
                    "Page Rank",
                    "Domain Rank",
                    "Ref URL Status Code",
                    "URL Status Code ",
                    "New?",
                    "Lost?",
                    "Broken?",
                    "First Seen",
                    "Prev Seen",
                    "Last Seen",
                    "anchor",
                    "Text Pre",
                    "Text Post",
                    "Link Type",
                    "Links Count",
                    "Group Count",
                    "Ref Domain IP",
                    "Ref Domain Country",
                    "Page From External Links",
                    "Page From Internal Links",
                    "Page From Size",
                    "Page From language",
                    "Page from Title",
                    "Spam Score URL",
                    "Is Indirect Link",
                    "Indirect Link Path",
                ])

                for it in bl_items:
                    w.writerow([
                        it.get("domain_from") or "",
                        it.get("url_from") or "",
                        it.get("url_to") or "",
                        it.get("dofollow"),
                        it.get("backlink_spam_score") or "",
                        it.get("rank") or "",
                        it.get("page_from_rank") or "",
                        it.get("domain_from_rank") or "",
                        it.get("page_from_status_code") or "",
                        it.get("url_to_status_code") or "",
                        it.get("is_new"),
                        it.get("is_lost"),
                        it.get("is_broken"),
                        it.get("first_seen") or "",
                        it.get("prev_seen") or "",
                        it.get("last_seen") or "",
                        (it.get("anchor") or "")[:255],
                        (it.get("text_pre") or "")[:255],
                        (it.get("text_post") or "")[:255],
                        it.get("item_type") or "",
                        it.get("links_count") or "",
                        it.get("group_count") or "",
                        it.get("domain_from_ip") or "",
                        it.get("domain_from_country") or "",
                        it.get("page_from_external_links") or "",
                        it.get("page_from_internal_links") or "",
                        it.get("page_from_size") or "",
                        it.get("page_from_language") or "",
                        it.get("page_from_title") or "",
                        it.get("url_to_spam_score") or "",
                        it.get("is_indirect_link"),
                        it.get("indirect_link_path") or "",
                    ])

                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                target_safe = escape(target)

                txt = (
                    f"🔗 <b>Backlinks огляд для</b> <b>{target_safe}</b>\n"
                    f"• Backlinks (всього): {escape(str(backlinks_total))}\n"
                    f"• Referring domains: {escape(str(refdomains_total))}\n"
                    f"• Dofollow (total): {escape(str(dofollow_total))} | "
                    f"Nofollow (total): {escape(str(nofollow_total))}\n"
                    f"• У топ {len(bl_items)} посилань: dofollow {dof_sample}, nofollow {nof_sample}\n\n"
                    f"Топ реф.доменів:\n" + ("\n".join(rd_lines) if rd_lines else "—") + "\n\n"
                    f"Топ анкорів:\n" + ("\n".join(a_lines) if a_lines else "—") +
                    f"\n\n💰 Списано {escape(str(need_credits))}. Баланс: {escape(str(bal_now))}"
                )

                await update.message.reply_text(
                    txt,
                    parse_mode="HTML",
                    reply_markup=services_menu_keyboard()
                )

                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename=f"{target}_backlinks_live_overview.csv"),
                    caption="CSV з деталями по backlinks (live, top 100)"
                )
                return

            # --- Audit URL (one-line) ---
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
                desc = meta.get("description") or ""
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
                await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=services_menu_keyboard())
                return

            # fallback
            bal_now = get_balance(uid)
            return await update.message.reply_text(
                f"Інструмент поки не реалізовано. Баланс: {bal_now}",
                reply_markup=services_menu_keyboard(),
            )

        except Exception as e:
            return await update.message.reply_text(f"Помилка: {e}")

    # Стандартні пункти меню (коли не чекаємо параметри інструменту)
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

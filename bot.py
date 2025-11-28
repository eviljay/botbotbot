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


load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DFS_LOGIN = os.getenv("DATAFORSEO_LOGIN")
DFS_PASS = os.getenv("DATAFORSEO_PASSWORD")
DFS_BASE = os.getenv("DATAFORSEO_BASE", "https://api.dataforseo.com")
LIQPAY_PUB = os.getenv("LIQPAY_PUBLIC_KEY", "")
LIQPAY_PRIV = os.getenv("LIQPAY_PRIVATE_KEY", "")
CREDIT_PRICE_UAH = _parse_float_env("CREDIT_PRICE_UAH", 1.0)

SERP_CHARGE_UAH                   = _parse_float_env("SERP_CHARGE_UAH", 5.0)
KW_IDEAS_CHARGE_UAH               = _parse_float_env("KW_IDEAS_CHARGE_UAH", 5.0)
GAP_CHARGE_UAH                    = _parse_float_env("GAP_CHARGE_UAH", 10.0)
BACKLINKS_CHARGE_UAH              = _parse_float_env("BACKLINKS_CHARGE_UAH", 5.0)
BACKLINKS_FULL_EXPORT_CHARGE_UAH  = _parse_float_env("BACKLINKS_FULL_EXPORT_CHARGE_UAH", 5.0)
AUDIT_CHARGE_UAH                  = _parse_float_env("AUDIT_CHARGE_UAH", 5.0)

ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()]

dfs = DataForSEO(DFS_LOGIN, DFS_PASS, DFS_BASE) if DFS_LOGIN and DFS_PASS else None

# ====== Утиліти ======
def main_menu_keyboard(registered: bool) -> ReplyKeyboardMarkup:
    if registered:
        rows = [
            [KeyboardButton("🧰 Сервіси"), KeyboardButton("💳 Поповнити")],
            [KeyboardButton("📊 Баланс"), KeyboardButton("☎️ Телефон")],
        ]
    else:
        rows = [
            [KeyboardButton("🧰 Сервіси")],
            [KeyboardButton("📊 Баланс"), KeyboardButton("☎️ Телефон")],
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


SERP_LOCATIONS = [
    "Ukraine",
    "Poland",
    "Germany",
    "Sweden",
    "United States",
    "United Kingdom",
    "Canada",
]

SERP_LANGUAGES = [
    "Ukrainian",
    "Polish",
    "German",
    "Swedish",
    "English",
]


def countries_keyboard() -> ReplyKeyboardMarkup:
    rows = []
    row: list[KeyboardButton] = []
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
    row: list[KeyboardButton] = []
    for i, name in enumerate(SERP_LANGUAGES, start=1):
        row.append(KeyboardButton(name))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def _set_menu_keyboard(update: Update, context: ContextTypes.DEFAULT_TYPE, kb: ReplyKeyboardMarkup):
    """
    Виставляємо Reply-клавіатуру без блимаючого зникання:
    1) надсилаємо окремим повідомленням
    2) відповідаємо користувачу з тією ж клавіатурою
    """
    try:
        await update.message.reply_chat_action("typing")
    except Exception:
        pass
    await update.message.reply_text("Оновлюю меню…", reply_markup=kb)


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


def _topup_cta() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton("💳 Поповнити")]], resize_keyboard=True)

# ... (тут ідуть всі інші твої допоміжні функції, checkout, телефони, баланс і т.д. – я їх не змінював) ...


# ============ ДІАЛОГОВІ ФЛОУ ДЛЯ SERP / KEYWORD IDEAS / GAP ============

async def _start_serp_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Почати діалоговий SERP-флоу."""
    context.user_data["serp_state"] = "keyword"
    context.user_data["serp_params"] = {}
    # гасимо старий wizard-режим
    context.user_data.pop("await_tool", None)
    await update.message.reply_text(
        "🔍 SERP трекінг\n\nВведи keyword, який хочеш перевірити в Google:",
        reply_markup=ReplyKeyboardRemove(),
    )


async def _handle_serp_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """State-machine для SERP: keyword → country → language → depth."""
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

    # 1) keyword
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

    # 2) country
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

    # 3) language
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
            "Глибина SERP: обери 10, 20 або 30.",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("10"), KeyboardButton("20"), KeyboardButton("30")]],
                resize_keyboard=True,
            ),
        )
        return

    # 4) depth + запуск запиту
    if state == "depth":
        try:
            depth = int(text)
        except ValueError:
            await update.message.reply_text("Напиши 10, 20 або 30 як глибину:")
            return
        if depth not in (10, 20, 30):
            await update.message.reply_text("Підтримуються значення 10, 20 або 30.")
            return

        keyword = (params.get("keyword") or "").strip()
        country = params.get("country") or "Ukraine"
        language = params.get("language") or "Ukrainian"

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
                location_name=country,
                language_name=language,
                depth=depth,
            )
        except Exception as e:
            log.exception("SERP request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        items = _extract_first_items(resp)
        if not items:
            bal_now = get_balance(uid)
            await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")
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
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="serp_top.csv"),
            caption="CSV із результатами SERP",
        )
        return


async def start_kwideas_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Почати діалоговий флоу Keyword Ideas."""
    context.user_data["kwideas_state"] = "keyword"
    context.user_data["kwideas"] = {}
    context.user_data.pop("await_tool", None)
    await update.message.reply_text(
        "🧠 Keyword Ideas\n\nВведи seed keyword:",
        reply_markup=ReplyKeyboardRemove(),
    )


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

    # 1) keyword
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

    # 2) country
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

    # 3) language
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

    # 4) limit + запуск
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
        country = data.get("country") or "Ukraine"
        language = data.get("language") or "Ukrainian"

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
            f"Шукаю keyword ideas для *{kw}* ({country}, {language}, {limit})…",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )

        try:
            resp = await dfs.keywords_for_keywords(
                kw,
                location_name=country,
                language_name=language,
                limit=limit,
            )
        except Exception as e:
            log.exception("KW ideas request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        items = _extract_first_items(resp)
        if not items:
            bal_now = get_balance(uid)
            await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")
            return

        lines = []
        for it in items[:10]:
            kw_i = it.get("keyword") or it.get("keyword_text") or "—"
            vol = it.get("search_volume") or it.get("avg_monthly_searches") or it.get("search_volume_avg") or "-"
            cpc = it.get("cpc") or it.get("cost_per_click") or "-"
            lines.append(f"• {kw_i} — vol: {vol}, CPC: {cpc}")
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
        await update.message.reply_text(
            preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
            parse_mode="Markdown",
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="keyword_ideas.csv"),
            caption="CSV з ідеями ключових",
        )
        return


async def start_gap_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Почати діалоговий GAP-флоу."""
    context.user_data["gap_state"] = "target"
    context.user_data["gap"] = {}
    context.user_data.pop("await_tool", None)
    await update.message.reply_text(
        "⚔️ GAP\n\nВведи свій сайт (target), напр. `mydomain.com`:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )


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

    # 1) target
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
            "Тепер введи конкурентів через кому, напр.: `site1.com, site2.com, site3.com`",
            parse_mode="Markdown",
        )
        return

    # 2) competitors
    if state == "competitors":
        raw = text.strip()
        if not raw:
            await update.message.reply_text(
                "Введи хоча б одного конкурента через кому, напр.: `site1.com, site2.com`",
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

    # 3) country
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

    # 4) language + запуск
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
        country = data.get("country") or "Ukraine"
        language = data.get("language") or "Ukrainian"

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
                target,
                competitors,
                location_name=country,
                language_name=language,
                limit=50,
            )
        except Exception as e:
            log.exception("GAP request failed")
            await update.message.reply_text(f"Помилка від DataForSEO: {e}")
            return

        tasks = resp.get("tasks") or []
        rows = []
        for t in tasks:
            result = t.get("result") or []
            if not result:
                continue
            r0 = result[0]
            items = r0.get("items") or []
            comp_list = (t.get("data") or {}).get("competitors") or ["competitor"]
            comp_name = comp_list[0] if isinstance(comp_list, list) and comp_list else "competitor"
            for it in items:
                kw = it.get("keyword") or it.get("keyword_text") or ""
                vol = it.get("search_volume") or it.get("avg_monthly_searches") or ""
                my_rank = it.get("target_rank") or it.get("rank") or ""
                comp_ranks = it.get("competitor_ranks") or it.get("ranks") or {}
                rows.append((kw, vol, my_rank, comp_name, comp_ranks))

        if not rows:
            bal_now = get_balance(uid)
            await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")
            return

        lines = []
        for kw, vol, my, comp_name, comp_ranks in rows[:10]:
            if isinstance(comp_ranks, dict):
                comp_str = ", ".join(f"{k}:{v}" for k, v in comp_ranks.items())
            elif isinstance(comp_ranks, list):
                comp_str = ", ".join(str(x) for x in comp_ranks[:3])
            else:
                comp_str = "-"
            lines.append(f"• {kw} — vol:{vol}, ми:{my}, vs {comp_name}: {comp_str}")
        preview = "⚔️ *Keyword Gap*\n" + "\n".join(lines)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["keyword", "search_volume", "our_rank", "competitor", "competitor_ranks"])
        for kw, vol, my, comp_name, comp_ranks in rows:
            if isinstance(comp_ranks, dict):
                comp_str = "; ".join(f"{k}:{v}" for k, v in comp_ranks.items())
            elif isinstance(comp_ranks, list):
                comp_str = "; ".join(str(x) for x in comp_ranks)
            else:
                comp_str = ""
            w.writerow([kw, vol, my, comp_name, comp_str])
        csv_bytes = buf.getvalue().encode()

        bal_now = get_balance(uid)
        await update.message.reply_text(
            preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}",
            parse_mode="Markdown",
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(csv_bytes), filename="keyword_gap.csv"),
            caption="CSV з результатами Keyword Gap",
        )
        return


# ============ on_menu_text з урахуванням нових флоу ============

async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id

    # Якщо активний якийсь діалоговий флоу — обробляємо його тут
    if context.user_data.get("serp_state"):
        await _handle_serp_flow(update, context, text)
        return

    if context.user_data.get("kwideas_state"):
        await handle_kwideas_flow(update, context, text)
        return

    if context.user_data.get("gap_state"):
        await handle_gap_flow(update, context, text)
        return

    # Перемикач нижніх меню
    if text in ("🧰 Сервіси", "Сервіси"):
        context.chat_data["in_services"] = True
        await _set_menu_keyboard(update, context, services_menu_keyboard())
        return

    if text == "⬅️ Назад" and not (
        context.user_data.get("serp_state")
        or context.user_data.get("kwideas_state")
        or context.user_data.get("gap_state")
    ):
        context.chat_data["in_services"] = False
        await _set_menu_keyboard(update, context, main_menu_keyboard(_registered(uid)))
        return

    # Швидкий вибір сервісу (reply-кнопки)
    if text in ("🔍 SERP", "🧠 Keyword Ideas", "⚔️ Gap", "🔗 Backlinks", "🛠️ Аудит"):
        if text == "🔍 SERP":
            await _start_serp_flow(update, context)
            return
        if text == "🧠 Keyword Ideas":
            await start_kwideas_flow(update, context)
            return
        if text == "⚔️ Gap":
            await start_gap_flow(update, context)
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

    # Wizard для сервісів (залишив для Backlinks/Audit та старого one-line input)
    aw = context.user_data.get("await_tool")
    if aw:
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
            # ===== SERP (старий формат: one-line) =====
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

            # ===== Keywords Ideas (старий формат) =====
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

            # ===== Keyword Gap (перероблений, без target2 / invalid field) =====
            if aw == "gap":
                comps_raw = opts.get("comps") or opts.get("competitors") or ""
                competitors = [x.strip() for x in comps_raw.split(",") if x.strip()]
                if not main or not competitors:
                    return await update.message.reply_text(
                        "Формат: `mydomain.com | comps=site1.com,site2.com`",
                        parse_mode="Markdown",
                    )

                rows = []
                try:
                    resp = await dfs.keywords_gap(
                        main,
                        competitors,
                        location_name=country,
                        language_name=lang,
                        limit=limit,
                    )
                except Exception as e:
                    log.exception("GAP request failed (wizard)")
                    await update.message.reply_text(f"Помилка від DataForSEO: {e}")
                    return

                tasks = resp.get("tasks") or []
                for t in tasks:
                    result = t.get("result") or []
                    if not result:
                        continue
                    r0 = result[0]
                    items = r0.get("items") or []
                    comp_list = (t.get("data") or {}).get("competitors") or ["competitor"]
                    comp_name = comp_list[0] if isinstance(comp_list, list) and comp_list else "competitor"
                    for it in items:
                        kw = it.get("keyword") or it.get("keyword_text") or ""
                        vol = it.get("search_volume") or it.get("avg_monthly_searches") or ""
                        my_rank = it.get("target_rank") or it.get("rank") or ""
                        comp_ranks = it.get("competitor_ranks") or it.get("ranks") or {}
                        rows.append((kw, vol, my_rank, comp_name, comp_ranks))

                if not rows:
                    bal_now = get_balance(uid)
                    return await update.message.reply_text(f"Нічого не знайшов 😕\nБаланс: {bal_now}")

                lines = []
                for kw, vol, my, comp_name, comp_ranks in rows[:10]:
                    if isinstance(comp_ranks, dict):
                        comp_str = ", ".join(f"{k}:{v}" for k, v in comp_ranks.items())
                    elif isinstance(comp_ranks, list):
                        comp_str = ", ".join(str(x) for x in comp_ranks[:3])
                    else:
                        comp_str = "-"
                    lines.append(f"• {kw} — vol:{vol}, ми:{my}, vs {comp_name}: {comp_str}")
                preview = "⚔️ *Keyword Gap*\n" + "\n".join(lines)

                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["keyword", "search_volume", "our_rank", "competitor", "competitor_ranks"])
                for kw, vol, my, comp_name, comp_ranks in rows:
                    if isinstance(comp_ranks, dict):
                        comp_str = "; ".join(f"{k}:{v}" for k, v in comp_ranks.items())
                    elif isinstance(comp_ranks, list):
                        comp_str = "; ".join(str(x) for x in comp_ranks)
                    else:
                        comp_str = ""
                    w.writerow([kw, vol, my, comp_name, comp_str])
                csv_bytes = buf.getvalue().encode()

                bal_now = get_balance(uid)
                await update.message.reply_text(preview + f"\n\n💰 Списано {need_credits}. Баланс: {bal_now}", parse_mode="Markdown")
                await update.message.reply_document(
                    document=InputFile(io.BytesIO(csv_bytes), filename="keyword_gap.csv"),
                    caption="CSV з результатами Keyword Gap"
                )
                return

            # ===== Backlinks / Audit далі по старому... =====
            # (цей код я не переписував; він залишився таким, як у тебе був)
            # ...
        except Exception as e:
            log.exception("Unexpected error in on_menu_text wizard")
            await update.message.reply_text(f"Неочікувана помилка: {e}")
            return

# ... і далі по файлу – handler-и start, help, поповнення, checkout, main() і т.д.

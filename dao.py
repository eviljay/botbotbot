# dao.py
import os
import json
import sqlite3
import threading
from typing import Optional, Tuple


DB_PATH = os.getenv("DB_PATH", "./data/bot.db")
_lock = threading.Lock()

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS users (
      user_id INTEGER PRIMARY KEY,
      balance INTEGER NOT NULL DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS payments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      provider TEXT NOT NULL,
      order_reference TEXT NOT NULL UNIQUE,
      amount INTEGER NOT NULL,
      currency TEXT NOT NULL,
      status TEXT NOT NULL,
      raw_json TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS usage (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      domain TEXT,
      scope TEXT,
      price INTEGER NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    """
]
# dao.py — додай поруч з іншими функціями
def add_credits(user_id: int, credits: int) -> int:
    """
    Додає користувачу credits і повертає новий баланс.
    """
    ensure_user(user_id)
    import sqlite3
    conn = sqlite3.connect(DB_PATH)  # використовуй той самий DB_PATH, що й інші функції
    try:
        cur = conn.cursor()
        cur.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE user_id = ?", (int(credits), int(user_id)))
        conn.commit()
        # повертаємо оновлений баланс
        row = cur.execute("SELECT balance FROM users WHERE user_id = ?", (int(user_id),)).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()
        
def _ensure_dir() -> None:
    d = os.path.dirname(DB_PATH) or "."
    os.makedirs(d, exist_ok=True)

def _conn() -> sqlite3.Connection:
    _ensure_dir()
    return sqlite3.connect(DB_PATH)

def _has_column(c: sqlite3.Connection, table: str, col: str) -> bool:
    cur = c.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def init_db() -> None:
    with _conn() as c:
        for ddl in SCHEMA:
            c.execute(ddl)
        if not _has_column(c, "users", "phone"):
            c.execute("ALTER TABLE users ADD COLUMN phone TEXT")

def ensure_user(user_id: int) -> None:
    with _lock, _conn() as c:
        c.execute("INSERT OR IGNORE INTO users(user_id, balance) VALUES(?, 0)", (user_id,))

def get_balance(user_id: int) -> int:
    with _conn() as c:
        row = c.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        return int(row[0]) if row else 0

def get_phone(user_id: int) -> Optional[str]:
    with _conn() as c:
        row = c.execute("SELECT phone FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row[0] if row and row[0] is not None else None

def add_balance(user_id: int, credits: int) -> None:
    with _lock, _conn() as c:
        ensure_user(user_id)
        c.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (int(credits), user_id))

def charge(user_id: int, amount: int, domain: Optional[str], scope: Optional[str]) -> bool:
    with _lock, _conn() as c:
        row = c.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        bal = int(row[0]) if row else 0
        if bal < amount:
            return False
        c.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))
        c.execute(
            "INSERT INTO usage(user_id, domain, scope, price) VALUES(?,?,?,?)",
            (user_id, domain, scope, amount)
        )
        return True

def insert_payment(user_id: int, provider: str, order_reference: str, amount_cents: int, currency: str, status: str, raw: dict) -> None:
    with _lock, _conn() as c:
        c.execute(
            "INSERT INTO payments (user_id, provider, order_reference, amount, currency, status, raw_json) "
            "VALUES (?,?,?,?,?,?,?)",
            (user_id, provider, order_reference, amount_cents, currency, status, json.dumps(raw))
        )

def update_payment_status(order_reference: str, status: str, raw: Optional[dict] = None) -> None:
    with _lock, _conn() as c:
        if raw is None:
            c.execute(
                "UPDATE payments SET status=?, updated_at=datetime('now') WHERE order_reference=?",
                (status, order_reference)
            )
        else:
            c.execute(
                "UPDATE payments SET status=?, raw_json=?, updated_at=datetime('now') WHERE order_reference=?",
                (status, json.dumps(raw), order_reference)
            )

def find_payment(order_reference: str):
    with _conn() as c:
        return c.execute(
            "SELECT user_id, amount, currency, status FROM payments WHERE order_reference=?",
            (order_reference,)
        ).fetchone()

def register_or_update_phone(user_id: int, phone: str, initial_bonus: int = 10) -> tuple[bool, int]:
    """
    Нараховує бонус при ПЕРШОМУ додаванні телефону (коли phone був NULL/порожній).
    Якщо телефон уже був — лише оновлює без бонусу.
    Повертає (is_first_registration, credited_amount).
    """
    with _lock, _conn() as c:
        row = c.execute("SELECT phone FROM users WHERE user_id=?", (user_id,)).fetchone()
        if row is None:
            c.execute(
                "INSERT INTO users(user_id, phone, balance) VALUES(?,?,?)",
                (user_id, phone, int(initial_bonus))
            )
            return True, int(initial_bonus)
        current_phone = row[0]
        if not current_phone:
            c.execute(
                "UPDATE users SET phone=?, balance = balance + ? WHERE user_id=?",
                (phone, int(initial_bonus), user_id)
            )
            return True, int(initial_bonus)
        if current_phone != phone:
            c.execute("UPDATE users SET phone=? WHERE user_id=?", (phone, user_id))
            return False, 0
        return False, 0

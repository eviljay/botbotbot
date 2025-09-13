# dao.py
import os
import sqlite3
from contextlib import contextmanager
from typing import Optional, Tuple, List, Dict

DB_PATH = os.getenv("DB_PATH", "/root/mybot/data/bot.db")

@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    with _conn() as c:
        # базова таблиця користувачів
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0,
            phone   TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # історія списань/поповнень (якщо ще нема)
        c.execute("""
        CREATE TABLE IF NOT EXISTS balance_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            delta   INTEGER NOT NULL,
            reason  TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """)

def ensure_user(user_id: int):
    with _conn() as c:
        c.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))

def get_balance(user_id: int) -> int:
    with _conn() as c:
        row = c.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        return int(row["balance"]) if row else 0

def get_phone(user_id: int) -> Optional[str]:
    with _conn() as c:
        row = c.execute("SELECT phone FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["phone"] if (row and row["phone"]) else None

def register_or_update_phone(user_id: int, phone: str, initial_bonus: int = 0) -> Tuple[bool, int]:
    """
    Повертає: (is_new, credited)
    Якщо користувача створено вперше і телефону не було — нарахувати bonus.
    """
    credited = 0
    with _conn() as c:
        # чи був юзер
        existed = c.execute("SELECT phone FROM users WHERE user_id=?", (user_id,)).fetchone()
        if existed is None:
            c.execute("INSERT INTO users (user_id, phone, balance) VALUES (?, ?, ?)",
                      (user_id, phone, initial_bonus))
            credited = initial_bonus
            if credited:
                c.execute("INSERT INTO balance_log (user_id, delta, reason) VALUES (?, ?, ?)",
                          (user_id, credited, "initial_bonus"))
            return True, credited
        else:
            # якщо телефону не було і є бонус
            if (not existed["phone"]) and initial_bonus > 0:
                c.execute("UPDATE users SET phone=?, balance=balance+? WHERE user_id=?",
                          (phone, initial_bonus, user_id))
                credited = initial_bonus
                c.execute("INSERT INTO balance_log (user_id, delta, reason) VALUES (?, ?, ?)",
                          (user_id, credited, "initial_bonus"))
            else:
                c.execute("UPDATE users SET phone=? WHERE user_id=?", (phone, user_id))
            return False, credited

def charge(user_id: int, cost: int, domain: str = "", scope: str = "") -> bool:
    """
    Списує cost кредитів. Повертає True/False.
    """
    with _conn() as c:
        row = c.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return False
        bal = int(row["balance"])
        if bal < cost:
            return False
        c.execute("UPDATE users SET balance=balance-? WHERE user_id=?", (cost, user_id))
        c.execute("INSERT INTO balance_log (user_id, delta, reason) VALUES (?, ?, ?)",
                  (user_id, -cost, f"backlinks:{domain}:{scope}"))
        return True

# ====== АДМІН-ЮТІЛІТИ ======

def list_users(offset: int = 0, limit: int = 10) -> List[Dict]:
    with _conn() as c:
        rows = c.execute("""
            SELECT user_id, balance, phone, created_at
            FROM users
            ORDER BY created_at DESC, user_id DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()
        return [dict(r) for r in rows]

def count_users() -> int:
    with _conn() as c:
        row = c.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
        return int(row["cnt"]) if row else 0

def get_user(user_id: int) -> Optional[Dict]:
    with _conn() as c:
        row = c.execute("""
            SELECT user_id, balance, phone, created_at
            FROM users WHERE user_id=?
        """, (user_id,)).fetchone()
        return dict(row) if row else None

def add_credits(user_id: int, credits: int, reason: str = "admin_adjust") -> bool:
    """
    Додає (або віднімає, якщо credits від’ємні) кредити.
    """
    with _conn() as c:
        row = c.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return False
        c.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (credits, user_id))
        c.execute("INSERT INTO balance_log (user_id, delta, reason) VALUES (?, ?, ?)",
                  (user_id, credits, reason))
        return True

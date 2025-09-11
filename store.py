import sqlite3, os, threading

DB_PATH = os.getenv("DB_PATH", "bot.db")
_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.execute("""CREATE TABLE IF NOT EXISTS jobs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            domain TEXT NOT NULL,
            freq TEXT NOT NULL CHECK(freq IN ('daily','weekly')),
            created_at TEXT NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS snapshots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            url_from TEXT NOT NULL,
            first_seen TEXT,
            UNIQUE(domain, url_from)
        )""")

def add_job(chat_id: int, domain: str, freq: str):
    with _lock, sqlite3.connect(DB_PATH) as c:
        c.execute("INSERT INTO jobs(chat_id,domain,freq,created_at) VALUES(?,?,?,datetime('now'))",
                  (chat_id, domain, freq))
        return c.lastrowid

def get_jobs(chat_id: int = None):
    with sqlite3.connect(DB_PATH) as c:
        if chat_id:
            cur = c.execute("SELECT id, chat_id, domain, freq FROM jobs WHERE chat_id=?", (chat_id,))
        else:
            cur = c.execute("SELECT id, chat_id, domain, freq FROM jobs")
        rows = cur.fetchall()
        return [{"id": r[0], "chat_id": r[1], "domain": r[2], "freq": r[3]} for r in rows]

def save_snapshot(domain: str, url_from: str, first_seen: str):
    with _lock, sqlite3.connect(DB_PATH) as c:
        try:
            c.execute("INSERT OR IGNORE INTO snapshots(domain,url_from,first_seen) VALUES(?,?,?)",
                      (domain, url_from, first_seen))
            return c.rowcount > 0
        except sqlite3.IntegrityError:
            return False
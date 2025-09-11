# scheduler.py
from datetime import datetime
from store import add_job, get_jobs, save_snapshot

_dfs = None

def setup_scheduler(app, dfs_client):
    """Attach an hourly tick to Telegram's JobQueue."""
    global _dfs
    _dfs = dfs_client
    # start after 10s, then every hour
    app.job_queue.run_repeating(_tick, interval=3600, first=10)

async def add_watch_job(chat_id: int, domain: str, freq: str):
    return add_job(chat_id, domain, freq)

async def list_jobs(chat_id: int):
    return get_jobs(chat_id)

async def _process_domain(bot, chat_id: int, domain: str):
    data = await _dfs.backlinks_live(domain, limit=200, order_by="first_seen,desc")
    items = _extract_items(data)
    new_links = []
    for it in items:
        url_from = (it.get("page_from") or {}).get("url_from") or it.get("url_from")
        first_seen = it.get("first_seen")
        if url_from and save_snapshot(domain, url_from, first_seen):
            new_links.append((url_from, first_seen, it.get("anchor")))
    if new_links:
        msg = f"Нові беклінки для {domain}:\n" + "\n".join([f"• {u} (first_seen {fs})" for u, fs, _ in new_links[:20]])
        await bot.send_message(chat_id=chat_id, text=msg)

async def _tick(context):
    now = datetime.utcnow()
    bot = context.application.bot
    for j in get_jobs():
        if j["freq"] == "daily" and now.hour == 7:
            await _process_domain(bot, j["chat_id"], j["domain"])
        elif j["freq"] == "weekly" and now.weekday() == 0 and now.hour == 7:
            await _process_domain(bot, j["chat_id"], j["domain"])

# small helper shared here to avoid circular import

def _extract_items(resp: dict) -> list[dict]:
    if not resp:
        return []
    tasks = resp.get("tasks") or []
    if not tasks:
        return []
    task = tasks[0] or {}
    if task.get("status_code") and task["status_code"] != 20000:
        return []
    results = task.get("result") or []
    if not results:
        return []
    return results[0].get("items") or []
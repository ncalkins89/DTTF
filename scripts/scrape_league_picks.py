#!/usr/bin/env python3
"""
Scrape all entrants' picks from playoffpicker.com/league/?id=11.

Requires PLAYOFFPICKER_EMAIL and PLAYOFFPICKER_PASSWORD in .env.
Writes unpivoted rows to the league_picks table:
    (username, game_date, entry_name, player_name, pra_scored, player_id)
"""
import asyncio
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

LEAGUE_URL = "https://www.playoffpicker.com/league/?id=11"
LOGIN_URL  = "https://www.playoffpicker.com/login/"
SESSION_FILE = ROOT / "data" / ".playoffpicker_session.json"

MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _ascii(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()


def _build_player_id_resolver():
    """
    Returns a callable: resolve(last_name_str, game_date_str, pra_scored_int) → player_id | None

    Strategy:
    1. Get all game_logs rows for the given game_date from the DB.
    2. Filter to players whose last name (ascii-normalized) matches the site name.
    3. If exactly one match, return that player_id.
    4. If multiple, use pra_scored to pick the unique one.
    5. If zero, fall back to roster-only last-name match (no game log for this date).
    """
    import sqlite3
    from pathlib import Path
    DB_PATH = Path(__file__).parent.parent / "data" / "dttf.db"
    if not DB_PATH.exists():
        return lambda *_: None

    # Build last-name → [(player_id, player_name), ...] from rosters
    try:
        cx = sqlite3.connect(DB_PATH)
        cx.row_factory = sqlite3.Row
        roster_rows = cx.execute(
            "SELECT DISTINCT player_id, player_name FROM rosters"
        ).fetchall()
        cx.close()
    except Exception:
        return lambda *_: None

    # last_name_ascii → list of (player_id, full_name)
    name_to_ids: dict[str, list[tuple]] = {}
    for r in roster_rows:
        parts = r["player_name"].split()
        if not parts:
            continue
        last = _ascii(parts[-1])
        name_to_ids.setdefault(last, []).append((r["player_id"], r["player_name"]))

    def resolve(last_name: str, game_date: str, pra_scored) -> int | None:
        key = _ascii(last_name.strip())
        candidates = name_to_ids.get(key, [])
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0][0]
        # Multiple candidates: try to narrow by game_log for this date + PRA
        if pra_scored is None:
            return None
        try:
            cx = sqlite3.connect(DB_PATH)
            cx.row_factory = sqlite3.Row
            pids = [c[0] for c in candidates]
            ph = ",".join("?" * len(pids))
            log_rows = cx.execute(
                f"SELECT player_id, pra FROM game_logs WHERE game_date = ? AND player_id IN ({ph})",
                [game_date, *pids],
            ).fetchall()
            cx.close()
        except Exception:
            return None
        matches = [r["player_id"] for r in log_rows if r["pra"] is not None and round(r["pra"]) == int(pra_scored)]
        if len(matches) == 1:
            return matches[0]
        return None

    return resolve


def _parse_date(header: str) -> str | None:
    """'Wed 4/29' → '2026-04-29'  (assumes current year)"""
    m = re.search(r"(\d{1,2})/(\d{1,2})", header)
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    year = datetime.now().year
    return f"{year}-{month:02d}-{day:02d}"


async def _login(page, email: str, password: str) -> None:
    await page.goto(LOGIN_URL, timeout=30000)
    await page.wait_for_load_state("networkidle")
    await page.fill('input[name="username"]', email)
    await page.fill('input[name="password"]', password)
    await page.click('input[name="Login"]')
    await page.wait_for_load_state("networkidle")


async def _save_session(context) -> None:
    SESSION_FILE.parent.mkdir(exist_ok=True)
    import json
    cookies = await context.cookies()
    SESSION_FILE.write_text(json.dumps(cookies))


async def _load_session(context) -> bool:
    if not SESSION_FILE.exists():
        return False
    import json
    try:
        cookies = json.loads(SESSION_FILE.read_text())
        await context.add_cookies(cookies)
        return True
    except Exception:
        return False


async def scrape() -> list[dict]:
    from playwright.async_api import async_playwright

    email    = os.environ.get("PLAYOFFPICKER_EMAIL", "")
    password = os.environ.get("PLAYOFFPICKER_PASSWORD", "")
    if not email or not password:
        print("[league_picks] PLAYOFFPICKER_EMAIL / PASSWORD not set — skipping")
        return []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page    = await context.new_page()

        # Try reusing a saved session first
        loaded = await _load_session(context)
        if loaded:
            await page.goto(LEAGUE_URL, timeout=30000)
            await page.wait_for_load_state("networkidle")
            if "login" in page.url.lower():
                loaded = False  # session expired

        if not loaded:
            await _login(page, email, password)
            await _save_session(context)
            await page.goto(LEAGUE_URL, timeout=30000)
            await page.wait_for_load_state("networkidle")

        if "login" in page.url.lower():
            print("[league_picks] login failed — check credentials")
            await browser.close()
            return []

        # Toggle "All" to show full pick history
        await page.click('text=All')
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(1000)

        # Parse the table
        table = await page.query_selector("table")
        if not table:
            print("[league_picks] no table found on page")
            await browser.close()
            return []

        # Extract column dates from <th> headers
        headers = await table.query_selector_all("thead th")
        date_cols: list[str | None] = []
        for th in headers:
            text = (await th.inner_text()).strip()
            date_cols.append(_parse_date(text))  # None for Rank/Entry/Score cols

        rows_out: list[dict] = []
        tr_list = await table.query_selector_all("tbody tr")

        for tr in tr_list:
            tds = await tr.query_selector_all("td")
            if len(tds) < 4:
                continue

            # Entry name + username from second cell
            entry_el  = await tds[1].query_selector("a")
            user_el   = await tds[1].query_selector(".secondary")
            entry_name = (await entry_el.inner_text()).strip()  if entry_el  else ""
            username   = (await user_el.inner_text()).strip()   if user_el   else ""
            if not username:
                continue

            # Remaining cells are pick cells aligned with date_cols
            for i, td in enumerate(tds):
                if i >= len(date_cols) or date_cols[i] is None:
                    continue
                game_date = date_cols[i]

                player_el = await td.query_selector(".player .name a")
                score_el  = await td.query_selector(".player .score")
                if not player_el:
                    continue  # "Shown At Tipoff", "No Pick", or empty

                player_name = (await player_el.inner_text()).strip()
                pra_text    = (await score_el.inner_text()).strip() if score_el else ""
                try:
                    pra_scored = int(pra_text)
                except ValueError:
                    pra_scored = None

                rows_out.append({
                    "username":    username,
                    "game_date":   game_date,
                    "entry_name":  entry_name,
                    "player_name": player_name,
                    "pra_scored":  pra_scored,
                })

        await browser.close()
        return rows_out


def main() -> None:
    from src.db import init_db, upsert_league_picks
    init_db()

    rows = asyncio.run(scrape())
    if not rows:
        print("[league_picks] no rows scraped")
        return

    resolve = _build_player_id_resolver()
    resolved = 0
    for row in rows:
        if row.get("player_id") is None and row.get("player_name"):
            pid = resolve(row["player_name"], row["game_date"], row.get("pra_scored"))
            if pid:
                row["player_id"] = pid
                resolved += 1

    upsert_league_picks(rows)
    print(f"[league_picks] upserted {len(rows)} pick rows ({resolved} with player_id resolved)")

    # Summary
    dates = sorted({r["game_date"] for r in rows})
    users = len({r["username"] for r in rows})
    print(f"[league_picks] {users} entrants × {len(dates)} dates: {dates[0]} → {dates[-1]}")


if __name__ == "__main__":
    main()

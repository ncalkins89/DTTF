"""
Fetch per-player USG% and TS% (Regular Season averages) per season.
Run on Oracle: python3 scripts/fetch_player_season_stats.py
Writes to data/dttf.db → player_season_stats table.

Uses LeagueDashPlayerStats (one call per season = 6 total) rather than
per-player fetching, so it's fast and stays well within rate limits.
"""
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from nba_api.stats.endpoints import LeagueDashPlayerStats

DB_PATH = Path(__file__).parent.parent / "data" / "dttf.db"
SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
REQUEST_DELAY = 1.0


def _create_table(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS player_season_stats (
            player_id  INTEGER NOT NULL,
            season     TEXT    NOT NULL,
            usg_pct    REAL,
            ts_pct     REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (player_id, season)
        )
    """)
    con.commit()


def _fetch(season: str) -> list[tuple]:
    time.sleep(REQUEST_DELAY)
    df = LeagueDashPlayerStats(
        season=season,
        measure_type_detailed_defense="Advanced",
        season_type_all_star="Regular Season",
    ).league_dash_player_stats.get_data_frame()

    now = datetime.utcnow().isoformat()
    rows = []
    for _, row in df.iterrows():
        usg = row.get("USG_PCT")
        ts  = row.get("TS_PCT")
        rows.append((
            int(row["PLAYER_ID"]),
            season,
            float(usg) if usg is not None else None,
            float(ts)  if ts  is not None else None,
            now,
        ))
    return rows


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    _create_table(con)

    for season in SEASONS:
        print(f"Fetching {season}...", flush=True)
        try:
            rows = _fetch(season)
            con.executemany("""
                INSERT OR REPLACE INTO player_season_stats
                (player_id, season, usg_pct, ts_pct, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            """, rows)
            con.commit()
            print(f"  {len(rows)} players stored")
        except Exception as e:
            print(f"  FAILED: {e}")

    # Sanity check
    n = con.execute("SELECT COUNT(*) FROM player_season_stats").fetchone()[0]
    print(f"\nTotal rows in player_season_stats: {n}")
    sample = con.execute(
        "SELECT season, player_id, usg_pct, ts_pct FROM player_season_stats LIMIT 5"
    ).fetchall()
    for r in sample:
        print(" ", r)
    con.close()


if __name__ == "__main__":
    main()

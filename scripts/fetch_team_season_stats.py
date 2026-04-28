"""
Fetch historical team DEF_RATING and PACE (Regular Season) per season.
Run on Oracle: python3 scripts/fetch_team_season_stats.py
Writes to data/dttf.db → team_season_stats table.
"""
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from nba_api.stats.endpoints import LeagueDashTeamStats
from nba_api.stats.static import teams as nba_teams

DB_PATH = Path(__file__).parent.parent / "data" / "dttf.db"
SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
REQUEST_DELAY = 1.0


def _create_table(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS team_season_stats (
            team_id   INTEGER NOT NULL,
            season    TEXT    NOT NULL,
            team_abbr TEXT,
            def_rating REAL,
            pace      REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (team_id, season)
        )
    """)
    con.commit()


def _fetch(season: str) -> list[tuple]:
    time.sleep(REQUEST_DELAY)
    df = LeagueDashTeamStats(
        season=season,
        measure_type_detailed_defense="Advanced",
        season_type_all_star="Regular Season",
    ).league_dash_team_stats.get_data_frame()

    abbr_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
    now = datetime.utcnow().isoformat()
    rows = []
    for _, row in df.iterrows():
        tid = int(row["TEAM_ID"])
        rows.append((
            tid,
            season,
            abbr_map.get(tid, str(row.get("TEAM_ABBREVIATION", ""))),
            float(row["DEF_RATING"]) if "DEF_RATING" in df.columns else None,
            float(row["PACE"])       if "PACE"       in df.columns else None,
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
                INSERT OR REPLACE INTO team_season_stats
                (team_id, season, team_abbr, def_rating, pace, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, rows)
            con.commit()
            print(f"  {len(rows)} teams stored")
        except Exception as e:
            print(f"  FAILED: {e}")

    # Sanity check
    n = con.execute("SELECT COUNT(*) FROM team_season_stats").fetchone()[0]
    print(f"\nTotal rows in team_season_stats: {n}")
    sample = con.execute(
        "SELECT season, team_abbr, def_rating, pace FROM team_season_stats LIMIT 5"
    ).fetchall()
    for r in sample:
        print(" ", r)
    con.close()


if __name__ == "__main__":
    main()

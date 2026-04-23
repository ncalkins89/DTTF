"""
SQLite persistence layer — all fetched data lives here.

  - schedule              game matchups per date
  - odds_per_game         Vegas moneyline per-game win probs
  - series_standings      W-L records per series
  - de_projections        DraftEdge PTS/REB/AST for a given date
  - fd_projections        FanDuel PTS/REB/AST for a given date
  - injuries              current ESPN injury report (no date, always-current)
  - game_logs             player game logs (last 20, TTL 6h)
  - rosters               team rosters (TTL 24h)
  - team_defense_ratings  DEF_RATING per team/season (TTL 24h)
"""
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "dttf.db"


def init_db() -> None:
    with _conn() as cx:
        cx.executescript("""
        CREATE TABLE IF NOT EXISTS schedule (
            game_id       TEXT PRIMARY KEY,
            game_date     TEXT NOT NULL,
            home_team_id  INTEGER NOT NULL,
            away_team_id  INTEGER NOT NULL,
            home_team_abbr TEXT NOT NULL,
            away_team_abbr TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS odds_per_game (
            date              TEXT NOT NULL,
            team_abbr         TEXT NOT NULL,
            per_game_win_prob REAL NOT NULL,
            fetched_at        TEXT NOT NULL,
            PRIMARY KEY (date, team_abbr)
        );

        CREATE TABLE IF NOT EXISTS series_standings (
            season        TEXT NOT NULL,
            home_team_id  INTEGER NOT NULL,
            away_team_id  INTEGER NOT NULL,
            home_wins     INTEGER NOT NULL DEFAULT 0,
            away_wins     INTEGER NOT NULL DEFAULT 0,
            updated_at    TEXT NOT NULL,
            PRIMARY KEY (season, home_team_id, away_team_id)
        );

        CREATE TABLE IF NOT EXISTS de_projections (
            date       TEXT NOT NULL,
            player_id  INTEGER NOT NULL,
            pts        REAL,
            reb        REAL,
            ast        REAL,
            pra        REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (date, player_id)
        );

        CREATE TABLE IF NOT EXISTS game_logs (
            player_id   INTEGER NOT NULL,
            season      TEXT NOT NULL,
            game_date   TEXT NOT NULL,
            matchup     TEXT,
            wl          TEXT,
            pts         REAL,
            reb         REAL,
            ast         REAL,
            min         REAL,
            pra         REAL,
            season_type TEXT,
            fetched_at  TEXT NOT NULL,
            PRIMARY KEY (player_id, season, game_date, season_type)
        );

        CREATE TABLE IF NOT EXISTS rosters (
            team_id     INTEGER NOT NULL,
            season      TEXT NOT NULL,
            player_id   INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            position    TEXT,
            fetched_at  TEXT NOT NULL,
            PRIMARY KEY (team_id, season, player_id)
        );

        CREATE TABLE IF NOT EXISTS team_defense_ratings (
            team_id    INTEGER NOT NULL,
            season     TEXT NOT NULL,
            team_abbr  TEXT,
            def_rating REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (team_id, season)
        );

        CREATE TABLE IF NOT EXISTS fd_projections (
            date       TEXT NOT NULL,
            player_id  INTEGER NOT NULL,
            pts        REAL,
            reb        REAL,
            ast        REAL,
            pra        REAL,
            min        REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (date, player_id)
        );

        CREATE TABLE IF NOT EXISTS injuries (
            player_name TEXT PRIMARY KEY,
            status      TEXT,
            comment     TEXT,
            fetched_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS game_lines (
            date       TEXT NOT NULL,
            team_abbr  TEXT NOT NULL,
            spread     REAL,
            total      REAL,
            is_home    INTEGER NOT NULL DEFAULT 0,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (date, team_abbr)
        );
        """)


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    try:
        yield cx
        cx.commit()
    finally:
        cx.close()


# ── Upserts ─────────────────────────────────────────────────────────────────

def upsert_schedule(games: list[dict]) -> None:
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO schedule
               (game_id, game_date, home_team_id, away_team_id, home_team_abbr, away_team_abbr)
               VALUES (:game_id, :game_date, :home_team_id, :away_team_id,
                       :home_team_abbr, :away_team_abbr)""",
            games,
        )
    print(f"[db] upserted {len(games)} schedule rows")


def upsert_odds(game_date: str, odds: dict[str, float]) -> None:
    now = datetime.utcnow().isoformat()
    rows = [
        {"date": game_date, "team_abbr": abbr, "per_game_win_prob": prob, "fetched_at": now}
        for abbr, prob in odds.items()
    ]
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO odds_per_game
               (date, team_abbr, per_game_win_prob, fetched_at)
               VALUES (:date, :team_abbr, :per_game_win_prob, :fetched_at)""",
            rows,
        )
    print(f"[db] upserted odds for {len(rows)} teams on {game_date}")


def upsert_series_standings(season: str, standings: list[dict]) -> None:
    now = datetime.utcnow().isoformat()
    rows = [
        {
            "season": season,
            "home_team_id": s["home_team_id"],
            "away_team_id": s["away_team_id"],
            "home_wins": s["home_wins"],
            "away_wins": s["away_wins"],
            "updated_at": now,
        }
        for s in standings
    ]
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO series_standings
               (season, home_team_id, away_team_id, home_wins, away_wins, updated_at)
               VALUES (:season, :home_team_id, :away_team_id,
                       :home_wins, :away_wins, :updated_at)""",
            rows,
        )
    print(f"[db] upserted {len(rows)} series standings")


def upsert_de_projections(game_date: str, projections: dict[int, dict]) -> None:
    now = datetime.utcnow().isoformat()
    rows = [
        {
            "date": game_date,
            "player_id": pid,
            "pts": p["pts"],
            "reb": p["reb"],
            "ast": p["ast"],
            "pra": p["pra"],
            "fetched_at": now,
        }
        for pid, p in projections.items()
    ]
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO de_projections
               (date, player_id, pts, reb, ast, pra, fetched_at)
               VALUES (:date, :player_id, :pts, :reb, :ast, :pra, :fetched_at)""",
            rows,
        )
    print(f"[db] upserted {len(rows)} DraftEdge projections for {game_date}")


# ── Reads ────────────────────────────────────────────────────────────────────

def get_schedule(game_date: str) -> list[dict]:
    with _conn() as cx:
        rows = cx.execute(
            "SELECT * FROM schedule WHERE game_date = ?", (game_date,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_odds(game_date: str) -> dict[str, float]:
    with _conn() as cx:
        rows = cx.execute(
            "SELECT team_abbr, per_game_win_prob FROM odds_per_game WHERE date = ?",
            (game_date,),
        ).fetchall()
    return {r["team_abbr"]: r["per_game_win_prob"] for r in rows}


def get_latest_odds() -> dict[str, float]:
    """Returns odds from the most recently loaded date, regardless of requested date."""
    with _conn() as cx:
        row = cx.execute(
            "SELECT date FROM odds_per_game ORDER BY fetched_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {}
        rows = cx.execute(
            "SELECT team_abbr, per_game_win_prob FROM odds_per_game WHERE date = ?",
            (row["date"],),
        ).fetchall()
    return {r["team_abbr"]: r["per_game_win_prob"] for r in rows}


def get_series_standings(season: str) -> list[dict]:
    with _conn() as cx:
        rows = cx.execute(
            """SELECT home_team_id, away_team_id, home_wins, away_wins
               FROM series_standings WHERE season = ?""",
            (season,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_de_projections(game_date: str) -> dict[int, dict]:
    with _conn() as cx:
        rows = cx.execute(
            "SELECT player_id, pts, reb, ast, pra FROM de_projections WHERE date = ?",
            (game_date,),
        ).fetchall()
    return {
        r["player_id"]: {
            "pts": r["pts"], "reb": r["reb"],
            "ast": r["ast"], "pra": r["pra"],
        }
        for r in rows
    }


# ── Game logs ────────────────────────────────────────────────────────────────

def upsert_game_logs(player_id: int, season: str, logs_df) -> None:
    """Replace all stored rows for this player+season with fresh data."""
    now = datetime.utcnow().isoformat()
    with _conn() as cx:
        cx.execute(
            "DELETE FROM game_logs WHERE player_id = ? AND season = ?",
            (player_id, season),
        )
        cx.executemany(
            """INSERT OR REPLACE INTO game_logs
               (player_id, season, game_date, matchup, wl, pts, reb, ast, min, pra, season_type, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    player_id, season,
                    str(row["GAME_DATE"])[:10],
                    row.get("MATCHUP"), row.get("WL"),
                    row.get("PTS"), row.get("REB"), row.get("AST"),
                    row.get("MIN"), row.get("PRA"),
                    row.get("SEASON_TYPE"), now,
                )
                for _, row in logs_df.iterrows()
            ],
        )


def get_game_logs(player_id: int, season: str, ttl_seconds: int = 21600):
    """Returns (df, is_fresh). df is empty if no rows; is_fresh=False means re-fetch."""
    import pandas as pd
    cutoff = (datetime.utcnow() - __import__("datetime").timedelta(seconds=ttl_seconds)).isoformat()
    with _conn() as cx:
        rows = cx.execute(
            "SELECT * FROM game_logs WHERE player_id = ? AND season = ?",
            (player_id, season),
        ).fetchall()
    if not rows:
        return pd.DataFrame(), False
    is_fresh = all(r["fetched_at"] >= cutoff for r in rows)
    df = pd.DataFrame([dict(r) for r in rows])
    df.columns = [c.upper() if c != "fetched_at" else c for c in df.columns]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df = df.sort_values("GAME_DATE", ascending=False).reset_index(drop=True)
    return df, is_fresh


# ── Rosters ──────────────────────────────────────────────────────────────────

def upsert_roster(team_id: int, season: str, players: list[dict]) -> None:
    now = datetime.utcnow().isoformat()
    with _conn() as cx:
        cx.execute(
            "DELETE FROM rosters WHERE team_id = ? AND season = ?",
            (team_id, season),
        )
        cx.executemany(
            """INSERT OR REPLACE INTO rosters
               (team_id, season, player_id, player_name, position, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [(team_id, season, p["player_id"], p["player_name"], p.get("position"), now)
             for p in players],
        )


def get_roster(team_id: int, season: str, ttl_seconds: int = 86400):
    """Returns (players_list, is_fresh)."""
    cutoff = (datetime.utcnow() - __import__("datetime").timedelta(seconds=ttl_seconds)).isoformat()
    with _conn() as cx:
        rows = cx.execute(
            "SELECT player_id, player_name, position, fetched_at FROM rosters WHERE team_id = ? AND season = ?",
            (team_id, season),
        ).fetchall()
    if not rows:
        return [], False
    is_fresh = all(r["fetched_at"] >= cutoff for r in rows)
    players = [{"player_id": r["player_id"], "player_name": r["player_name"], "position": r["position"]}
               for r in rows]
    return players, is_fresh


# ── Defense ratings ──────────────────────────────────────────────────────────

def upsert_def_ratings(season: str, df) -> None:
    now = datetime.utcnow().isoformat()
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO team_defense_ratings
               (team_id, season, team_abbr, def_rating, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            [(int(row["TEAM_ID"]), season, row.get("TEAM_ABBR"), row.get("DEF_RATING"), now)
             for _, row in df.iterrows()],
        )


def get_def_ratings(season: str, ttl_seconds: int = 86400):
    """Returns (df, is_fresh)."""
    import pandas as pd
    cutoff = (datetime.utcnow() - __import__("datetime").timedelta(seconds=ttl_seconds)).isoformat()
    with _conn() as cx:
        rows = cx.execute(
            "SELECT team_id, team_abbr, def_rating, fetched_at FROM team_defense_ratings WHERE season = ?",
            (season,),
        ).fetchall()
    if not rows:
        return pd.DataFrame(), False
    # Use any() — stale rows from non-playoff teams shouldn't force a re-fetch of fresh data.
    is_fresh = any(r["fetched_at"] >= cutoff for r in rows)
    df = pd.DataFrame([dict(r) for r in rows])
    df.columns = ["TEAM_ID", "TEAM_ABBR", "DEF_RATING", "fetched_at"]
    return df, is_fresh


# ── Game lines ───────────────────────────────────────────────────────────────

def upsert_game_lines(game_date: str, lines: dict[str, dict]) -> None:
    now = datetime.utcnow().isoformat()
    rows = [
        {
            "date": game_date,
            "team_abbr": abbr,
            "spread": v.get("spread"),
            "total": v.get("total"),
            "is_home": 1 if v.get("is_home") else 0,
            "fetched_at": now,
        }
        for abbr, v in lines.items()
    ]
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO game_lines
               (date, team_abbr, spread, total, is_home, fetched_at)
               VALUES (:date, :team_abbr, :spread, :total, :is_home, :fetched_at)""",
            rows,
        )
    print(f"[db] upserted game lines for {len(rows)} teams on {game_date}")


def get_game_lines(game_date: str) -> dict[str, dict]:
    with _conn() as cx:
        rows = cx.execute(
            "SELECT team_abbr, spread, total, is_home FROM game_lines WHERE date = ?",
            (game_date,),
        ).fetchall()
    return {
        r["team_abbr"]: {
            "spread": r["spread"],
            "total": r["total"],
            "is_home": bool(r["is_home"]),
        }
        for r in rows
    }


def get_latest_game_lines() -> dict[str, dict]:
    with _conn() as cx:
        row = cx.execute(
            "SELECT date FROM game_lines ORDER BY fetched_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {}
        rows = cx.execute(
            "SELECT team_abbr, spread, total, is_home FROM game_lines WHERE date = ?",
            (row["date"],),
        ).fetchall()
    return {
        r["team_abbr"]: {
            "spread": r["spread"],
            "total": r["total"],
            "is_home": bool(r["is_home"]),
        }
        for r in rows
    }


def upsert_fd_projections(game_date: str, projections: dict[int, dict]) -> None:
    now = datetime.utcnow().isoformat()
    rows = [
        {
            "date": game_date,
            "player_id": pid,
            "pts": p.get("pts"),
            "reb": p.get("reb"),
            "ast": p.get("ast"),
            "pra": p.get("pra"),
            "min": p.get("min"),
            "fetched_at": now,
        }
        for pid, p in projections.items()
    ]
    with _conn() as cx:
        cx.executemany(
            """INSERT OR REPLACE INTO fd_projections
               (date, player_id, pts, reb, ast, pra, min, fetched_at)
               VALUES (:date, :player_id, :pts, :reb, :ast, :pra, :min, :fetched_at)""",
            rows,
        )
    print(f"[db] upserted {len(rows)} FanDuel projections for {game_date}")


def get_fd_projections(game_date: str) -> dict[int, dict]:
    with _conn() as cx:
        rows = cx.execute(
            "SELECT player_id, pts, reb, ast, pra, min FROM fd_projections WHERE date = ?",
            (game_date,),
        ).fetchall()
    return {
        r["player_id"]: {
            "pts": r["pts"], "reb": r["reb"], "ast": r["ast"],
            "pra": r["pra"], "min": r["min"],
        }
        for r in rows
    }


def upsert_injuries(injuries: dict[str, dict]) -> None:
    now = datetime.utcnow().isoformat()
    rows = [
        {"player_name": name, "status": v.get("status", ""),
         "comment": v.get("comment", ""), "fetched_at": now}
        for name, v in injuries.items()
    ]
    with _conn() as cx:
        cx.execute("DELETE FROM injuries")
        cx.executemany(
            """INSERT INTO injuries (player_name, status, comment, fetched_at)
               VALUES (:player_name, :status, :comment, :fetched_at)""",
            rows,
        )
    print(f"[db] upserted {len(rows)} injury entries")


def get_injuries() -> dict[str, dict]:
    with _conn() as cx:
        rows = cx.execute(
            "SELECT player_name, status, comment FROM injuries"
        ).fetchall()
    return {r["player_name"]: {"status": r["status"], "comment": r["comment"]} for r in rows}


def get_known_game_dates() -> list[str]:
    """Returns all distinct game dates stored in the schedule table."""
    with _conn() as cx:
        rows = cx.execute("SELECT DISTINCT game_date FROM schedule ORDER BY game_date").fetchall()
    return [r["game_date"] for r in rows]


def get_last_updated(game_date: str) -> str | None:
    """Returns ISO UTC timestamp of the most recent data load for the given date, or None."""
    with _conn() as cx:
        row = cx.execute("""
            SELECT MAX(ts) FROM (
                SELECT MAX(fetched_at) AS ts FROM de_projections WHERE date = ?
                UNION ALL
                SELECT MAX(fetched_at) AS ts FROM fd_projections WHERE date = ?
                UNION ALL
                SELECT MAX(fetched_at) AS ts FROM odds_per_game WHERE date = ?
            )
        """, (game_date, game_date, game_date)).fetchone()
    return row[0] if row and row[0] else None

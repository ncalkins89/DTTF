#!/usr/bin/env python3
"""
backfill_history.py — fetch historical game logs for backtesting.

Fetches Playoffs + Regular Season logs for players who appeared in the
given historical seasons' playoffs. Uses diskcache (30-day TTL), so
re-running is free after the first pass.

Estimated runtime: ~15 min per season (0.7s delay × ~2 calls × ~650 players).
Already-cached players are skipped instantly.

Usage:
    python3 scripts/backfill_history.py                    # 2024-25 only
    python3 scripts/backfill_history.py --seasons 2024-25 2023-24 2022-23
    python3 scripts/backfill_history.py --dry-run          # show what would be fetched
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from nba_api.stats.endpoints import LeagueGameFinder, PlayerGameLog
from nba_api.stats.static import players as nba_players, teams as nba_teams
import pandas as pd

from src.data_fetcher import CACHE, REQUEST_DELAY
from src.db import init_db, upsert_game_logs

SEASONS_DEFAULT = ["2024-25"]


def get_playoff_player_ids(season: str) -> dict[int, str]:
    """
    Returns {player_id: player_name} for all players on rosters of teams
    that appeared in the given season's playoffs.
    Cached 30 days.
    """
    cache_key = f"backfill_playoff_pids_{season}"
    cached = CACHE.get(cache_key)
    if cached is not None:
        return cached

    print(f"  Finding playoff teams for {season} via LeagueGameFinder ...", flush=True)
    time.sleep(REQUEST_DELAY)
    try:
        finder = LeagueGameFinder(
            season_nullable=season,
            season_type_nullable="Playoffs",
            league_id_nullable="00",
        )
        df = finder.league_game_finder_results.get_data_frame()
    except Exception as e:
        print(f"  LeagueGameFinder failed: {e}")
        return {}

    if df.empty:
        return {}

    playoff_team_ids = df["TEAM_ID"].unique().tolist()
    print(f"  {len(playoff_team_ids)} playoff teams — fetching rosters ...", flush=True)

    from nba_api.stats.endpoints import CommonTeamRoster
    id_to_name = {p["id"]: p["full_name"] for p in nba_players.get_players()}
    result = {}
    for team_id in playoff_team_ids:
        time.sleep(REQUEST_DELAY)
        try:
            roster_df = CommonTeamRoster(team_id=team_id, season=season).common_team_roster.get_data_frame()
            for _, row in roster_df.iterrows():
                pid = int(row["PLAYER_ID"])
                result[pid] = id_to_name.get(pid, row.get("PLAYER", str(pid)))
        except Exception as e:
            print(f"  Roster fetch failed for team {team_id}: {e}")

    CACHE.set(cache_key, result, expire=86400 * 30)
    print(f"  Found {len(result)} players across {len(playoff_team_ids)} playoff teams.")
    return result


def fetch_logs_for_player(player_id: int, player_name: str, season: str) -> int:
    """
    Fetch and store Playoffs + Regular Season logs for one player/season.
    Returns number of games stored (0 if already cached).
    """
    cache_key = f"backfill_logs_{player_id}_{season}"
    if CACHE.get(cache_key):
        return 0  # already done

    rows = []
    for season_type in ("Playoffs", "Regular Season"):
        time.sleep(REQUEST_DELAY)
        try:
            logs = PlayerGameLog(
                player_id=player_id,
                season=season,
                season_type_all_star=season_type,
                timeout=15,
            ).player_game_log.get_data_frame()
        except Exception as e:
            print(f"    [{season_type}] fetch failed: {e}", flush=True)
            continue

        if logs.empty:
            continue

        logs = logs.copy()
        logs["SEASON_TYPE"] = season_type
        logs["GAME_DATE"] = pd.to_datetime(logs["GAME_DATE"], format="mixed")
        for col in ["PTS", "REB", "AST"]:
            if col in logs.columns:
                logs[col] = pd.to_numeric(logs[col], errors="coerce").fillna(0)
        logs["PRA"] = logs["PTS"] + logs["REB"] + logs["AST"]
        logs["MIN"] = pd.to_numeric(
            logs["MIN"].astype(str).str.split(":").str[0], errors="coerce"
        ).fillna(0)
        logs = logs[logs["MIN"] >= 5]
        cols = ["GAME_DATE", "MATCHUP", "WL", "PTS", "REB", "AST", "MIN", "SEASON_TYPE", "PRA"]
        available = [c for c in cols if c in logs.columns]
        rows.append(logs[available])

    if not rows:
        CACHE.set(cache_key, True, expire=86400 * 30)
        return 0

    combined = pd.concat(rows, ignore_index=True).sort_values("GAME_DATE", ascending=False)
    combined = combined.reset_index(drop=True)
    upsert_game_logs(player_id, season, combined)
    CACHE.set(cache_key, True, expire=86400 * 30)
    return len(combined)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical game logs.")
    parser.add_argument("--seasons", nargs="+", default=SEASONS_DEFAULT,
                        metavar="SEASON", help="Seasons to backfill (e.g. 2024-25 2023-24)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fetched without hitting the API")
    args = parser.parse_args()

    init_db()

    for season in args.seasons:
        print(f"\n=== Backfilling {season} ===")
        players = get_playoff_player_ids(season)
        if not players:
            print("  No playoff players found — skipping.")
            continue

        total = len(players)
        done = 0
        games_stored = 0

        for i, (pid, name) in enumerate(sorted(players.items(), key=lambda x: x[1])):
            cache_key = f"backfill_logs_{pid}_{season}"
            already_cached = bool(CACHE.get(cache_key))

            if args.dry_run:
                status = "cached" if already_cached else "needs fetch"
                print(f"  [{i+1}/{total}] {name} — {status}")
                continue

            if already_cached:
                done += 1
                continue

            n = fetch_logs_for_player(pid, name, season)
            done += 1
            games_stored += n
            print(f"  [{done}/{total}] {name}: {n} games stored", flush=True)

        if not args.dry_run:
            print(f"\n  {season} complete — {done} players processed, {games_stored} game rows stored.")

    print("\n✓ Backfill done.")


if __name__ == "__main__":
    main()

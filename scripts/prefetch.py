"""
Cache Prefetch Script
---------------------
Warms the diskcache with all data needed for today's dashboard.
Run this once before opening the dashboard — it takes ~5-10 minutes
but afterward the dashboard loads instantly.

Also accepts --backtest to prefetch historical playoff data for the
backtest script (takes 30-60 minutes, runs in the background).

Usage:
    cd /Users/nathancalkins/Code/dttf
    python3 scripts/prefetch.py            # today's data only
    python3 scripts/prefetch.py --backtest # + historical seasons
"""

import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_fetcher import (
    get_active_roster,
    get_player_game_logs,
    get_player_game_logs_season,
    get_series_standings,
    get_team_defense_ratings,
    get_todays_games,
)

BACKTEST_SEASONS = ["2021-22", "2022-23", "2023-24"]


def prefetch_today():
    print(f"── Prefetching today's data ({date.today()}) ──")

    print("  Fetching today's games...", end=" ", flush=True)
    games = get_todays_games()
    print(f"{len(games)} games found")
    if not games:
        print("  No games today. Nothing to prefetch.")
        return

    print("  Fetching defense ratings...", end=" ", flush=True)
    get_team_defense_ratings()
    print("done")

    print("  Fetching series standings...", end=" ", flush=True)
    get_series_standings()
    print("done")

    all_team_ids = set()
    for game in games:
        all_team_ids.add(game["home_team_id"])
        all_team_ids.add(game["away_team_id"])

    all_players = []
    print(f"  Fetching rosters for {len(all_team_ids)} teams...", end=" ", flush=True)
    for team_id in all_team_ids:
        roster = get_active_roster(team_id)
        all_players.extend(roster)
    print(f"{len(all_players)} players total")

    print(f"  Fetching game logs for {len(all_players)} players (this is the slow part)...")
    for i, player in enumerate(all_players, 1):
        pid = player["player_id"]
        name = player["player_name"]
        print(f"    [{i}/{len(all_players)}] {name}...", end=" ", flush=True)
        try:
            logs = get_player_game_logs(pid)
            print(f"{len(logs)} games cached")
        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(2)

    print("\n  Today's data prefetch complete. Dashboard will load instantly.")


def prefetch_backtest():
    print("\n── Prefetching historical backtest data ──")
    print("  This will take 30-60 minutes. You can leave it running.\n")

    from nba_api.stats.endpoints import LeagueGameLog

    for season in BACKTEST_SEASONS:
        print(f"  Season {season}:")

        print(f"    Getting player IDs...", end=" ", flush=True)
        time.sleep(0.7)
        try:
            logs = LeagueGameLog(
                season=season,
                season_type_all_star="Playoffs",
                player_or_team_abbreviation="P",
            ).league_game_log.get_data_frame()
            player_ids = logs["PLAYER_ID"].astype(int).unique().tolist()
            print(f"{len(player_ids)} players")
        except Exception as e:
            print(f"ERROR: {e}")
            continue

        print(f"    Fetching playoff game logs for {len(player_ids)} players...")
        for i, pid in enumerate(player_ids, 1):
            if i % 20 == 0:
                print(f"      ...{i}/{len(player_ids)}")
            try:
                get_player_game_logs_season(pid, season, "Playoffs")
                # Also grab prior regular season for context
                prior_season = _prior_season(season)
                get_player_game_logs_season(pid, prior_season, "Regular Season")
            except Exception as e:
                print(f"      [warn] player {pid}: {e}")
                time.sleep(2)

        print(f"    {season} done.\n")

    print("Backtest prefetch complete. Run scripts/backtest.py whenever you're ready.")


def _prior_season(season: str) -> str:
    """'2022-23' → '2021-22'"""
    start = int(season.split("-")[0])
    return f"{start - 1}-{str(start)[-2:]}"


if __name__ == "__main__":
    prefetch_today()
    if "--backtest" in sys.argv:
        prefetch_backtest()

#!/usr/bin/env python3
"""
update_db.py — bring dttf.db fully up to date.

Idempotent: all writes use upserts, safe to run multiple times.
Incremental: skips game logs and def ratings already fresh in DB (6h / 24h TTL).

Usage:
    python3 scripts/update_db.py              # today's date, full update
    python3 scripts/update_db.py --date 2026-04-19
    python3 scripts/update_db.py --skip-logs  # skip slow per-player log fetch
"""
import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from src.db import (
    init_db,
    upsert_schedule,
    upsert_odds,
    upsert_game_lines,
    upsert_series_standings,
    upsert_de_projections,
    upsert_fd_projections,
    upsert_injuries,
    get_game_logs,
    get_def_ratings,
)


def _step(n: int, label: str) -> None:
    print(f"\n[{n}] {label} ...", flush=True)


# All fetch functions use diskcache internally. Delete the relevant key first
# so the update script always gets live data, not a cached response.
def _bust(key: str) -> None:
    from src.data_fetcher import CACHE
    CACHE.delete(key)


def update_schedule(game_date: str) -> list[dict]:
    _step(1, f"Schedule — {game_date}")
    from src.data_fetcher import CURRENT_SEASON, get_todays_games
    _bust(f"todays_games_{game_date}")
    games = get_todays_games(game_date)
    if not games:
        print("  No games found for this date.")
        return []
    upsert_schedule(games)
    for g in games:
        print(f"  {g['home_team_abbr']} vs {g['away_team_abbr']}")
    return games


def update_odds(game_date: str) -> None:
    _step(2, "Odds + game lines (The-Odds-API)")
    import os
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        print("  ODDS_API_KEY not set — skipping.")
        return
    _bust("odds_api_all_markets")
    from src.odds import _fetch_all_markets
    win_probs, game_lines = _fetch_all_markets(api_key)
    if not win_probs:
        print("  No odds returned (games may be live or completed).")
        return
    upsert_odds(game_date, win_probs)
    upsert_game_lines(game_date, game_lines)
    for abbr in sorted(win_probs):
        prob = win_probs[abbr]
        line = game_lines.get(abbr, {})
        parts = [f"{prob:.1%}"]
        if line.get("spread") is not None:
            parts.append(f"spread={line['spread']:+.1f}")
        if line.get("total") is not None:
            parts.append(f"O/U={line['total']}")
        print(f"  {abbr}: {' '.join(parts)}")


def update_series_standings(season: str) -> None:
    _step(3, "Series standings")
    _bust(f"series_standings_{season}")
    from src.data_fetcher import get_series_standings
    from nba_api.stats.static import teams as nba_teams
    team_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
    standings = get_series_standings(season)
    if not standings:
        print("  No standings found.")
        return
    upsert_series_standings(season, standings)
    for s in standings:
        h = team_map.get(s["home_team_id"], str(s["home_team_id"]))
        a = team_map.get(s["away_team_id"], str(s["away_team_id"]))
        print(f"  {h} {s['home_wins']}-{s['away_wins']} {a}")


def update_fd_projections(game_date: str) -> None:
    _step(4, "FanDuel projections")
    _bust("fanduel_projections")
    from src.external import fetch_fanduel_projections
    from nba_api.stats.static import players as nba_players
    projs = fetch_fanduel_projections()
    if not projs:
        print("  No projections found (no slate today?).")
        return
    upsert_fd_projections(game_date, projs)
    id_to_name = {p["id"]: p["full_name"] for p in nba_players.get_players()}
    for pid, p in sorted(projs.items(), key=lambda x: -(x[1]["pra"] or 0))[:10]:
        print(f"  {id_to_name.get(pid, pid)}: PRA={p['pra']}")
    if len(projs) > 10:
        print(f"  ... and {len(projs) - 10} more")


def update_injuries() -> None:
    _step(5, "Injuries (ESPN)")
    _bust("espn_injuries")
    from src.external import fetch_injuries
    injuries = fetch_injuries()
    if not injuries:
        print("  No injury data returned.")
        return
    upsert_injuries(injuries)
    out = [n for n, v in injuries.items() if v.get("status") == "Out"]
    dtd = [n for n, v in injuries.items() if v.get("status") == "Day-To-Day"]
    print(f"  {len(out)} Out, {len(dtd)} Day-To-Day ({len(injuries)} total)")


def update_de_projections(game_date: str) -> None:
    _step(6, "DraftEdge projections")
    _bust("draftedge_projections")
    from src.external import fetch_draftedge_projections
    from nba_api.stats.static import players as nba_players
    projs = fetch_draftedge_projections()
    if not projs:
        print("  No projections found.")
        return
    upsert_de_projections(game_date, projs)
    id_to_name = {p["id"]: p["full_name"] for p in nba_players.get_players()}
    for pid, p in sorted(projs.items(), key=lambda x: -x[1]["pra"])[:10]:
        print(f"  {id_to_name.get(pid, pid)}: PRA={p['pra']}")
    if len(projs) > 10:
        print(f"  ... and {len(projs) - 10} more")


def update_def_ratings(season: str) -> None:
    _step(7, "Defense ratings")
    df, is_fresh = get_def_ratings(season)
    if is_fresh:
        print("  Already fresh (< 24h) — skipping.")
        return
    from src.data_fetcher import get_team_defense_ratings
    ratings = get_team_defense_ratings(season)
    n = len(ratings) if hasattr(ratings, "__len__") else "?"
    print(f"  Updated {n} teams.")


def _get_playoff_team_ids(season: str) -> list[int]:
    """All team IDs currently in the playoff bracket."""
    from src.db import get_series_standings as db_get_standings
    standings = db_get_standings(season)
    ids = set()
    for s in standings:
        ids.add(s["home_team_id"])
        ids.add(s["away_team_id"])
    return sorted(ids)


def update_game_logs(games: list[dict], season: str) -> None:
    _step(8, "Game logs — all playoff teams (skips players fresh < 6h)")
    from src.data_fetcher import get_active_roster, get_player_game_logs

    # Cover all 16 playoff teams, not just today's matchups.
    playoff_ids = _get_playoff_team_ids(season)
    today_ids = {tid for g in games for tid in [g["home_team_id"], g["away_team_id"]]}
    all_team_ids = sorted(set(playoff_ids) | today_ids)
    if not all_team_ids:
        print("  No teams found — skipping.")
        return

    seen: set[int] = set()
    total = fetched = skipped = 0

    for team_id in all_team_ids:
        for player in get_active_roster(team_id):
            pid = player["player_id"]
            if pid in seen:
                continue
            seen.add(pid)
            total += 1

            _, is_fresh = get_game_logs(pid, season)
            if is_fresh:
                skipped += 1
                continue

            logs = get_player_game_logs(pid, season, allow_api_fetch=True)
            fetched += 1
            print(f"  [{fetched}] {player['player_name']}: {len(logs)} games", flush=True)

    print(f"  Done — {fetched} fetched, {skipped} skipped (fresh), {total} total.")


def update_prior_season_logs(prior_season: str) -> None:
    """Fetch prior season game logs once (30-day TTL — season is over, data never changes)."""
    _step(9, f"Prior season logs ({prior_season}) — skips players fresh < 30d")
    from src.db import get_game_logs
    from src.data_fetcher import CURRENT_SEASON, get_active_roster, get_player_game_logs

    playoff_ids = _get_playoff_team_ids(CURRENT_SEASON)
    if not playoff_ids:
        print("  No playoff teams found — skipping.")
        return

    seen: set[int] = set()
    fetched = skipped = 0
    for team_id in playoff_ids:
        for player in get_active_roster(team_id):
            pid = player["player_id"]
            if pid in seen:
                continue
            seen.add(pid)
            _, is_fresh = get_game_logs(pid, prior_season, ttl_seconds=30 * 86400)
            if is_fresh:
                skipped += 1
                continue
            logs = get_player_game_logs(pid, prior_season, allow_api_fetch=True, ttl_seconds=30 * 86400)
            fetched += 1
            print(f"  [{fetched}] {player['player_name']}: {len(logs)} prior-season games", flush=True)

    print(f"  Done — {fetched} fetched, {skipped} skipped (fresh), {len(seen)} total.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bring dttf.db up to date.")
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Game date to load (YYYY-MM-DD, default: today)")
    parser.add_argument("--skip-logs", action="store_true",
                        help="Skip per-player game log fetch (faster, ~10s vs ~5min)")
    args = parser.parse_args()

    from src.data_fetcher import CURRENT_SEASON, PRIOR_SEASON

    print(f"=== DTTF update_db — {args.date} ===")
    init_db()

    games = update_schedule(args.date)
    update_odds(args.date)
    update_series_standings(CURRENT_SEASON)
    update_fd_projections(args.date)
    update_injuries()
    update_de_projections(args.date)
    update_def_ratings(CURRENT_SEASON)

    if args.skip_logs:
        print("\n[8] Game logs — skipped (--skip-logs)")
        print("\n[9] Prior season logs — skipped (--skip-logs)")
    elif not games:
        print("\n[8] Game logs — skipped (no games for this date)")
        update_prior_season_logs(PRIOR_SEASON)
    else:
        update_game_logs(games, CURRENT_SEASON)
        update_prior_season_logs(PRIOR_SEASON)

    print("\n✓ Done. Open the dashboard: python3 src/dashboard.py")


if __name__ == "__main__":
    main()

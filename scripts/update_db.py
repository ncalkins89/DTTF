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
    upsert_model_projections,
    get_game_logs,
    get_def_ratings,
    get_de_projections,
    get_fd_projections,
)


def _step(n: int, label: str) -> None:
    print(f"\n[{n}] {label} ...", flush=True)


def update_schedule(game_date: str) -> list[dict]:
    _step(1, f"Schedule — {game_date}")
    from src.data_fetcher import CURRENT_SEASON, get_todays_games
    games = get_todays_games(game_date)
    if not games:
        print("  No games found for this date.")
        return []
    upsert_schedule(games)
    for g in games:
        print(f"  {g['home_team_abbr']} vs {g['away_team_abbr']}")
    return games


def update_odds(game_date: str) -> None:
    _step(2, "Odds + game lines (The-Odds-API) — DISABLED")
    # Disabled: free tier quota (500 req/mo) exhausted. Per-game win probs and
    # spread/O/U data fall back to stale DB values. See docs/disabled_data.md.
    print("  Skipped (disabled — see docs/disabled_data.md)")


def update_series_standings(season: str) -> None:
    _step(3, "Series standings")
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


def update_series_odds() -> None:
    _step(4, "Series odds (DraftKings via ScraperAPI)")
    import sqlite3
    from datetime import datetime, timedelta
    from src.db import DB_PATH
    try:
        with sqlite3.connect(DB_PATH) as cx:
            row = cx.execute("SELECT MIN(fetched_at) FROM series_odds").fetchone()
        last = row[0] if row and row[0] else None
        if last:
            age = datetime.utcnow() - datetime.fromisoformat(last)
            if age < timedelta(hours=4):
                print(f"  Skipping — last fetched {int(age.total_seconds() / 60)}m ago (TTL 4h)")
                return
    except Exception:
        pass
    from src.series_odds import fetch_series_win_probs
    result = fetch_series_win_probs(force_refresh=True)
    if not result:
        print("  No series odds returned.")
    else:
        print(f"  {len(result) // 2} series loaded.")


def update_fd_projections(game_date: str) -> None:
    _step(5, "FanDuel projections")
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
    _step(6, "Injuries (ESPN)")
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
    _step(7, "DraftEdge projections")
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
    _step(8, "Defense ratings")
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
    _step(9, "Game logs — all playoff teams (skips players fresh < 6h)")
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
    _step(10, f"Prior season logs ({prior_season}) — skips players fresh < 30d")
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


def update_model_projections_snapshot(game_date: str, games: list[dict], season: str) -> None:
    """
    Snapshot every player's projections for today into model_projections.
    Idempotent — INSERT OR REPLACE means re-running just overwrites.
    Runs after game logs, DE, and FD are all loaded so projections are complete.
    """
    _step(11, f"Model projections snapshot — {game_date}")
    if not games:
        print("  No games — skipping.")
        return

    from src.data_fetcher import (
        get_active_roster, get_player_game_logs_365,
        get_team_defense_ratings, get_series_standings,
    )
    from src.projections import project_player
    from src.db import (
        get_series_standings as db_get_series_standings,
        get_latest_odds as db_get_latest_odds,
    )
    from src.odds import fetch_series_win_probs, get_series_record_for_team
    from nba_api.stats.static import teams as nba_teams

    TEAM_MAP = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}

    def_ratings = get_team_defense_ratings()
    series_standings = db_get_series_standings(season) or get_series_standings(season)
    per_game_probs = db_get_latest_odds()
    series_win_probs = fetch_series_win_probs(series_standings, per_game_probs)
    de_projs = get_de_projections(game_date)
    fd_projs = get_fd_projections(game_date)

    rows = []
    seen: set[int] = set()
    for game in games:
        for team_id, opp_team_id, team_abbr, opp_abbr in [
            (game["home_team_id"], game["away_team_id"],
             game["home_team_abbr"], game["away_team_abbr"]),
            (game["away_team_id"], game["home_team_id"],
             game["away_team_abbr"], game["home_team_abbr"]),
        ]:
            series_record = get_series_record_for_team(team_abbr, series_standings, TEAM_MAP)
            per_game_p = per_game_probs.get(team_abbr)
            series_win_prob = series_win_probs.get(team_abbr)

            for player in get_active_roster(team_id):
                pid = player["player_id"]
                if pid in seen:
                    continue
                seen.add(pid)

                logs = get_player_game_logs_365(pid)
                proj = project_player(
                    player_id=pid,
                    opponent_team_id=opp_team_id,
                    game_logs=logs,
                    def_ratings=def_ratings,
                    series_record=series_record,
                    per_game_win_prob=per_game_p,
                    current_round=1,
                )
                our_proj = proj["projected_pra"]
                de = de_projs.get(pid)
                fd = fd_projs.get(pid)
                de_pra = de["pra"] if de else None
                fd_pra = fd["pra"] if fd else None

                pl_avg = None
                if not logs.empty:
                    pl = logs[logs["SEASON_TYPE"] == "Playoffs"]
                    if not pl.empty:
                        pl_avg = round(pl["PRA"].mean(), 1)

                from src.blend import blend as _blend
                pred_blended, _ = _blend(our_proj, de_pra, fd_pra)
                if pred_blended is None:
                    pred_blended = our_proj

                rows.append({
                    "player_id": pid,
                    "player_name": player["player_name"],
                    "team_abbr": team_abbr,
                    "opp_abbr": opp_abbr,
                    "our_proj": our_proj,
                    "pred_blended": pred_blended,
                    "de_proj": de_pra,
                    "fd_proj": fd_pra,
                    "series_win_prob": series_win_prob,
                })

    upsert_model_projections(game_date, rows)
    print(f"  Saved {len(rows)} player projections.")


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
    update_series_odds()
    update_fd_projections(args.date)
    update_injuries()
    update_de_projections(args.date)
    update_def_ratings(CURRENT_SEASON)

    if args.skip_logs:
        print("\n[9] Game logs — skipped (--skip-logs)")
        print("\n[10] Prior season logs — skipped (--skip-logs)")
    elif not games:
        print("\n[9] Game logs — skipped (no games for this date)")
        update_prior_season_logs(PRIOR_SEASON)
    else:
        update_game_logs(games, CURRENT_SEASON)
        update_prior_season_logs(PRIOR_SEASON)

    # Step 11 runs after logs+projections are all loaded — snapshot must be last
    if not args.skip_logs and games:
        update_model_projections_snapshot(args.date, games, CURRENT_SEASON)
    else:
        print("\n[11] Model projections snapshot — skipped (no games or --skip-logs)")

    # Step 12: re-fit blend weights from all available historical data
    if not args.skip_logs:
        _step(12, "Blend weights estimation")
        from scripts.estimate_blend_weights import main as estimate_blend_weights
        estimate_blend_weights()
    else:
        print("\n[12] Blend weights estimation — skipped (--skip-logs)")

    _step(13, "League picks (playoffpicker.com)")
    from scripts.scrape_league_picks import main as scrape_league_picks
    scrape_league_picks()

    print("\n✓ Done. Open the dashboard: python3 src/dashboard.py")


if __name__ == "__main__":
    main()

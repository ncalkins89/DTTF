import time
from datetime import date, datetime
from pathlib import Path

import diskcache
import pandas as pd
from nba_api.stats.endpoints import (
    CommonTeamRoster,
    LeagueDashTeamStats,
    PlayerGameLog,
    ScoreboardV2,
)
from nba_api.stats.static import teams as nba_teams

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"
CACHE = diskcache.Cache(str(CACHE_DIR))

CURRENT_SEASON = "2025-26"
PRIOR_SEASON = "2024-25"
REQUEST_DELAY = 0.5  # seconds between uncached nba_api calls


def _cached(key: str, ttl: int, fetch_fn):
    result = CACHE.get(key)
    if result is None:
        time.sleep(REQUEST_DELAY)
        result = fetch_fn()
        CACHE.set(key, result, expire=ttl)
    return result


def get_todays_games(game_date: str | None = None) -> list[dict]:
    if game_date is None:
        game_date = date.today().strftime("%Y-%m-%d")

    def fetch():
        sb = ScoreboardV2(game_date=game_date)
        header = sb.game_header.get_data_frame()
        if header.empty:
            return []
        team_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
        seen_ids = set()
        games = []
        for _, row in header.iterrows():
            gid = row["GAME_ID"]
            if gid in seen_ids:
                continue
            seen_ids.add(gid)
            games.append({
                "game_id": gid,
                "home_team_id": int(row["HOME_TEAM_ID"]),
                "away_team_id": int(row["VISITOR_TEAM_ID"]),
                "home_team_abbr": team_map.get(int(row["HOME_TEAM_ID"]), ""),
                "away_team_abbr": team_map.get(int(row["VISITOR_TEAM_ID"]), ""),
                "game_date": game_date,
            })
        return games

    return _cached(f"todays_games_{game_date}", 3600, fetch)


def get_active_roster(team_id: int, season: str = CURRENT_SEASON) -> list[dict]:
    from src.db import get_roster, upsert_roster
    players, is_fresh = get_roster(team_id, season)
    if is_fresh:
        return players
    time.sleep(REQUEST_DELAY)
    try:
        roster = CommonTeamRoster(team_id=team_id, season=season)
        df = roster.common_team_roster.get_data_frame()
        players = [
            {"player_id": int(row["PLAYER_ID"]), "player_name": row["PLAYER"], "position": row["POSITION"]}
            for _, row in df.iterrows()
        ]
        upsert_roster(team_id, season, players)
    except Exception as e:
        print(f"[data_fetcher] roster fetch failed for {team_id}: {e}")
        if players:
            return players  # return stale data rather than empty
    return players


def get_player_game_logs(
    player_id: int,
    season: str = CURRENT_SEASON,
    allow_api_fetch: bool = True,
    ttl_seconds: int = 21600,
) -> pd.DataFrame:
    from src.db import get_game_logs, upsert_game_logs
    df, is_fresh = get_game_logs(player_id, season, ttl_seconds)
    # Return existing data if fresh, OR if we can't fetch anyway (stale > empty).
    if not df.empty and (is_fresh or not allow_api_fetch):
        return df
    if not allow_api_fetch:
        return pd.DataFrame()
    try:
        time.sleep(REQUEST_DELAY)
        playoff_logs = PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Playoffs",
            timeout=10,
        ).player_game_log.get_data_frame()
        time.sleep(REQUEST_DELAY)
        regular_logs = PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
            timeout=10,
        ).player_game_log.get_data_frame()

        playoff_logs["SEASON_TYPE"] = "Playoffs"
        regular_logs["SEASON_TYPE"] = "Regular Season"

        combined = pd.concat([playoff_logs, regular_logs], ignore_index=True)
        combined = combined.sort_values("GAME_DATE", ascending=False)

        cols = ["GAME_DATE", "MATCHUP", "WL", "PTS", "REB", "AST", "MIN", "SEASON_TYPE"]
        available = [c for c in cols if c in combined.columns]
        combined = combined[available].copy()

        combined["GAME_DATE"] = pd.to_datetime(combined["GAME_DATE"])
        for col in ["PTS", "REB", "AST"]:
            if col in combined.columns:
                combined[col] = pd.to_numeric(combined[col], errors="coerce").fillna(0)
        combined["PRA"] = combined["PTS"] + combined["REB"] + combined["AST"]
        combined["MIN"] = pd.to_numeric(
            combined["MIN"].astype(str).str.split(":").str[0], errors="coerce"
        ).fillna(0)
        combined = combined[combined["MIN"] >= 5]
        combined = combined.reset_index(drop=True)

        upsert_game_logs(player_id, season, combined)
        return combined
    except Exception as e:
        print(f"[data_fetcher] game logs fetch failed for {player_id}: {e}")
        return df if not df.empty else pd.DataFrame()


def get_player_game_logs_365(player_id: int) -> pd.DataFrame:
    """Full game logs for current + prior season (DB only, no date cap)."""
    import pandas as pd
    from src.db import get_game_logs
    cur, _ = get_game_logs(player_id, CURRENT_SEASON)
    prior, _ = get_game_logs(player_id, PRIOR_SEASON, ttl_seconds=30 * 86400)
    parts = [df for df in [cur, prior] if not df.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts).sort_values("GAME_DATE", ascending=False).reset_index(drop=True)


def get_player_game_logs_season(
    player_id: int,
    season: str,
    season_type: str = "Playoffs",
) -> pd.DataFrame:
    """Used by backtest script to fetch a specific historical season."""
    def fetch():
        logs = PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star=season_type,
        ).player_game_log.get_data_frame()
        if logs.empty:
            return pd.DataFrame()
        logs["SEASON_TYPE"] = season_type
        logs["GAME_DATE"] = pd.to_datetime(logs["GAME_DATE"])
        for col in ["PTS", "REB", "AST"]:
            logs[col] = pd.to_numeric(logs.get(col, 0), errors="coerce").fillna(0)
        logs["PRA"] = logs["PTS"] + logs["REB"] + logs["AST"]
        logs["MIN"] = pd.to_numeric(
            logs["MIN"].astype(str).str.split(":").str[0], errors="coerce"
        ).fillna(0)
        logs = logs[logs["MIN"] >= 5]
        return logs.reset_index(drop=True)

    return _cached(f"game_logs_historical_{player_id}_{season}_{season_type}", 86400 * 30, fetch)


def get_team_defense_ratings(season: str = CURRENT_SEASON) -> pd.DataFrame:
    from src.db import get_def_ratings, upsert_def_ratings
    df, is_fresh = get_def_ratings(season)
    if is_fresh:
        return df
    try:
        time.sleep(REQUEST_DELAY)
        stats = LeagueDashTeamStats(
            season=season,
            measure_type_detailed_defense="Defense",
            season_type_all_star="Playoffs",
        ).league_dash_team_stats.get_data_frame()

        if stats.empty:
            stats = LeagueDashTeamStats(
                season=season,
                measure_type_detailed_defense="Defense",
                season_type_all_star="Regular Season",
            ).league_dash_team_stats.get_data_frame()

        stats = stats[["TEAM_ID", "TEAM_NAME", "DEF_RATING"]].copy()
        stats["TEAM_ID"] = stats["TEAM_ID"].astype(int)
        stats["DEF_RATING"] = pd.to_numeric(stats["DEF_RATING"], errors="coerce")
        abbr_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
        stats["TEAM_ABBR"] = stats["TEAM_ID"].map(abbr_map)
        stats = stats[["TEAM_ID", "TEAM_ABBR", "DEF_RATING"]].reset_index(drop=True)

        upsert_def_ratings(season, stats)
        return stats
    except Exception as e:
        print(f"[data_fetcher] def ratings fetch failed: {e}")
        return df if not df.empty else pd.DataFrame()


def get_series_standings(season: str = CURRENT_SEASON) -> list[dict]:
    """
    Returns playoff series records for all active series.

    Computed from actual game results via LeagueGameFinder — immune to the
    PlayoffPicture bug that returns the previous season's completed series data.
    Falls back to 0-0 records derived from today's schedule when no games have
    been played yet (e.g. opening day).
    """
    from nba_api.stats.endpoints import LeagueGameFinder

    def fetch():
        try:
            time.sleep(REQUEST_DELAY)
            finder = LeagueGameFinder(
                season_nullable=season,
                season_type_nullable="Playoffs",
                league_id_nullable="00",
            )
            games_df = finder.league_game_finder_results.get_data_frame()
        except Exception as e:
            print(f"[series_standings] LeagueGameFinder failed: {e}")
            games_df = pd.DataFrame()

        if games_df.empty:
            print("[series_standings] No playoff games recorded yet; defaulting to 0-0 from today's schedule")
            todays = get_todays_games()
            return [
                {
                    "home_team_id": g["home_team_id"],
                    "away_team_id": g["away_team_id"],
                    "home_wins": 0,
                    "away_wins": 0,
                }
                for g in todays
            ]

        # Each game_id appears twice — once per team. Build per-game records.
        # The NBA API sets WL mid-game, so we must verify today's games are truly
        # final via the live scoreboard before counting them.
        from datetime import datetime
        from zoneinfo import ZoneInfo
        today_str = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")

        # Fetch final game IDs from today's live scoreboard
        final_today: set[str] = set()
        try:
            import requests as _req
            sb = _req.get(
                "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
                timeout=5,
            ).json()
            for g in sb["scoreboard"]["games"]:
                if g["gameStatus"] == 3:  # 3 = Final
                    final_today.add(str(g["gameId"]))
        except Exception as e:
            print(f"[series_standings] scoreboard fetch failed: {e} — today's games excluded")

        game_records: dict[str, dict] = {}
        for _, row in games_df.iterrows():
            game_id = str(row["GAME_ID"])
            game_date_str = str(row.get("GAME_DATE", ""))[:10]
            # Skip today's games unless confirmed Final by live scoreboard
            if game_date_str >= today_str and game_id not in final_today:
                continue
            team_id = int(row["TEAM_ID"])
            wl = str(row.get("WL", ""))
            matchup = str(row.get("MATCHUP", ""))

            if game_id not in game_records:
                game_records[game_id] = {}

            if "vs." in matchup:
                game_records[game_id]["home_team_id"] = team_id
                game_records[game_id]["home_won"] = wl == "W"
            else:
                game_records[game_id]["away_team_id"] = team_id

        # Aggregate per-team wins, keyed by sorted (team_a_id, team_b_id) pair.
        # The "home" team for a series is whoever hosted Game 1 (= higher seed).
        series_wins: dict[tuple, dict[int, int]] = {}
        series_home: dict[tuple, tuple] = {}  # key -> (home_id, away_id) from first game

        for game in game_records.values():
            home_id = game.get("home_team_id")
            away_id = game.get("away_team_id")
            if not home_id or not away_id:
                continue
            key = (min(home_id, away_id), max(home_id, away_id))
            if key not in series_wins:
                series_wins[key] = {home_id: 0, away_id: 0}
                series_home[key] = (home_id, away_id)
            winner = home_id if game.get("home_won") else away_id
            series_wins[key][winner] = series_wins[key].get(winner, 0) + 1

        results = []
        for key, wins in series_wins.items():
            home_id, away_id = series_home[key]
            results.append({
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_wins": wins.get(home_id, 0),
                "away_wins": wins.get(away_id, 0),
            })

        # Ensure every series from today's schedule is represented.
        # LeagueGameFinder only returns completed games, so series whose
        # Game 1 hasn't finished yet (or wasn't recorded yet) would be missing.
        known_pairs = {
            (min(r["home_team_id"], r["away_team_id"]),
             max(r["home_team_id"], r["away_team_id"]))
            for r in results
        }
        for g in get_todays_games():
            pair = (min(g["home_team_id"], g["away_team_id"]),
                    max(g["home_team_id"], g["away_team_id"]))
            if pair not in known_pairs:
                results.append({
                    "home_team_id": g["home_team_id"],
                    "away_team_id": g["away_team_id"],
                    "home_wins": 0,
                    "away_wins": 0,
                })
                known_pairs.add(pair)

        print(f"[series_standings] {len(results)} series (game results + today's schedule)")
        return results

    return _cached(f"series_standings_{season}", 600, fetch)


def clear_cache() -> None:
    CACHE.clear()

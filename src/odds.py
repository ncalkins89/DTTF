import os
import time

import requests

ODDS_API_BASE = "https://api.the-odds-api.com/v4"


def american_to_implied_prob(american_odds: int) -> float:
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def normalize_probs(prob_a: float, prob_b: float) -> tuple[float, float]:
    total = prob_a + prob_b
    if total == 0:
        return 0.5, 0.5
    return prob_a / total, prob_b / total


def _fetch_all_markets(api_key: str) -> tuple[dict[str, float], dict[str, dict]]:
    """
    Single Odds API call fetching h2h + spreads + totals.
    Returns:
      - win_probs:  {team_abbr: per_game_win_probability}
      - game_lines: {team_abbr: {"spread": float, "total": float, "is_home": bool}}
        spread = points home team is favored by (negative = underdog)
        total  = O/U line for the game
    """
    try:
        resp = requests.get(
            f"{ODDS_API_BASE}/sports/basketball_nba/odds/",
            params={
                "apiKey": api_key,
                "regions": "us",
                "markets": "h2h,spreads,totals",
                "oddsFormat": "american",
            },
            timeout=10,
        )
        remaining = resp.headers.get("x-requests-remaining", "?")
        print(f"[odds-api] requests remaining this month: {remaining}")
        resp.raise_for_status()
        games = resp.json()
    except Exception as e:
        print(f"[odds-api] fetch failed: {e}")
        return {}, {}

    from nba_api.stats.static import teams as nba_teams
    name_to_abbr = {}
    for t in nba_teams.get_teams():
        name_to_abbr[t["full_name"].lower()] = t["abbreviation"]
        name_to_abbr[t["nickname"].lower()] = t["abbreviation"]

    win_probs: dict[str, float] = {}
    game_lines: dict[str, dict] = {}

    for game in games:
        home_name = game.get("home_team", "").lower()
        away_name = game.get("away_team", "").lower()
        home_abbr = name_to_abbr.get(home_name, "")
        away_abbr = name_to_abbr.get(away_name, "")
        if not home_abbr or not away_abbr:
            continue

        home_probs, away_probs = [], []
        home_spreads, totals = [], []

        for bookmaker in game.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                key = market["key"]
                outcomes = market.get("outcomes", [])

                if key == "h2h":
                    probs = {o["name"].lower(): american_to_implied_prob(o["price"])
                             for o in outcomes}
                    h = probs.get(home_name)
                    a = probs.get(away_name)
                    if h and a:
                        home_probs.append(h)
                        away_probs.append(a)

                elif key == "spreads":
                    for o in outcomes:
                        if o["name"].lower() == home_name and "point" in o:
                            home_spreads.append(o["point"])

                elif key == "totals":
                    for o in outcomes:
                        if o["name"] == "Over" and "point" in o:
                            totals.append(o["point"])

        if home_probs:
            avg_home = sum(home_probs) / len(home_probs)
            avg_away = sum(away_probs) / len(away_probs)
            norm_home, norm_away = normalize_probs(avg_home, avg_away)
            win_probs[home_abbr] = round(norm_home, 3)
            win_probs[away_abbr] = round(norm_away, 3)

        avg_spread = round(sum(home_spreads) / len(home_spreads), 1) if home_spreads else None
        avg_total = round(sum(totals) / len(totals), 1) if totals else None

        # home spread is positive when home is underdog, negative when favored
        # away spread is the inverse
        for abbr, is_home in [(home_abbr, True), (away_abbr, False)]:
            spread = avg_spread if is_home else (-avg_spread if avg_spread is not None else None)
            game_lines[abbr] = {
                "spread": spread,
                "total": avg_total,
                "is_home": is_home,
            }

    return win_probs, game_lines


def fetch_game_lines() -> dict[str, dict]:
    """Returns {team_abbr: {"spread", "total", "is_home"}} for tonight's games."""
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        return {}
    _, lines = _fetch_all_markets(api_key)
    return lines


def _fetch_odds_api_game_probs(api_key: str) -> dict[str, float]:
    win_probs, _ = _fetch_all_markets(api_key)
    return win_probs


def fetch_per_game_win_probs(game_date: str | None = None) -> dict[str, float]:
    """
    Returns {team_abbr: per_game_win_probability} for teams playing today.

    Primary: DB (odds_per_game table, populated by update_db.py).
    Fallback: live Odds API fetch when ODDS_API_KEY is set.
    """
    from datetime import date as _date
    from src.db import get_odds as db_get_odds
    today = game_date or str(_date.today())
    db_odds = db_get_odds(today)
    if db_odds:
        return db_odds
    api_key = os.environ.get("ODDS_API_KEY", "")
    if not api_key:
        return {}
    return _fetch_odds_api_game_probs(api_key)


_RAW_BLEND = 0.65  # blend factor: shrink per-game edge toward 50/50 for Markov


def fetch_series_win_probs(
    series_standings: list[dict] | None = None,
    per_game_probs: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Returns {team_abbr: series_win_probability} for all active playoff series.

    Primary: DraftKings REST API (live market prices, 10-min cache).
    Fallback: Markov chain over per-game h2h odds when DK is unavailable.
    """
    # Primary: DraftKings series winner market via REST API (ScraperAPI → DK)
    dk_result = {}
    try:
        from src.series_odds import fetch_series_win_probs as dk_fetch
        dk_data = dk_fetch()
        if dk_data:
            dk_result = {abbr: v["series_win_prob"] for abbr, v in dk_data.items()}
            print(f"[odds] series win probs via: DraftKings API ({len(dk_result) // 2} series)")
    except Exception as e:
        print(f"[odds] DraftKings live fetch failed: {e}")

    # Fill in any teams missing from live fetch using DB (e.g. DK pulls lines during live games)
    from src.db import get_series_odds as db_get_series_odds
    db_data = db_get_series_odds()
    if db_data:
        filled = [abbr for abbr, v in db_data.items() if abbr not in dk_result]
        for abbr, v in db_data.items():
            if abbr not in dk_result:
                dk_result[abbr] = v["series_win_prob"]
        if filled:
            print(f"[odds] filled {len(filled)} teams from DB (missing from live DK): {filled}")

    # Fallback: Markov chain
    from nba_api.stats.static import teams as nba_teams
    from src.projections import compute_series_win_probability

    if series_standings is None:
        from src.data_fetcher import get_series_standings
        series_standings = get_series_standings()

    if per_game_probs is None:
        per_game_probs = fetch_per_game_win_probs()

    if dk_result:
        team_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
        for series in series_standings:
            home_abbr = team_map.get(series["home_team_id"], "")
            away_abbr = team_map.get(series["away_team_id"], "")
            if home_abbr and home_abbr not in dk_result:
                print(f"[odds] WARNING: {home_abbr} not on DK board — series win prob will be null")
            if away_abbr and away_abbr not in dk_result:
                print(f"[odds] WARNING: {away_abbr} not on DK board — series win prob will be null")
        print(f"[odds] series win probs via: DraftKings API")

    return dk_result


def _markov_series_win_probs(
    series_standings: list[dict],
    per_game_probs: dict[str, float],
) -> dict[str, float]:
    """Markov chain series win probabilities from per-game odds. Not used by default."""
    from nba_api.stats.static import teams as nba_teams
    from src.projections import compute_series_win_probability
    team_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
    result = {}
    for series in series_standings:
        home_abbr = team_map.get(series["home_team_id"], "")
        away_abbr = team_map.get(series["away_team_id"], "")
        home_w = series["home_wins"]
        away_w = series["away_wins"]
        raw_p = per_game_probs.get(home_abbr, 0.5)
        per_game_p = 0.5 + (raw_p - 0.5) * _RAW_BLEND
        home_prob = compute_series_win_probability(home_w, away_w, per_game_p)
        if home_abbr:
            result[home_abbr] = round(home_prob, 3)
        if away_abbr:
            result[away_abbr] = round(1.0 - home_prob, 3)
    return result


def get_series_record_for_team(
    team_abbr: str,
    series_standings: list[dict],
    team_id_to_abbr: dict[int, str],
) -> dict:
    for series in series_standings:
        home_abbr = team_id_to_abbr.get(series["home_team_id"], "")
        away_abbr = team_id_to_abbr.get(series["away_team_id"], "")
        if home_abbr == team_abbr:
            return {"wins": series["home_wins"], "losses": series["away_wins"]}
        if away_abbr == team_abbr:
            return {"wins": series["away_wins"], "losses": series["home_wins"]}
    return {"wins": 0, "losses": 0}

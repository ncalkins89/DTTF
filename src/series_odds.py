"""
series_odds.py — fetch NBA playoff series win probabilities from DraftKings.

Primary source: category 1264 (Series Winner market).
Fallback for 3-3 (Game 7) series: category 487 (Game Lines moneyline).
When a series is at 3-3, the game moneyline equals the series win probability.

Also logs every fetch to dk_odds_audit so we can track whether:
  - Any Game 7 series ever appear in cat 1264 (contradicts the theory)
  - Any non-Game 7 series ever go missing from cat 1264 (also interesting)
"""
import requests
from nba_api.stats.static import teams as nba_teams

_DK_BASE = (
    "https://sportsbook-nash.draftkings.com"
    "/api/sportscontent/dkusnj/v1/leagues/42648"
)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}


def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def _normalize_pair(p1: float, p2: float) -> tuple[float, float]:
    total = p1 + p2
    if total == 0:
        return 0.5, 0.5
    return p1 / total, p2 / total


def _parse_american(s: str) -> int | None:
    s = s.replace("−", "-").replace("–", "-").strip()
    try:
        return int(s)
    except ValueError:
        return None


def _nickname_to_abbr() -> dict[str, str]:
    result = {}
    for t in nba_teams.get_teams():
        result[t["nickname"].lower()] = t["abbreviation"]
        result[t["full_name"].lower()] = t["abbreviation"]
        result[t["city"].lower()] = t["abbreviation"]
    return result


def _dk_get(path: str) -> dict:
    """GET a DK Nash API path, routing through ScraperAPI if SCRAPER_API_KEY is set.
    Raises on HTTP or timeout errors — no silent fallback.
    """
    import os
    url = _DK_BASE + path
    key = os.environ.get("SCRAPER_API_KEY", "")
    if key:
        fetch_url = "https://api.scraperapi.com?api_key=" + key + "&url=" + requests.utils.quote(url, safe="")
        print(f"[series_odds] fetching {path} via ScraperAPI")
    else:
        fetch_url = url
        print(f"[series_odds] fetching {path} direct (no SCRAPER_API_KEY)")

    for attempt in range(3):
        try:
            resp = requests.get(fetch_url, headers=_HEADERS, timeout=60)
            break
        except requests.Timeout:
            if attempt == 2:
                raise
            continue
    resp.raise_for_status()
    return resp.json()


def _parse_series_winner(data: dict) -> dict[str, dict]:
    """Parse category 1264 (Series Winner) response.
    Returns {team_abbr: {series_win_prob, american_odds, opponent_abbr, odds_source}}
    """
    nick_map = _nickname_to_abbr()

    market_to_event = {
        m["id"]: m["eventId"]
        for m in data.get("markets", [])
        if m.get("name") == "Series Winner"
    }

    by_market: dict[str, list[dict]] = {}
    for sel in data.get("selections", []):
        by_market.setdefault(sel.get("marketId", ""), []).append(sel)

    results: dict[str, dict] = {}
    for market_id, sels in by_market.items():
        if len(sels) != 2 or market_id not in market_to_event:
            continue

        parsed = []
        for sel in sels:
            odds = _parse_american(sel.get("displayOdds", {}).get("american", ""))
            nickname = (sel.get("participants") or [{}])[0].get("seoIdentifier", "")
            abbr = nick_map.get(nickname.lower())
            if not abbr:
                label = sel.get("label", "")
                abbr = nick_map.get(label.lower()) or nick_map.get(label.split()[-1].lower() if label else "")
            if odds is not None and abbr:
                parsed.append((abbr, odds))

        if len(parsed) != 2:
            for sel in sels:
                nick = (sel.get("participants") or [{}])[0].get("seoIdentifier", "?")
                print(f"[series_odds] cat1264 unmatched: '{nick}'")
            continue

        (a1, o1), (a2, o2) = parsed
        p1, p2 = _normalize_pair(american_to_prob(o1), american_to_prob(o2))
        results[a1] = {"series_win_prob": round(p1, 3), "american_odds": o1, "opponent_abbr": a2, "odds_source": "dk_cat1264"}
        results[a2] = {"series_win_prob": round(p2, 3), "american_odds": o2, "opponent_abbr": a1, "odds_source": "dk_cat1264"}
        print(f"[series_odds] cat1264  {a1} {o1:+d} ({p1:.1%}) vs {a2} {o2:+d} ({p2:.1%})")

    return results


def _parse_game_moneylines(data: dict) -> dict[str, dict]:
    """Parse category 487 (Game Lines) Moneyline selections.
    Returns {team_abbr: {series_win_prob, american_odds, opponent_abbr, odds_source}}
    """
    nick_map = _nickname_to_abbr()

    market_to_event = {
        m["id"]: m["eventId"]
        for m in data.get("markets", [])
        if m.get("name") == "Moneyline"
    }

    by_market: dict[str, list[dict]] = {}
    for sel in data.get("selections", []):
        by_market.setdefault(sel.get("marketId", ""), []).append(sel)

    results: dict[str, dict] = {}
    for market_id, sels in by_market.items():
        if len(sels) != 2 or market_id not in market_to_event:
            continue

        parsed = []
        for sel in sels:
            odds = _parse_american(sel.get("displayOdds", {}).get("american", ""))
            label = sel.get("label", "")
            abbr = nick_map.get(label.lower()) or nick_map.get(label.split()[-1].lower() if label else "")
            if odds is not None and abbr:
                parsed.append((abbr, odds))

        if len(parsed) != 2:
            continue

        (a1, o1), (a2, o2) = parsed
        p1, p2 = _normalize_pair(american_to_prob(o1), american_to_prob(o2))
        results[a1] = {"series_win_prob": round(p1, 3), "american_odds": o1, "opponent_abbr": a2, "odds_source": "dk_cat487_moneyline"}
        results[a2] = {"series_win_prob": round(p2, 3), "american_odds": o2, "opponent_abbr": a1, "odds_source": "dk_cat487_moneyline"}
        print(f"[series_odds] cat487ml {a1} {o1:+d} ({p1:.1%}) vs {a2} {o2:+d} ({p2:.1%})")

    return results


def fetch_series_win_probs(force_refresh: bool = False) -> dict[str, dict]:
    """Fetch series win probabilities from DK and save to DB.

    - Cat 1264 (Series Winner) for all active series
    - Cat 487 (Game Moneyline) as additional source for series not in cat 1264
    - When a series is 3-3 (Game 7), game moneyline == series win probability
    - Logs each observation to dk_odds_audit for long-run theory validation

    Raises on API failure — no silent fallback.
    """
    from src.db import (
        get_series_standings,
        upsert_series_odds,
        log_dk_odds_audit,
    )
    from src.data_fetcher import CURRENT_SEASON
    import nba_api.stats.static.teams as _nba_teams_mod

    team_map = {t["id"]: t["abbreviation"] for t in _nba_teams_mod.get_teams()}

    # Fetch both categories (raises on failure)
    cat1264_data = _dk_get("/categories/1264")
    cat487_data  = _dk_get("/categories/487")

    sw_result = _parse_series_winner(cat1264_data)
    ml_result = _parse_game_moneylines(cat487_data)

    # Merge: cat1264 wins; cat487 fills gaps for series not in cat1264
    result = dict(sw_result)
    for abbr, v in ml_result.items():
        if abbr not in result:
            result[abbr] = v

    # Audit: log observation for every active (non-decided) series
    standings = get_series_standings(CURRENT_SEASON) or []
    audit_records = []
    for s in standings:
        if s["home_wins"] >= 4 or s["away_wins"] >= 4:
            continue
        h = team_map.get(s["home_team_id"])
        a = team_map.get(s["away_team_id"])
        if not h or not a:
            continue

        in_sw  = int(h in sw_result)
        in_ml  = int(h in ml_result)
        gn     = s["home_wins"] + s["away_wins"] + 1

        audit_records.append({
            "home_abbr":         h,
            "away_abbr":         a,
            "home_wins":         s["home_wins"],
            "away_wins":         s["away_wins"],
            "game_number":       gn,
            "in_cat1264":        in_sw,
            "in_cat487_ml":      in_ml,
            "cat1264_home_odds": sw_result.get(h, {}).get("american_odds"),
            "cat1264_away_odds": sw_result.get(a, {}).get("american_odds"),
            "cat487_home_odds":  ml_result.get(h, {}).get("american_odds"),
            "cat487_away_odds":  ml_result.get(a, {}).get("american_odds"),
        })

        theory_ok = (gn == 7 and not in_sw) or (gn < 7 and in_sw)
        if not theory_ok:
            if gn == 7 and in_sw:
                print(f"[series_odds] THEORY VIOLATION: Game 7 {h}/{a} HAS cat1264 Series Winner market")
            elif gn < 7 and not in_sw:
                print(f"[series_odds] THEORY VIOLATION: Game {gn} {h}/{a} MISSING from cat1264 (not a Game 7)")

    if audit_records:
        log_dk_odds_audit(audit_records)
        print(f"[series_odds] logged {len(audit_records)} audit rows")

    if result:
        upsert_series_odds(result)
        print(f"[series_odds] saved {len(result) // 2} series to DB")
    else:
        print("[series_odds] WARNING: no series odds returned from either DK category")

    return result


if __name__ == "__main__":
    import json
    data = fetch_series_win_probs(force_refresh=True)
    print(json.dumps(data, indent=2))

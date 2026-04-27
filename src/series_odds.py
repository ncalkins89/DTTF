"""
series_odds.py — fetch NBA playoff series winner odds from DraftKings REST API.

Uses the public DraftKings Nash API (no auth, no Playwright required).
Returns {team_abbr: {"series_win_prob": float, "american_odds": int, "opponent_abbr": str}}
"""
import re

import requests
from nba_api.stats.static import teams as nba_teams

_DK_API_URL = (
    "https://sportsbook-nash.draftkings.com/api/sportscontent"
    "/dkusnj/v1/leagues/42648/categories/1264"
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
    s = s.replace("\u2212", "-").replace("\u2013", "-").strip()
    try:
        return int(s)
    except ValueError:
        return None


def _nickname_to_abbr() -> dict[str, str]:
    """Map team nickname (e.g. 'Spurs') and city/full variants to abbreviation."""
    result = {}
    for t in nba_teams.get_teams():
        result[t["nickname"].lower()] = t["abbreviation"]
        result[t["full_name"].lower()] = t["abbreviation"]
        result[t["city"].lower()] = t["abbreviation"]
    return result


def _fetch_dk_api() -> dict[str, dict]:
    import os
    scraper_key = os.environ.get("SCRAPER_API_KEY", "")
    if scraper_key:
        fetch_url = f"https://api.scraperapi.com?api_key={scraper_key}&url={requests.utils.quote(_DK_API_URL, safe='')}"
        print("[series_odds] fetching via ScraperAPI")
    else:
        fetch_url = _DK_API_URL
        print("[series_odds] fetching direct (no SCRAPER_API_KEY)")

    for attempt in range(3):
        try:
            resp = requests.get(fetch_url, headers=_HEADERS, timeout=60)
            break
        except requests.Timeout:
            if attempt == 2:
                raise
            continue
    resp.raise_for_status()
    data = resp.json()

    nick_map = _nickname_to_abbr()

    # Build event → participants map
    event_participants: dict[str, list[dict]] = {}
    for event in data.get("events", []):
        eid = event["id"]
        event_participants[eid] = [p["name"] for p in event.get("participants", [])]

    # Build market → event map
    market_to_event: dict[str, str] = {
        m["id"]: m["eventId"]
        for m in data.get("markets", [])
        if m.get("name") == "Series Winner"
    }

    # Group selections by market
    by_market: dict[str, list[dict]] = {}
    for sel in data.get("selections", []):
        mid = sel.get("marketId", "")
        by_market.setdefault(mid, []).append(sel)

    results: dict[str, dict] = {}

    for market_id, sels in by_market.items():
        if len(sels) != 2:
            continue
        eid = market_to_event.get(market_id)
        if eid is None:
            continue

        parsed = []
        for sel in sels:
            odds_str = sel.get("displayOdds", {}).get("american", "")
            odds = _parse_american(odds_str)
            nickname = (sel.get("participants") or [{}])[0].get("seoIdentifier", "")
            abbr = nick_map.get(nickname.lower())
            if odds is None or not abbr:
                # Try label as fallback
                label = sel.get("label", "")
                # label is like "SA Spurs" or "NY Knicks" — try last word as nickname
                last_word = label.split()[-1].lower() if label else ""
                abbr = abbr or nick_map.get(last_word)
            if odds is not None and abbr:
                parsed.append((abbr, odds))

        if len(parsed) != 2:
            # Log unmatched
            for sel in sels:
                nick = (sel.get("participants") or [{}])[0].get("seoIdentifier", "?")
                print(f"[series_odds] unmatched: '{nick}'")
            continue

        (abbr1, odds1), (abbr2, odds2) = parsed
        p1 = american_to_prob(odds1)
        p2 = american_to_prob(odds2)
        p1n, p2n = _normalize_pair(p1, p2)

        results[abbr1] = {
            "series_win_prob": round(p1n, 3),
            "american_odds": odds1,
            "opponent_abbr": abbr2,
        }
        results[abbr2] = {
            "series_win_prob": round(p2n, 3),
            "american_odds": odds2,
            "opponent_abbr": abbr1,
        }
        print(f"[series_odds] {abbr1} {odds1:+d} ({p1n:.1%}) vs {abbr2} {odds2:+d} ({p2n:.1%})")

    return results


def fetch_series_win_probs(force_refresh: bool = False) -> dict[str, dict]:
    """Returns {team_abbr: {"series_win_prob": float, "american_odds": int, "opponent_abbr": str}}"""
    try:
        result = _fetch_dk_api()
    except Exception as e:
        print(f"[series_odds] API fetch failed: {e}")
        result = {}

    if result:
        from src.db import upsert_series_odds
        upsert_series_odds(result)
        print(f"[series_odds] {len(result) // 2} series loaded from DK API, saved to DB")
    else:
        print("[series_odds] fetch failed — returning empty")

    return result


if __name__ == "__main__":
    import json
    data = fetch_series_win_probs(force_refresh=True)
    print(json.dumps(data, indent=2))

"""
External projection fetchers.

Currently supports DraftEdge (free, no auth required).
Returns {player_id: {"pts", "reb", "ast", "pra", "team", "opp"}} keyed by NBA API player_id.
"""
import difflib
import gzip
import json
import re
import urllib.request
from pathlib import Path

import diskcache

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"
CACHE = diskcache.Cache(str(CACHE_DIR))

_DRAFTEDGE_URL = "https://draftedge.com/draftedge-data/nba_proj_dk.json"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://draftedge.com/nba/nba-daily-projections/",
    "Accept-Encoding": "gzip, deflate",
}


def _parse_name(html: str) -> str:
    m = re.search(r'<p class="teamview mb-0">([^<]+)', html)
    if not m:
        return ""
    return m.group(1).strip()


def _parse_team(html: str) -> str:
    m = re.search(r"uploads/([a-z]+)\.png", html)
    return m.group(1).upper() if m else ""


def _parse_opp(html: str) -> str:
    m = re.search(r"vs([A-Z]{2,3})", html)
    return m.group(1).upper() if m else ""


def _build_name_map() -> dict[str, int]:
    """Lowercase player name → player_id from NBA API static list."""
    from nba_api.stats.static import players as nba_players
    return {p["full_name"].lower(): p["id"] for p in nba_players.get_players()}


def _fuzzy_match(name: str, name_map: dict[str, int]) -> int | None:
    key = name.lower()
    if key in name_map:
        return name_map[key]
    matches = difflib.get_close_matches(key, name_map.keys(), n=1, cutoff=0.82)
    return name_map[matches[0]] if matches else None


def fetch_draftedge_projections() -> dict[int, dict]:
    """
    Returns {player_id: {"pts", "reb", "ast", "pra", "team", "opp"}}
    for today's slate. Cached 30 minutes.
    """
    cache_key = "draftedge_projections"
    cached = CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        req = urllib.request.Request(_DRAFTEDGE_URL, headers=_HEADERS)
        resp = urllib.request.urlopen(req, timeout=10)
        raw = resp.read()
        try:
            rows = json.loads(gzip.decompress(raw))
        except Exception:
            rows = json.loads(raw)
    except Exception as e:
        print(f"[draftedge] fetch failed: {e}")
        return {}

    name_map = _build_name_map()
    result = {}
    unmatched = []

    for row in rows:
        name_html = row.get("NAME", "")
        name = _parse_name(name_html)
        if not name:
            continue

        player_id = _fuzzy_match(name, name_map)
        if player_id is None:
            unmatched.append(name)
            continue

        try:
            pts = float(row.get("PTS") or 0)
            reb = float(row.get("REB") or 0)
            ast = float(row.get("AST") or 0)
        except (TypeError, ValueError):
            continue

        result[player_id] = {
            "pts": round(pts, 1),
            "reb": round(reb, 1),
            "ast": round(ast, 1),
            "pra": round(pts + reb + ast, 1),
            "team": _parse_team(name_html),
            "opp": _parse_opp(name_html),
        }

    if unmatched:
        print(f"[draftedge] {len(unmatched)} unmatched players: {unmatched[:5]}")
    print(f"[draftedge] matched {len(result)}/{len(rows)} players")

    CACHE.set(cache_key, result, expire=1800)
    return result


_FD_RESEARCH_URL = "https://www.fanduel.com/research/nba/fantasy/dfs-projections"
_FD_GRAPHQL_URL = "https://fdresearch-api.fanduel.com/graphql"
_FD_GRAPHQL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Referer": _FD_RESEARCH_URL,
}
_FD_QUERY = """
query GetProjections($input: ProjectionsInput!) {
  getProjections(input: $input) {
    ... on NbaPlayer {
      player { name }
      team { abbreviation }
      gameInfo {
        homeTeam { abbreviation }
        awayTeam { abbreviation }
      }
      minutes points rebounds assists fantasy
    }
  }
}
"""


def _get_fd_slate_id() -> str | None:
    """Fetch today's main NBA DFS slate ID from FanDuel research page."""
    import requests as _req
    r = _req.get(_FD_RESEARCH_URL, headers={"User-Agent": _FD_GRAPHQL_HEADERS["User-Agent"]}, timeout=15)
    r.raise_for_status()
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', r.text)
    if not m:
        return None
    data = json.loads(m.group(1))
    slates = data.get("props", {}).get("pageProps", {}).get("projectionInfo", {}).get("slatesFilter", [])
    main = next((s for s in slates if s.get("label") == "Main"), None)
    return (main or slates[0])["value"] if slates else None


def fetch_fanduel_projections() -> dict[int, dict]:
    """
    Returns {player_id: {"pts", "reb", "ast", "pra", "min", "fd_fantasy", "team", "opp"}}
    from FanDuel Research DFS projections. Cached 30 minutes.
    """
    import requests as _req

    cache_key = "fanduel_projections"
    cached = CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        slate_id = _get_fd_slate_id()
        if not slate_id:
            print("[fanduel] could not find slate ID")
            return {}

        payload = {
            "query": _FD_QUERY,
            "variables": {
                "input": {
                    "type": "DAILY",
                    "position": "NBA_PLAYER",
                    "sport": "NBA",
                    "slateId": slate_id,
                }
            },
            "operationName": "GetProjections",
        }
        resp = _req.post(_FD_GRAPHQL_URL, headers=_FD_GRAPHQL_HEADERS, json=payload, timeout=15)
        resp.raise_for_status()
        rows = resp.json().get("data", {}).get("getProjections", [])
    except Exception as e:
        print(f"[fanduel] fetch failed: {e}")
        return {}

    name_map = _build_name_map()
    result = {}
    unmatched = []

    for row in rows:
        if not row:
            continue
        name = row.get("player", {}).get("name", "")
        if not name:
            continue

        player_id = _fuzzy_match(name, name_map)
        if player_id is None:
            unmatched.append(name)
            continue

        team = row.get("team", {}).get("abbreviation", "")
        game = row.get("gameInfo", {})
        home = game.get("homeTeam", {}).get("abbreviation", "")
        away = game.get("awayTeam", {}).get("abbreviation", "")
        opp = away if team == home else home

        try:
            pts = float(row.get("points") or 0)
            reb = float(row.get("rebounds") or 0)
            ast = float(row.get("assists") or 0)
            mins = float(row.get("minutes") or 0)
            fantasy = float(row.get("fantasy") or 0)
        except (TypeError, ValueError):
            continue

        result[player_id] = {
            "pts": round(pts, 1),
            "reb": round(reb, 1),
            "ast": round(ast, 1),
            "pra": round(pts + reb + ast, 1),
            "min": round(mins, 1),
            "fd_fantasy": round(fantasy, 1),
            "team": team,
            "opp": opp,
        }

    if unmatched:
        print(f"[fanduel] {len(unmatched)} unmatched: {unmatched[:5]}")
    print(f"[fanduel] matched {len(result)}/{len(rows)} players (slate {slate_id})")

    CACHE.set(cache_key, result, expire=1800)
    return result


_ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"

def fetch_injuries() -> dict[str, dict]:
    """
    Returns {player_name_lower: {"status": str, "comment": str}} from ESPN.
    Status values: "Out", "Day-To-Day", "Questionable", "Probable", "Doubtful".
    Cached 30 min.
    """
    cache_key = "espn_injuries"
    cached = CACHE.get(cache_key)
    if cached is not None:
        return cached

    import requests
    try:
        data = requests.get(_ESPN_INJURIES_URL, timeout=10).json()
    except Exception as e:
        print(f"[injuries] fetch failed: {e}")
        return {}

    result = {}
    for team in data.get("injuries", []):
        for p in team.get("injuries", []):
            name = p.get("athlete", {}).get("displayName", "")
            if not name:
                continue
            result[name.lower()] = {
                "status": p.get("status", ""),
                "comment": p.get("shortComment", ""),
            }

    CACHE.set(cache_key, result, expire=1800)
    print(f"[injuries] {len(result)} players on injury report")
    return result

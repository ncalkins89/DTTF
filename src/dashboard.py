import json
import os
import sys
import time
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import Dash, Input, Output, State, callback_context, dcc, html
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv(Path(__file__).parent.parent / ".env")

# Ensure DB tables exist (no-op if already created)
from src.db import init_db as _init_db
_init_db()

from src.data_fetcher import (
    clear_cache,
    get_active_roster,
    get_player_game_logs,
    get_player_game_logs_365,
    get_series_standings,
    get_team_defense_ratings,
    get_todays_games,
)
from src.odds import fetch_per_game_win_probs, fetch_series_win_probs, get_series_record_for_team
from src.external import fetch_draftedge_projections, fetch_fanduel_projections, fetch_injuries
from src.research import compute_local_signals
from src.db import (
    init_db,
    get_schedule as db_get_schedule,
    get_odds as db_get_odds,
    get_latest_odds as db_get_latest_odds,
    get_series_standings as db_get_series_standings,
    get_de_projections as db_get_de_projections,
    get_fd_projections as db_get_fd_projections,
    get_injuries as db_get_injuries,
    get_game_lines as db_get_game_lines,
    get_latest_game_lines as db_get_latest_game_lines,
    get_last_updated as db_get_last_updated,
    get_known_game_dates as db_get_known_game_dates,
    get_game_logs as db_get_game_logs,
    get_all_game_logs_batch as db_get_all_game_logs_batch,
)
from src.picks import (
    get_pick_history,
    remove_pick,
    get_used_player_ids,
    record_pick,
    update_actual_pra,
)
from src.projections import project_player

from nba_api.stats.static import teams as nba_teams

TEAM_MAP = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}
TEAM_ABBR_MAP = {v: k for k, v in TEAM_MAP.items()}

EXT_PROJ_PATH = Path(__file__).parent.parent / "data" / "external_projections.json"

NBA_TEAM_COLORS = {
    "ATL": "#E03A3E", "BOS": "#007A33", "BKN": "#AAAAAA", "CHA": "#00788C",
    "CHI": "#CE1141", "CLE": "#860038", "DAL": "#00538C", "DEN": "#FEC524",
    "DET": "#C8102E", "GSW": "#1D428A", "HOU": "#CE1141", "IND": "#FDBB30",
    "LAC": "#C8102E", "LAL": "#FDB927", "MEM": "#5D76A9", "MIA": "#98002E",
    "MIL": "#00471B", "MIN": "#236192", "NOP": "#B4975A", "NYK": "#F58426",
    "OKC": "#007AC1", "ORL": "#0077C0", "PHI": "#006BB6", "PHX": "#E56020",
    "POR": "#E03A3E", "SAC": "#5A2D81", "SAS": "#C4CED4", "TOR": "#CE1141",
    "UTA": "#F9A01B", "WAS": "#E31837",
}


def _hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def load_external_projections() -> dict:
    if EXT_PROJ_PATH.exists():
        with open(EXT_PROJ_PATH) as f:
            return json.load(f)
    return {}


def save_external_projection(game_date: str, player_id: int, pra: float) -> None:
    data = load_external_projections()
    data[f"{game_date}_{player_id}"] = pra
    EXT_PROJ_PATH.parent.mkdir(exist_ok=True)
    with open(EXT_PROJ_PATH, "w") as f:
        json.dump(data, f, indent=2)


_df_cache: dict[str, tuple] = {}  # "{date}_{round}" -> (df, timestamp)
_playoff_players_cache: list[dict] = []  # [{player_id, player_name, team_abbr}]

def _get_all_playoff_players() -> list[dict]:
    """All players on active playoff teams, cached in-process."""
    global _playoff_players_cache
    if _playoff_players_cache:
        return _playoff_players_cache
    from src.data_fetcher import CURRENT_SEASON
    standings = db_get_series_standings(CURRENT_SEASON) or get_series_standings()
    seen_teams: set[int] = set()
    result = []
    for s in standings:
        for team_id, abbr in [
            (s["home_team_id"], TEAM_MAP.get(s["home_team_id"], "")),
            (s["away_team_id"], TEAM_MAP.get(s["away_team_id"], "")),
        ]:
            if team_id in seen_teams:
                continue
            seen_teams.add(team_id)
            for p in get_active_roster(team_id):
                result.append({
                    "player_id": p["player_id"],
                    "player_name": p["player_name"],
                    "team_abbr": abbr,
                })
    result.sort(key=lambda x: x["player_name"])
    _playoff_players_cache = result
    return result

def build_todays_player_df(game_date: str | None = None, current_round: int = 1) -> pd.DataFrame:
    if game_date is None:
        game_date = date.today().isoformat()

    cache_key = f"{game_date}_{current_round}"
    ttl = 300 if game_date == date.today().isoformat() else 3600
    cached = _df_cache.get(cache_key)
    if cached:
        df, ts = cached
        if time.time() - ts < ttl:
            print(f"[dashboard] cache hit for {game_date}", flush=True)
            return df

    # Prefer DB (load_db.py populates it each morning) → fall back to live fetch
    games = db_get_schedule(game_date) or get_todays_games(game_date)
    if not games:
        return pd.DataFrame()

    def_ratings = get_team_defense_ratings()

    from src.data_fetcher import CURRENT_SEASON
    db_standings = db_get_series_standings(CURRENT_SEASON)  # DB first
    series_standings = db_standings if db_standings else get_series_standings()

    # Fill in 0-0 entries for any series in today's games not already in standings.
    # Needed when viewing future dates — those series may not be in the DB yet.
    covered = {
        (s["home_team_id"], s["away_team_id"]) for s in series_standings
    }
    for game in games:
        pair = (game["home_team_id"], game["away_team_id"])
        rev = (game["away_team_id"], game["home_team_id"])
        if pair not in covered and rev not in covered:
            series_standings.append({
                "home_team_id": game["home_team_id"],
                "away_team_id": game["away_team_id"],
                "home_wins": 0,
                "away_wins": 0,
            })
            covered.add(pair)

    db_odds = db_get_odds(game_date) or db_get_latest_odds() or fetch_per_game_win_probs()

    per_game_probs = db_odds
    series_win_probs = fetch_series_win_probs(series_standings, per_game_probs)
    used_ids = get_used_player_ids()
    ext_projs = load_external_projections()

    db_de = db_get_de_projections(game_date)  # DB first
    is_today = game_date == date.today().isoformat()
    # Only fall back to live DE / live game lines for today's date.
    # For past dates the live APIs return today's data which would be wrong.
    de_projs = db_de if db_de else (fetch_draftedge_projections() if is_today else {})
    fd_projs = db_get_fd_projections(game_date) or (fetch_fanduel_projections() if is_today else {})
    injuries = db_get_injuries() or (fetch_injuries() if is_today else {})

    from src.odds import fetch_game_lines
    game_lines = db_get_game_lines(game_date) or (
        (db_get_latest_game_lines() or fetch_game_lines()) if is_today else {}
    )

    from src.data_fetcher import CURRENT_SEASON as _CS, PRIOR_SEASON as _PS

    # Collect all (team, opp) pairs and rosters first so we can batch-load logs.
    team_game_info = []
    for game in games:
        for team_id, opp_team_id, team_abbr, opp_abbr, is_home in [
            (game["home_team_id"], game["away_team_id"],
             game["home_team_abbr"], game["away_team_abbr"], True),
            (game["away_team_id"], game["home_team_id"],
             game["away_team_abbr"], game["home_team_abbr"], False),
        ]:
            roster = get_active_roster(team_id)
            team_game_info.append((team_id, opp_team_id, team_abbr, opp_abbr, is_home, roster, game))

    all_pids = list({p["player_id"] for _, _, _, _, _, roster, _ in team_game_info for p in roster})
    log_cache = db_get_all_game_logs_batch(all_pids, [_CS, _PS])

    def _get_logs_365(pid: int) -> pd.DataFrame:
        cur = log_cache.get((pid, _CS), pd.DataFrame())
        prior = log_cache.get((pid, _PS), pd.DataFrame())
        parts = [df for df in [cur, prior] if not df.empty]
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts).sort_values("GAME_DATE", ascending=False).reset_index(drop=True)

    rows = []
    for team_id, opp_team_id, team_abbr, opp_abbr, is_home, roster, game in team_game_info:
        series_record = get_series_record_for_team(team_abbr, series_standings, TEAM_MAP)
        per_game_p = per_game_probs.get(team_abbr, 0.5)
        series_win_prob = series_win_probs.get(team_abbr, 0.5)

        for player in roster:
            pid = player["player_id"]
            logs = _get_logs_365(pid)
            proj = project_player(
                player_id=pid,
                opponent_team_id=opp_team_id,
                game_logs=logs,
                def_ratings=def_ratings,
                series_record=series_record,
                per_game_win_prob=per_game_p,
                current_round=current_round,
            )
            ext_key = f"{game_date}_{pid}"
            ext_pra = ext_projs.get(ext_key)
            de = de_projs.get(pid)
            de_pra = de["pra"] if de else None
            fd = fd_projs.get(pid)
            fd_pra = fd["pra"] if fd else None

            rs_avg = playoff_avg = None
            if not logs.empty:
                rs = logs[logs["SEASON_TYPE"] == "Regular Season"]
                pl = logs[logs["SEASON_TYPE"] == "Playoffs"]
                if not rs.empty:
                    rs_avg = round(rs["PRA"].mean(), 1)
                if not pl.empty:
                    playoff_avg = round(pl["PRA"].mean(), 1)

            has_recent_games = not logs.empty
            de_loaded = len(de_projs) > 10
            likely_out = has_recent_games and de is None and de_loaded

            our_pra = proj["projected_pra"]
            all_sources = [p for p in [our_pra, de_pra, fd_pra, playoff_avg] if p is not None]
            pred_pra = round(sum(all_sources) / len(all_sources), 1)

            lose_prob = 1.0 - series_win_prob
            pred_urgency = round(pred_pra * lose_prob, 2)
            urgency_ours = round(our_pra * lose_prob, 2)
            urgency_de   = round(de_pra  * lose_prob, 2) if de_pra  is not None else None
            urgency_fd   = round(fd_pra  * lose_prob, 2) if fd_pra  is not None else None

            inj = injuries.get(player["player_name"].lower(), {})
            inj_status = inj.get("status", "")
            inj_comment = inj.get("comment", "")
            if inj_status == "Out":
                status = "❌ Out"
            elif inj_status == "Day-To-Day":
                status = "⚠ DTD"
            else:
                status = ""

            signal_bullets = compute_local_signals(
                player_id=pid,
                player_name=player["player_name"],
                team_abbr=team_abbr,
                opp_abbr=opp_abbr,
                game_date=game_date,
                logs=logs,
                injury_data=injuries,
                schedule=[],
                game_lines=game_lines,
            )
            if inj_comment:
                signal_bullets = [f"🩹 {inj_comment}"] + signal_bullets
            signals_text = "\n".join(signal_bullets) if signal_bullets else ""

            rows.append({
                "player_id": pid,
                "Player": player["player_name"],
                "Pos": player["position"],
                "Team": team_abbr,
                "Opp": opp_abbr,
                "Status": status,
                "Pred": pred_pra,
                "Our Proj": our_pra,
                "DE Proj": de_pra if de_pra is not None else None,
                "DE Pts": de["pts"] if de else None,
                "DE Reb": de["reb"] if de else None,
                "DE Ast": de["ast"] if de else None,
                "FD Proj": fd_pra if fd_pra is not None else None,
                "FD Pts": fd["pts"] if fd else None,
                "FD Reb": fd["reb"] if fd else None,
                "FD Ast": fd["ast"] if fd else None,
                "FD Min": fd["min"] if fd else None,
                "RS Avg": rs_avg,
                "PO Avg": playoff_avg,
                "Ext Proj": ext_pra,
                "Series Win%": round(series_win_prob, 3),
                "series_win_prob_raw": series_win_prob,
                "series_lose_prob_raw": round(1.0 - series_win_prob, 3),
                "Urgency": pred_urgency,
                "Urgency_Ours": urgency_ours,
                "Urgency_DE": urgency_de,
                "Urgency_FD": urgency_fd,
                "Exp Games": proj["expected_future_games"],
                "Picked": pid in used_ids,
                "game_id": game["game_id"],
                "Inj Note": inj_comment if inj_comment else None,
                "Signals": signals_text,
                "is_home": is_home,
                "team_wins": series_record["wins"],
                "team_losses": series_record["losses"],
                "_proj": proj,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values("Urgency", ascending=False).reset_index(drop=True)
    _df_cache[cache_key] = (df, time.time())
    return df


# ── App setup ───────────────────────────────────────────────────────────────

# NOTE: _today_layout() must be defined before app.layout since it's called
# inline to embed today's tab in the main DOM (avoids dynamic layout race conditions).
def _today_layout():
    return html.Div([
        # ── Sub-tabs ────────────────────────────────────────────────────
        dbc.Tabs(id="today-subtabs", active_tab="subtab-players", className="mt-1", children=[
            dbc.Tab(label="Browse Players", tab_id="subtab-players"),
            dbc.Tab(label="Player Scatter", tab_id="subtab-scatter"),
            dbc.Tab(label="Compare Players", tab_id="subtab-compare"),
        ]),

        # ── Date strip (NBA scoreboard style) ───────────────────────────
        # Store tracking the week offset (0 = week containing today)
        dcc.Store(id="date-strip-offset", data=0),
        html.Div(style={"position": "relative"}, children=[
            # Real date picker — always rendered, zero-size, positioned so its
            # calendar popup floats naturally. JS clicks it when the 🗓 icon is pressed.
            html.Div(
                dcc.DatePickerSingle(
                    id="game-date-picker",
                    date=None,
                    display_format="MMM D, YYYY",
                    disabled_days=_compute_disabled_days(),
                    with_portal=False,
                ),
                id="date-picker-wrapper",
                style={"position": "absolute", "top": "44px", "left": "0",
                       "opacity": "0", "pointerEvents": "none", "height": "0",
                       "overflow": "visible", "zIndex": "1000"},
            ),
            # Strip row: ‹ [chips] › 🗓  |  spinner  updated-text
            html.Div([
                html.Button("‹", id="date-prev-btn", className="date-nav-btn"),
                html.Div(id="date-strip-chips",
                         style={"display": "flex", "gap": "2px", "alignItems": "center"}),
                html.Button("›", id="date-next-btn", className="date-nav-btn"),
                html.Button("🗓", id="date-cal-btn", className="date-nav-btn",
                            style={"fontSize": "17px", "marginLeft": "4px", "opacity": "0.65"}),
                dcc.Loading(
                    html.Div(style={"width": "20px", "height": "20px"}),
                    id="header-data-loading",
                    target_components={"loading-sentinel": "children"},
                    type="circle", color="#0071e3", delay_show=0,
                    style={"display": "inline-block", "verticalAlign": "middle",
                           "width": "20px", "height": "20px"},
                ),
            ], style={"display": "flex", "alignItems": "center", "gap": "0",
                      "background": "#f5f5f7", "borderRadius": "10px",
                      "padding": "4px 8px", "width": "fit-content"}),
        ]),

        # ── Selected date label ──────────────────────────────────────────
        html.Div(id="selected-date-label",
                 style={"fontSize": "13px", "color": "#6e6e73", "marginTop": "8px",
                        "marginBottom": "4px", "fontWeight": "500"}),

        # ── Schedule strip ───────────────────────────────────────────────
        dcc.Loading(
            html.Div(id="schedule-strip", className="mb-2"),
            type="circle", color="#0071e3", id="schedule-loading",
            delay_show=0, style={"minHeight": "60px"},
        ),

        # ── Browse Players subtab ────────────────────────────────────────
        html.Div(id="subtab-players-pane", className="mt-3", children=[
            dbc.Row([
                dbc.Col([
                    html.Small("Urgency model:", className="text-muted me-1"),
                    dbc.Select(
                        id="urgency-model-select",
                        options=[
                            {"label": "Pred (blended)", "value": "Urgency"},
                            {"label": "Ours",           "value": "Urgency_Ours"},
                            {"label": "DE",             "value": "Urgency_DE"},
                            {"label": "FD",             "value": "Urgency_FD"},
                        ],
                        value="Urgency",
                        style={"width": "150px", "fontSize": "13px", "display": "inline-block"},
                    ),
                ], width="auto", className="d-flex align-items-center mb-2"),
            ]),
            dcc.Loading(
                html.Div(id="today-table-container"),
                type="circle", color="#0071e3", id="table-loading",
                delay_show=0, style={"minHeight": "500px"},
            ),
        ]),

        # ── Player Scatter subtab ────────────────────────────────────────
        html.Div(id="subtab-scatter-pane", className="mt-3", style={"display": "none"}, children=[
            dbc.Card(dbc.CardBody([
                dcc.Loading(
                    dcc.Graph(id="scatter-chart", config={"displayModeBar": True},
                              style={"height": "420px"}),
                    type="circle", color="#0071e3", delay_show=0, style={"minHeight": "420px"},
                ),
            ], style={"padding": "10px 12px"})),
        ]),

        # ── Compare Players subtab ───────────────────────────────────────
        html.Div(id="subtab-compare-pane", className="mt-3", style={"display": "none"}, children=[
            dbc.Card(dbc.CardBody([
                dbc.Row([
                    dbc.Col(
                        dcc.Dropdown(id="compare-player-dropdown",
                                     placeholder="Select players to compare...",
                                     multi=True, style={"fontSize": "13px"}),
                        width=9,
                    ),
                    dbc.Col(
                        dbc.Select(id="compare-metric-select",
                                   options=[
                                       {"label": "PRA", "value": "PRA"},
                                       {"label": "Points", "value": "PTS"},
                                       {"label": "Rebounds", "value": "REB"},
                                       {"label": "Assists", "value": "AST"},
                                       {"label": "Minutes", "value": "MIN"},
                                   ],
                                   value="PRA",
                                   style={"fontSize": "13px"}),
                        width=2,
                    ),
                ], className="mb-3"),
                dcc.Loading(
                    dcc.Graph(id="compare-chart", config={"displayModeBar": False},
                              style={"height": "420px"}),
                    type="circle", color="#0071e3", delay_show=0,
                ),
            ])),
        ]),

    ])

_CUSTOM_CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; background: #f5f5f7; color: #1d1d1f; }
.card { border: none; border-radius: 12px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
.btn-sm { border-radius: 8px; font-size: 13px; }
.form-control, .form-select { border-radius: 8px; border-color: #d2d2d7; font-size: 13px; }
.schedule-chip { display:inline-block; background:#fff; border:1px solid #d2d2d7; border-radius:10px; padding:8px 14px; margin:3px; font-size:12px; box-shadow:0 1px 3px rgba(0,0,0,.06); }
.date-nav-btn { background:none; border:none; font-size:22px; color:#0071e3; cursor:pointer; padding:0 8px; line-height:1; flex-shrink:0; }
.date-nav-btn:hover { color:#0051a8; }
/* All chips identical fixed size — only color/bg changes for active/hover */
.date-chip { display:inline-flex; flex-direction:column; align-items:center; padding:5px 0; border-radius:8px; cursor:pointer; width:54px; flex-shrink:0; transition:background 0.12s; user-select:none; box-sizing:border-box; }
.date-chip:hover:not(.no-game) { background:#e5e5ea; }
.date-chip.active { background:#0071e3 !important; }
.date-chip.active .date-chip-day { color:rgba(255,255,255,0.8) !important; }
.date-chip.active .date-chip-num, .date-chip.active .date-chip-num span { color:#fff !important; }
.date-chip.no-game { opacity:0.3; cursor:default; }
.date-chip-day { font-size:10px; font-weight:500; color:#8e8e93; letter-spacing:0.6px; text-transform:uppercase; line-height:1.5; white-space:nowrap; }
.date-chip-num { font-size:14px; font-weight:600; color:#1d1d1f; line-height:1.3; white-space:nowrap; }
/* Hide the date picker text input — show only the calendar popup */
#date-picker-wrapper .SingleDatePickerInput__withBorder { border:none !important; background:transparent !important; }
#date-picker-wrapper input.DateInput_input { opacity:0; height:1px; padding:0; margin:0; min-width:0; width:1px; position:absolute; }
#date-picker-wrapper .DateInput { width:1px; overflow:hidden; }
#date-picker-wrapper .SingleDatePickerInput_calendarIcon { display:none !important; }
.ag-header-group-cell-label { justify-content: center !important; font-weight: 600; color: #1d1d1f; }
.ag-theme-alpine .ag-cell { padding-left: 8px !important; padding-right: 8px !important; }
.ag-theme-alpine .ag-header-cell { padding-left: 8px !important; padding-right: 8px !important; }
.series-win-high { color: #16a34a; font-weight: 700; font-size: 14px; }
.series-win-mid  { color: #ca8a04; font-weight: 700; font-size: 14px; }
.series-win-low  { color: #dc2626; font-weight: 700; font-size: 14px; }
.ag-tooltip { white-space: pre-line; max-width: 340px; font-size: 12px; line-height: 1.6; background: #fff; color: #1d1d1f; border: 1px solid #d2d2d7; border-radius: 10px; padding: 10px 14px; box-shadow: 0 4px 16px rgba(0,0,0,.12); }

/* ── Top-level tabs: Apple-style segmented control ── */
#main-tabs { border: none !important; background: #e5e5ea; border-radius: 10px; padding: 3px; display: inline-flex; gap: 2px; }
#main-tabs .nav-item { flex: 1; }
#main-tabs .nav-link { border: none !important; border-radius: 8px; padding: 6px 20px; font-size: 14px; font-weight: 500; color: #6e6e73; background: transparent; transition: background 0.15s, color 0.15s; white-space: nowrap; }
#main-tabs .nav-link.active { background: #fff !important; color: #1d1d1f !important; box-shadow: 0 1px 4px rgba(0,0,0,.15); font-weight: 600; }
#main-tabs .nav-link:hover:not(.active) { color: #1d1d1f; }

/* ── Sub-tabs: small muted underline ── */
#today-subtabs, #history-subtabs { border-bottom: 1px solid #e5e5ea !important; }
#today-subtabs .nav-link, #history-subtabs .nav-link { border: none; color: #8e8e93; font-size: 12px; font-weight: 500; padding: 5px 14px; }
#today-subtabs .nav-link.active, #history-subtabs .nav-link.active { color: #0071e3; border-bottom: 2px solid #0071e3; background: none; font-weight: 600; }
#today-subtabs .nav-link:hover:not(.active), #history-subtabs .nav-link:hover:not(.active) { color: #1d1d1f; }
"""

app = Dash(__name__,
           external_stylesheets=[dbc.themes.BOOTSTRAP],
           suppress_callback_exceptions=True)
app.title = "DTTF — Drive to the Finals"
server = app.server  # gunicorn entry point — safe to have locally too

# ── Layout ──────────────────────────────────────────────────────────────────

app.index_string = app.index_string.replace(
    "</head>", f"<style>{_CUSTOM_CSS}</style></head>"
)

def _history_layout():
    return html.Div([
        dbc.Tabs(id="history-subtabs", active_tab="history-record", className="mt-1", children=[
            dbc.Tab(label="Record Picks", tab_id="history-record"),
            dbc.Tab(label="Team Commitment", tab_id="history-commitment"),
        ]),

        # Record Picks sub-pane
        html.Div(id="history-record-pane", className="mt-3", children=[
            dbc.Card(dbc.CardBody([
                dbc.InputGroup([
                    dcc.Dropdown(id="pick-dropdown", placeholder="Select player(s)...",
                                 multi=True,
                                 style={"minWidth": "400px", "fontSize": "13px"}),
                    dbc.Button("Record Pick(s)", id="record-pick-btn",
                               color="primary", size="sm"),
                ]),
                html.Div(id="pick-status", className="mt-2"),
                html.Hr(className="my-2"),
                dbc.InputGroup([
                    dcc.Dropdown(id="remove-pick-dropdown", placeholder="Select pick to remove...",
                                 style={"minWidth": "400px", "fontSize": "13px"}),
                    dbc.Button("Remove Pick", id="remove-pick-btn",
                               color="danger", size="sm", outline=True),
                ]),
                html.Div(id="remove-pick-status", className="mt-2"),
            ]), className="mb-3"),
            dcc.Loading(html.Div(id="history-table-container"), type="circle", color="#0071e3",
                        delay_show=0, style={"minHeight": "200px"}),
            html.Hr(),
            html.Details([
                html.Summary("Update actual PRA for a past pick",
                             style={"cursor": "pointer", "color": "#aaa"}),
                dbc.Row([
                    dbc.Col(dcc.Dropdown(id="actual-player-dropdown",
                                         placeholder="Pick to update...",
                                         style={"color": "#000"}), width=4),
                    dbc.Col(dbc.Input(id="actual-pra-input", type="number",
                                       placeholder="Actual PRA", min=0, max=150), width=2),
                    dbc.Col(dbc.Button("Update", id="update-actual-btn", color="secondary"), width=2),
                ], className="mt-2"),
                html.Div(id="update-actual-status", className="small text-muted mt-1"),
            ]),
        ]),

        # Team Commitment sub-pane
        html.Div(id="history-commitment-pane", className="mt-3", style={"display": "none"}, children=[
            dbc.Card(dbc.CardBody([
                html.P("How many picks you've used from each team, and remaining eligible players.",
                       className="text-muted mb-3", style={"fontSize": "12px"}),
                dcc.Loading(
                    dcc.Graph(id="team-commitment-chart", config={"displayModeBar": False},
                              style={"height": "380px"}),
                    type="circle", color="#0071e3", delay_show=0,
                ),
            ])),
        ]),
    ])


def _compute_disabled_days() -> list[str]:
    """Return all dates between first and last known game date that have no game scheduled."""
    from datetime import timedelta
    game_dates = set(db_get_known_game_dates())
    if len(game_dates) < 2:
        return []
    min_d = date.fromisoformat(min(game_dates))
    max_d = date.fromisoformat(max(game_dates))
    disabled = []
    cur = min_d
    while cur <= max_d:
        s = cur.isoformat()
        if s not in game_dates:
            disabled.append(s)
        cur += timedelta(days=1)
    return disabled


app.layout = dbc.Container(
    fluid=True,
    style={"maxWidth": "1400px", "padding": "0 24px"},
    children=[
        dcc.Store(id="player-df-store"),
        dcc.Store(id="selected-player-id"),
        dcc.Store(id="load-db-trigger"),
        dcc.Store(id="today-data-store"),
        dcc.Store(id="picks-store"),  # fires after a pick is written to DB
        # Sentinel: updated by the slow data-fetch callback so all dcc.Loading
        # components can use target_components={"loading-sentinel": "children"}
        # to track the full loading duration. A real DOM div (not dcc.Store)
        # so data-dash-is-loading propagates reliably.
        html.Div(id="loading-sentinel", style={"display": "none"}),
        dcc.Interval(id="startup-check", interval=500, max_intervals=1),
        dcc.Interval(id="date-init", interval=100, max_intervals=1),

        # ── Header ──────────────────────────────────────────────────────
        dbc.Row([
            dbc.Col(html.H4("🏀 Drive to the Finals",
                            style={"fontWeight": "700", "letterSpacing": "-0.5px", "margin": "0"}),
                    width="auto", className="d-flex align-items-center"),
            dbc.Col(
                html.Span(id="last-updated-text",
                          style={"fontSize": "11px", "color": "#8e8e93"}),
                width="auto", className="d-flex align-items-center ms-3",
            ),
            dbc.Col([
                dbc.Button("Clear Cache", id="clear-cache-btn", color="light", size="sm", className="me-1"),
                html.Span(id="cache-status", className="text-muted small ms-2"),
            ], width="auto", className="d-flex align-items-center ms-auto"),
        ], className="py-2 mb-2 align-items-center",
           style={"borderBottom": "1px solid #d2d2d7"}),

        html.Div(id="db-status-banner"),

        dbc.Tabs(
            id="main-tabs",
            active_tab="tab-today",
            children=[
                dbc.Tab(label="Today's Picks", tab_id="tab-today"),
                dbc.Tab(label="Player Model", tab_id="tab-model"),
                dbc.Tab(label="Manage Picks", tab_id="tab-history"),
            ],
            className="mt-2",
        ),

        # Today tab pane is always in the DOM to avoid race conditions with dynamic layout.
        # Shown/hidden via CSS based on active tab.
        html.Div(id="today-tab-pane", children=_today_layout(), style={"marginTop": "1rem"}),
        html.Div(id="history-tab-pane", children=_history_layout(), style={"display": "none", "marginTop": "1rem"}),

        # Player Model tab renders dynamically (no persistent callback targets needed)
        html.Div(id="tab-content", className="mt-3", style={"display": "none"}),
    ],
)


# ── DB status banner ────────────────────────────────────────────────────────

@app.callback(
    Output("schedule-strip", "children"),
    Input("today-data-store", "data"),
    Input("loading-sentinel", "children"),
)
def render_schedule_strip(store_data, _sentinel):
    if not store_data or not store_data.get("rows"):
        return []
    game_date = store_data.get("game_date")
    df = pd.DataFrame(store_data["rows"])

    def pct_span(p):
        cls = "series-win-high" if p > 0.6 else "series-win-mid" if p > 0.4 else "series-win-low"
        return html.Span(f"{p:.0%}", className=cls)

    seen = set()
    chips = []
    # home rows have is_home=True; use them to build each chip
    home_rows = df[df["is_home"] == True].drop_duplicates("game_id")
    for _, r in home_rows.iterrows():
        gid = r["game_id"]
        if gid in seen:
            continue
        seen.add(gid)
        home = r["Team"]; away = r["Opp"]
        home_p = r["series_win_prob_raw"]
        away_p = round(1 - home_p, 3)
        home_w = r["team_wins"]; home_l = r["team_losses"]
        # away record is the inverse
        away_row = df[(df["game_id"] == gid) & (df["is_home"] == False)]
        if not away_row.empty:
            away_w = away_row.iloc[0]["team_wins"]
            away_l = away_row.iloc[0]["team_losses"]
        else:
            away_w, away_l = home_l, home_w  # fallback

        # Standings include today's result for past dates, so don't add 1.
        is_past = game_date and str(game_date)[:10] < date.today().isoformat()
        game_num = home_w + away_w if is_past else home_w + away_w + 1
        chips.append(html.Div([
            html.Div(f"Game {game_num}",
                     style={"fontSize": "10px", "color": "#aaa", "marginBottom": "2px", "textTransform": "uppercase", "letterSpacing": "0.5px"}),
            html.Div([
                html.Span(home, style={"fontWeight": "700", "fontSize": "15px"}),
                html.Span(f" {home_w}-{home_l}", style={"color": "#6e6e73", "fontSize": "12px", "marginLeft": "3px"}),
                html.Span("  ", style={"margin": "0 2px"}),
                pct_span(home_p),
                html.Span(" @ ", style={"color": "#aaa", "margin": "0 7px", "fontSize": "13px"}),
                html.Span(away, style={"fontWeight": "700", "fontSize": "15px"}),
                html.Span(f" {away_w}-{away_l}", style={"color": "#6e6e73", "fontSize": "12px", "marginLeft": "3px"}),
                html.Span("  ", style={"margin": "0 2px"}),
                pct_span(away_p),
            ]),
        ], className="schedule-chip"))

    return html.Div(chips, style={"display": "flex", "flexWrap": "wrap", "gap": "6px"})


@app.callback(
    Output("game-date-picker", "date"),
    Input("date-init", "n_intervals"),
    prevent_initial_call=False,
)
def init_date_picker(_):
    return date.today().isoformat()


# ── Date strip: render chips & handle navigation ─────────────────────────────

@app.callback(
    Output("date-strip-offset", "data"),
    Input("date-prev-btn", "n_clicks"),
    Input("date-next-btn", "n_clicks"),
    State("date-strip-offset", "data"),
    prevent_initial_call=True,
)
def shift_date_strip(prev_clicks, next_clicks, offset):
    triggered = callback_context.triggered_id
    if triggered == "date-prev-btn":
        return (offset or 0) - 7
    return (offset or 0) + 7


@app.callback(
    Output("date-strip-chips", "children"),
    Input("date-strip-offset", "data"),
    Input("game-date-picker", "date"),
)
def render_date_strip(offset, selected_date):
    from datetime import timedelta
    offset = offset or 0
    today = date.today()
    selected = date.fromisoformat(str(selected_date)[:10]) if selected_date else today

    # Anchor week so that the selected date is always visible
    # Week starts on Monday of the week containing (today + offset days)
    anchor = today + timedelta(days=offset)
    week_start = anchor - timedelta(days=anchor.weekday())  # Monday

    game_dates = set(db_get_known_game_dates())
    today_iso = today.isoformat()
    chips = []
    for i in range(7):
        d = week_start + timedelta(days=i)
        d_iso = d.isoformat()
        is_active = d == selected
        has_game = d_iso in game_dates
        is_past = d_iso < today_iso
        # Dim only past dates with no known games (future dates may not be in DB yet)
        dim = is_past and not has_game
        cls = "date-chip"
        if is_active:
            cls += " active"
        if dim:
            cls += " no-game"
        clickable = not dim
        chips.append(
            html.Div(
                [
                    html.Div(d.strftime("%a").upper(), className="date-chip-day"),
                    html.Div(
                        [
                            html.Span(d.strftime("%b").upper(),
                                      style={"fontSize": "9px", "marginRight": "3px",
                                             "opacity": "0.7"}),
                            html.Span(str(d.day)),
                        ],
                        className="date-chip-num",
                        style={"display": "flex", "alignItems": "baseline",
                               "justifyContent": "center"},
                    ),
                ],
                className=cls,
                id={"type": "date-chip", "date": d_iso},
                n_clicks=0 if clickable else None,
            )
        )
    return chips


@app.callback(
    Output("game-date-picker", "date", allow_duplicate=True),
    Output("date-strip-offset", "data", allow_duplicate=True),
    Input({"type": "date-chip", "date": dash.ALL}, "n_clicks"),
    State({"type": "date-chip", "date": dash.ALL}, "id"),
    State("date-strip-offset", "data"),
    prevent_initial_call=True,
)
def chip_date_click(n_clicks_list, id_list, offset):
    from datetime import timedelta
    if not any(n for n in (n_clicks_list or []) if n):
        return dash.no_update, dash.no_update
    triggered = callback_context.triggered_id
    if not triggered or not isinstance(triggered, dict):
        return dash.no_update, dash.no_update
    clicked_date = triggered["date"]
    # Recentre the strip on the clicked date
    today = date.today()
    d = date.fromisoformat(clicked_date)
    new_offset = (d - today).days
    # Snap offset to nearest week start
    anchor = today + timedelta(days=new_offset)
    new_offset = (anchor - timedelta(days=anchor.weekday()) - (today - timedelta(days=today.weekday()))).days
    return clicked_date, new_offset


@app.callback(
    Output("selected-date-label", "children"),
    Input("game-date-picker", "date"),
)
def update_date_label(game_date):
    if not game_date:
        return ""
    try:
        d = date.fromisoformat(str(game_date)[:10])
        return d.strftime("%A, %B %-d, %Y")
    except Exception:
        return ""


@app.callback(
    Output("date-picker-wrapper", "style"),
    Input("date-cal-btn", "n_clicks"),
    Input("game-date-picker", "date"),
    State("date-picker-wrapper", "style"),
    prevent_initial_call=True,
)
def toggle_calendar(cal_clicks, _date_selected, current_style):
    triggered = callback_context.triggered_id
    # Close when a date is picked
    if triggered == "game-date-picker":
        return {"position": "absolute", "top": "44px", "left": "0",
                "opacity": "0", "pointerEvents": "none", "height": "0",
                "overflow": "visible", "zIndex": "1000"}
    # Toggle on calendar icon click
    is_hidden = not current_style or current_style.get("pointerEvents") == "none"
    if is_hidden:
        return {"position": "absolute", "top": "44px", "left": "0",
                "zIndex": "1000", "background": "#fff"}
    return {"position": "absolute", "top": "44px", "left": "0",
            "opacity": "0", "pointerEvents": "none", "height": "0",
            "overflow": "visible", "zIndex": "1000"}


# After the picker becomes visible, JS clicks its input to open the calendar immediately.
app.clientside_callback(
    """
    function(style) {
        if (!style || style.opacity === '0' || style.pointerEvents === 'none') {
            return window.dash_clientside.no_update;
        }
        setTimeout(function() {
            var wrapper = document.getElementById('date-picker-wrapper');
            if (!wrapper) return;
            var input = wrapper.querySelector('input[type="text"]');
            if (input) { input.focus(); input.click(); }
        }, 30);
        return window.dash_clientside.no_update;
    }
    """,
    Output("date-cal-btn", "title"),
    Input("date-picker-wrapper", "style"),
    prevent_initial_call=True,
)


@app.callback(
    Output("db-status-banner", "children"),
    Input("startup-check", "n_intervals"),
    Input("today-data-store", "data"),
    prevent_initial_call=False,
)
def check_db_status(_, store_data):
    today = date.today().isoformat()
    if db_get_schedule(today):
        return []
    loading = store_data is None
    return dbc.Alert(
        [
            html.Strong("No data for today. "),
            "Run a data refresh to load today's schedule and projections. ",
            html.Span([
                dbc.Spinner(size="sm", color="warning", className="ms-2"),
                html.Span(" Loading…", className="ms-1 small text-muted"),
            ]) if loading else dbc.Button("Load Now", id="load-db-btn", color="warning",
                                          size="sm", className="ms-2"),
            dbc.Spinner(html.Span(id="load-db-status", className="ms-2 small"), size="sm"),
        ],
        id="db-alert",
        color="warning",
        dismissable=True,
        className="mb-2",
    )


@app.callback(
    Output("load-db-status", "children"),
    Output("db-alert", "color"),
    Output("load-db-trigger", "data"),
    Input("load-db-btn", "n_clicks"),
    State("game-date-picker", "date"),
    prevent_initial_call=True,
)
def run_load_db(_, selected_date):
    import subprocess, threading
    load_date = selected_date or date.today().isoformat()
    def _run():
        subprocess.run(
            [sys.executable, "scripts/update_db.py", "--date", load_date],
            cwd=str(Path(__file__).parent.parent),
        )
    threading.Thread(target=_run, daemon=True).start()
    return "Loading in background…", "info", load_date


# ── Tab routing ─────────────────────────────────────────────────────────────

@app.callback(
    Output("today-tab-pane", "style"),
    Output("history-tab-pane", "style"),
    Output("tab-content", "style"),
    Input("main-tabs", "active_tab"),
)
def toggle_tab_visibility(tab):
    hide = {"display": "none"}
    show_mt = {"display": "block", "marginTop": "1rem"}
    if tab == "tab-today":
        return show_mt, hide, hide
    if tab == "tab-history":
        return hide, show_mt, hide
    return hide, hide, show_mt


@app.callback(Output("tab-content", "children"), Input("main-tabs", "active_tab"))
def render_tab(tab):
    if tab == "tab-model":
        return _model_layout()
    return html.Div()


@app.callback(
    Output("history-record-pane", "style"),
    Output("history-commitment-pane", "style"),
    Input("history-subtabs", "active_tab"),
)
def toggle_history_subtabs(subtab):
    show = {"display": "block"}
    hide = {"display": "none"}
    return (
        show if subtab == "history-record" else hide,
        show if subtab == "history-commitment" else hide,
    )


@app.callback(
    Output("subtab-players-pane", "style"),
    Output("subtab-scatter-pane", "style"),
    Output("subtab-compare-pane", "style"),
    Input("today-subtabs", "active_tab"),
)
def toggle_subtabs(subtab):
    show = {"display": "block"}
    hide = {"display": "none"}
    return (
        show if subtab == "subtab-players" else hide,
        show if subtab == "subtab-scatter" else hide,
        show if subtab == "subtab-compare" else hide,
    )


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1: Today's Picks  (layout defined above app.layout for static embedding)
# ═══════════════════════════════════════════════════════════════════════════


_DISPLAY_COLS = [
    "player_id", "Player", "Inj Note", "Signals", "Pos", "Team", "Opp", "Status",
    "Pred", "Our Proj", "DE Proj", "DE Pts", "DE Reb", "DE Ast",
    "FD Proj", "FD Pts", "FD Reb", "FD Ast", "FD Min",
    "RS Avg", "PO Avg", "Ext Proj", "Series Win%",
    "Urgency", "Urgency_Ours", "Urgency_DE", "Urgency_FD",
    "Exp Games", "Picked", "game_id", "series_win_prob_raw", "series_lose_prob_raw",
    "team_wins", "team_losses", "is_home",
]


def _render_table_from_store(store_data, urgency_field):
    """Build the AG Grid from serialized store data. Fast — no DB calls."""
    if store_data is None:
        return html.P("Loading...", className="text-muted mt-3")
    if store_data.get("no_games"):
        gd = store_data.get("game_date") or "this date"
        return html.P(f"No games scheduled for {gd}.", className="text-muted mt-3")
    if not store_data.get("rows"):
        return html.P("No player data — run update_db to load today's data.", className="text-muted mt-3")

    df = pd.DataFrame(store_data["rows"])
    urgency_col = urgency_field or "Urgency"
    if urgency_col in df.columns:
        df = df.assign(Urgency_Display=df[urgency_col])
    elif "Urgency" in df.columns:
        df = df.assign(Urgency_Display=df["Urgency"])

    def _display_status(row):
        if row.get("Picked"):
            return "✓ Picked"
        return row.get("Status") or ""
    df = df.assign(Display_Status=df.apply(_display_status, axis=1))

    col_defs = [
        {"field": "Player", "pinned": "left",
         "tooltipField": "Signals",
         "cellStyle": {"function": "(params.data.Display_Status&&(params.data.Display_Status[0]==='✓'||params.data.Display_Status[0]==='❌')) ? {'fontWeight':'600','color':'#aaa','fontStyle':'italic','textDecoration':'line-through'} : {'fontWeight':'600'}"},
         "valueGetter": {"function": "params.data['Signals'] ? params.data['Player'] + ' ℹ' : params.data['Player']"}},
        {"field": "Display_Status", "headerName": "Status",
         "cellStyle": {"function": "({'color': params.value&&params.value[0]==='✓' ? '#6e6e73' : params.value&&params.value[0]==='❌' ? '#dc2626' : '#e67e22', 'fontSize':'12px', 'fontWeight':'600'})"}},
        {"field": "Pos"},
        {"field": "Team"},
        {"field": "Opp"},
        {"field": "Urgency_Display", "headerName": "Urgency", "width": 85,
         "sort": "desc",
         "cellStyle": {"function": "(params.data.Display_Status&&(params.data.Display_Status[0]==='✓'||params.data.Display_Status[0]==='❌')) ? {'fontWeight':'700','color':'#aaa','fontStyle':'italic','textDecoration':'line-through'} : {'fontWeight':'700','color': params.value > 25 ? '#16a34a' : params.value > 12 ? '#ca8a04' : '#dc2626'}"}},
        {"headerName": "Score Projection", "children": [
            {"field": "Pred", "headerName": "Pred"},
            {"field": "Our Proj", "headerName": "Ours"},
            {"field": "DE Proj",  "headerName": "DE"},
            {"field": "FD Proj",  "headerName": "FD"},
            {"field": "RS Avg",   "headerName": "RS Avg"},
            {"field": "PO Avg",   "headerName": "PO Avg"},
        ]},
        {"headerName": "DE Split", "children": [
            {"field": "DE Pts", "headerName": "Pts", "width": 58},
            {"field": "DE Reb", "headerName": "Reb", "width": 58},
            {"field": "DE Ast", "headerName": "Ast", "width": 58},
        ]},
        {"headerName": "FD Split", "children": [
            {"field": "FD Pts", "headerName": "Pts", "width": 58},
            {"field": "FD Reb", "headerName": "Reb", "width": 58},
            {"field": "FD Ast", "headerName": "Ast", "width": 58},
            {"field": "FD Min", "headerName": "Min", "width": 58},
        ]},
        {"headerName": "Series", "children": [
            {"field": "Series Win%", "headerName": "Win%", "width": 68,
             "valueFormatter": {"function": "params.value != null ? (params.value*100).toFixed(0)+'%' : ''"}},
            {"field": "Exp Games", "headerName": "Exp G", "width": 70},
        ]},
    ]

    return dag.AgGrid(
        id="today-ag-grid",
        rowData=df.to_dict("records"),
        columnDefs=col_defs,
        defaultColDef={
            "sortable": True, "filter": True, "resizable": True,
            "minWidth": 40, "cellDataType": False,
            "suppressHeaderMenuButton": True,
            "cellStyle": {"function": "(params.data.Display_Status&&(params.data.Display_Status[0]==='✓'||params.data.Display_Status[0]==='❌')) ? {'color':'#aaa','fontStyle':'italic','textDecoration':'line-through'} : {}"},
        },
        columnSize="autoSize",
        dashGridOptions={
            "rowHeight": 34,
            "headerHeight": 38,
            "groupHeaderHeight": 28,
            "suppressCellFocus": True,
            "rowSelection": "multiple",
            "rowMultiSelectWithClick": True,
            "tooltipShowDelay": 200,
            "tooltipHideDelay": 6000,
            "autoSizeStrategy": {"type": "fitCellContents", "skipHeader": False},
            "onGridSizeChanged": {"function": "params.api.autoSizeAllColumns(false)"},
        },
        style={"height": "500px"},
        className="ag-theme-alpine",
    )


@app.callback(
    Output("today-table-container", "children"),
    Output("today-data-store", "data"),
    Output("loading-sentinel", "children"),
    Input("main-tabs", "active_tab"),
    Input("clear-cache-btn", "n_clicks"),
    Input("game-date-picker", "date"),
    Input("load-db-trigger", "data"),
    State("urgency-model-select", "value"),
    prevent_initial_call=False,
)
def load_and_render_today(tab, _, game_date, _load_trigger, urgency_field):
    if tab != "tab-today" and tab is not None:
        return dash.no_update, None, dash.no_update
    try:
        df = build_todays_player_df(game_date=game_date)
    except Exception as e:
        print(f"[dashboard] build_todays_player_df error: {e}", flush=True)
        import traceback; traceback.print_exc()
        store = {"rows": [], "error": str(e)}
        return html.P(f"Error loading data: {e}", className="text-danger mt-3"), store, game_date
    if df.empty:
        print("[dashboard] df is empty", flush=True)
        store = {"rows": [], "game_date": game_date, "no_games": True}
        return _render_table_from_store(store, urgency_field), store, game_date
    print(f"[dashboard] built {len(df)} rows", flush=True)
    available = [c for c in _DISPLAY_COLS if c in df.columns]
    store = {"rows": df[available].to_dict("records"), "game_date": game_date}

    # Pre-warm cache for adjacent game dates in the background.
    effective_date = game_date or date.today().isoformat()
    import threading
    threading.Thread(target=_prefetch_adjacent_dates, args=(effective_date,), daemon=True).start()

    return _render_table_from_store(store, urgency_field), store, game_date


def _prefetch_adjacent_dates(current_date: str) -> None:
    known = db_get_known_game_dates()
    if not known or current_date not in known:
        return
    idx = known.index(current_date)
    adjacent = []
    if idx > 0:
        adjacent.append(known[idx - 1])
    if idx < len(known) - 1:
        adjacent.append(known[idx + 1])
    for d in adjacent:
        cache_key = f"{d}_1"
        if cache_key in _df_cache:
            _, ts = _df_cache[cache_key]
            if time.time() - ts < 3600:
                continue
        try:
            print(f"[prefetch] warming cache for {d}", flush=True)
            build_todays_player_df(game_date=d)
        except Exception as e:
            print(f"[prefetch] {d} failed: {e}", flush=True)


@app.callback(
    Output("pick-dropdown", "options"),
    Input("main-tabs", "active_tab"),
    Input("today-data-store", "data"),
)
def populate_pick_dropdown(tab, _store):
    all_playoff = _get_all_playoff_players()
    used_ids = get_used_player_ids()
    return [
        {"label": f"{p['player_name']} ({p['team_abbr']})", "value": p["player_id"]}
        for p in all_playoff if p["player_id"] not in used_ids
    ]


@app.callback(
    Output("today-table-container", "children", allow_duplicate=True),
    Input("urgency-model-select", "value"),
    State("today-data-store", "data"),
    prevent_initial_call=True,
)
def rerender_for_urgency(urgency_field, store_data):
    return _render_table_from_store(store_data, urgency_field)


@app.callback(
    Output("compare-player-dropdown", "options"),
    Input("today-data-store", "data"),
)
def populate_compare_dropdown(store_data):
    players = _get_all_playoff_players()
    return [
        {"label": f"{p['player_name']} ({p['team_abbr']})", "value": p["player_id"]}
        for p in players
    ]


@app.callback(
    Output("compare-chart", "figure"),
    Input("compare-player-dropdown", "value"),
    Input("compare-metric-select", "value"),
)
def update_compare_chart(player_ids, metric):
    empty = go.Figure()
    empty.update_layout(
        template="plotly_white", height=380,
        annotations=[dict(text="Select players above to compare", showarrow=False,
                          font=dict(color="#999"), xref="paper", yref="paper", x=0.5, y=0.5)],
    )
    if not player_ids or len(player_ids) < 1:
        return empty

    from src.data_fetcher import CURRENT_SEASON
    metric = metric or "PRA"
    col_map = {"PRA": "PRA", "PTS": "PTS", "REB": "REB", "AST": "AST", "MIN": "MIN"}
    col = col_map.get(metric, "PRA")

    COLORS = ["#0071e3", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6"]
    fig = go.Figure()

    from nba_api.stats.static import players as nba_players_static
    pid_name_map = {p["id"]: p["full_name"] for p in nba_players_static.get_players()}

    for i, pid in enumerate(player_ids[:4]):
        logs, _ = db_get_game_logs(pid, CURRENT_SEASON)
        if logs.empty or col not in logs.columns:
            continue

        color = COLORS[i % len(COLORS)]
        pname = pid_name_map.get(pid, str(pid))
        logs = logs.sort_values("GAME_DATE")

        for season_type, symbol, marker_size in [("Regular Season", "circle", 7), ("Playoffs", "star", 11)]:
            seg = logs[logs["SEASON_TYPE"] == season_type].copy()
            if seg.empty:
                continue

            # Insert None rows to break the line at gaps > 14 days.
            seg = seg.sort_values("GAME_DATE").reset_index(drop=True)
            gaps = seg["GAME_DATE"].diff().dt.days > 14
            gap_indices = seg.index[gaps].tolist()
            x_vals, y_vals = list(seg["GAME_DATE"]), list(seg[col])
            for offset, idx in enumerate(gap_indices):
                insert_at = idx + offset
                x_vals.insert(insert_at, None)
                y_vals.insert(insert_at, None)

            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode="markers+lines",
                name=f"{pname} ({season_type[:2]})",
                legendgroup=str(pid),
                marker=dict(color=color, size=marker_size, symbol=symbol,
                            line=dict(color="#111", width=0.5)),
                line=dict(color=color, width=1.5,
                          dash="dot" if season_type == "Regular Season" else "solid"),
                hovertemplate=f"<b>{pname}</b><br>%{{x|%b %d}}<br>{metric}: <b>%{{y}}</b><br>{season_type}<extra></extra>",
                connectgaps=False,
            ))

    if not fig.data:
        return empty

    fig.update_layout(
        template="plotly_white",
        height=380,
        xaxis_title="Game Date",
        yaxis_title=metric,
        legend=dict(orientation="h", y=-0.2, font=dict(size=11)),
        margin=dict(l=40, r=20, t=20, b=60),
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
    )
    return fig


@app.callback(
    Output("pick-status", "children"),
    Output("picks-store", "data"),
    Input("record-pick-btn", "n_clicks"),
    State("pick-dropdown", "value"),
    State("today-data-store", "data"),
    prevent_initial_call=True,
)
def record_pick_callback(n_clicks, player_ids, store_data):
    no_trigger = dash.no_update
    if not player_ids:
        return dbc.Alert("Select at least one player.", color="warning", duration=3000), no_trigger
    if isinstance(player_ids, int):
        player_ids = [player_ids]
    rows = (store_data or {}).get("rows", [])
    all_playoff = {p["player_id"]: p for p in _get_all_playoff_players()}
    recorded, errors = [], []
    for pid in player_ids:
        row = next((r for r in rows if r["player_id"] == pid), None)
        if not row:
            p = all_playoff.get(pid)
            if not p:
                errors.append(f"Player {pid} not found in any playoff roster.")
                continue
            row = {
                "Player": p["player_name"],
                "Team": p["team_abbr"],
                "Opp": "",
                "Our Proj": 0.0,
                "Ext Proj": None,
                "game_id": "",
            }
        try:
            record_pick(
                player_id=pid,
                player_name=row["Player"],
                team_abbr=row["Team"],
                opponent_abbr=row.get("Opp", ""),
                projected_pra=row.get("Our Proj") or 0.0,
                game_id=row.get("game_id", ""),
                external_projected_pra=row["Ext Proj"] if row.get("Ext Proj") not in ("", None) else None,
            )
            recorded.append(row["Player"])
        except ValueError as e:
            errors.append(str(e))
    msgs = []
    if recorded:
        msgs.append(dbc.Alert(f"✓ Picked: {', '.join(recorded)}", color="success", duration=5000))
        # Bust the in-memory df cache so today's table reflects the new pick
        _df_cache.clear()
    if errors:
        msgs.append(dbc.Alert(" | ".join(errors), color="danger", duration=6000))
    picks_trigger = n_clicks if recorded else no_trigger
    return msgs or dbc.Alert("Nothing recorded.", color="warning", duration=3000), picks_trigger


@app.callback(
    Output("remove-pick-status", "children"),
    Output("picks-store", "data", allow_duplicate=True),
    Input("remove-pick-btn", "n_clicks"),
    State("remove-pick-dropdown", "value"),
    prevent_initial_call=True,
)
def remove_pick_callback(n_clicks, player_id):
    if not player_id:
        return dbc.Alert("Select a pick to remove.", color="warning", duration=3000), dash.no_update
    try:
        name = remove_pick(int(player_id))
        _df_cache.clear()
        return dbc.Alert(f"Removed: {name}", color="success", duration=4000), n_clicks
    except ValueError as e:
        return dbc.Alert(str(e), color="danger", duration=5000), dash.no_update


@app.callback(
    Output("today-data-store", "data", allow_duplicate=True),
    Output("today-table-container", "children", allow_duplicate=True),
    Input("picks-store", "data"),
    State("today-data-store", "data"),
    State("urgency-model-select", "value"),
    prevent_initial_call=True,
)
def patch_and_rerender_picks(_, store_data, urgency_field):
    """Update Picked flags and re-render table immediately after a pick change."""
    if not store_data or not store_data.get("rows"):
        return dash.no_update, dash.no_update
    used_ids = set(get_used_player_ids())
    rows = store_data["rows"]
    for row in rows:
        row["Picked"] = row["player_id"] in used_ids
    updated = {**store_data, "rows": rows}
    return updated, _render_table_from_store(updated, urgency_field)


@app.callback(
    Output("scatter-chart", "figure"),
    Input("today-data-store", "data"),
    Input("loading-sentinel", "children"),
)
def update_scatter_chart(store_data, _sentinel):
    empty = go.Figure()
    empty.update_layout(template="plotly_white", height=480)
    if not store_data or not store_data.get("rows"):
        return empty

    df = pd.DataFrame(store_data["rows"])
    if "series_win_prob_raw" not in df.columns or "Our Proj" not in df.columns:
        return empty

    # Fallback palette for unknown teams
    fallback = [
        "#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
        "#911eb4", "#42d4f4", "#f032e6", "#bfef45", "#fabed4",
        "#469990", "#dcbeff", "#9A6324", "#fffac8", "#800000", "#aaffc3",
    ]
    teams = sorted(df["Team"].unique())
    team_colors = {
        t: NBA_TEAM_COLORS.get(t, fallback[i % len(fallback)])
        for i, t in enumerate(teams)
    }

    # Top-N by urgency get last-name text labels so chart is readable without hovering
    top_n = df.nlargest(8, "Urgency")["player_id"].tolist()

    fig = go.Figure()

    for team in teams:
        tdf = df[df["Team"] == team].copy()
        color = team_colors[team]

        avail = tdf[~tdf["Picked"]]
        used = tdf[tdf["Picked"]]

        def _last(name):
            parts = name.split()
            return parts[-1] if len(parts) > 1 else name

        # Split available into active vs Out
        is_out = avail["Status"].str.startswith("❌", na=False)
        out_plot = avail[is_out & (avail["Pred"] != 0) & (avail["Pred"] != "")].copy()
        avail_plot = avail[~is_out & (avail["Pred"] != 0) & (avail["Pred"] != "")].copy()

        if not out_plot.empty:
            fig.add_trace(go.Scatter(
                x=out_plot["series_lose_prob_raw"],
                y=out_plot["Pred"].replace("", 0),
                mode="markers",
                name=f"{team} (out)",
                legendgroup=team,
                showlegend=False,
                marker=dict(color=color, size=10, opacity=0.3, symbol="x",
                            line=dict(color=color, width=2)),
                hovertemplate=(
                    "<b>%{customdata[0]}</b> — Out<br>"
                    "%{customdata[1]} vs %{customdata[2]}<br>"
                    "<extra></extra>"
                ),
                customdata=out_plot[["Player", "Team", "Opp"]].values,
            ))

        if not avail_plot.empty:
            labels = [
                _last(r["Player"]) if r["player_id"] in top_n else ""
                for _, r in avail_plot.iterrows()
            ]
            fig.add_trace(go.Scatter(
                x=avail_plot["series_lose_prob_raw"],
                y=avail_plot["Pred"],
                mode="markers+text",
                name=team,
                legendgroup=team,
                marker=dict(color=color, size=14, opacity=0.9,
                            line=dict(color="#111", width=1)),
                text=labels,
                textposition="top center",
                textfont=dict(size=12, color=color),
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "%{customdata[1]} vs %{customdata[2]}<br>"
                    "Pred: <b>%{y:.1f}</b>  (DE: %{customdata[4]}, Ours: %{customdata[5]})<br>"
                    "Series Elim%: <b>%{x:.0%}</b><br>"
                    "Urgency: <b>%{customdata[3]}</b><br>"
                    "<extra></extra>"
                ),
                customdata=avail_plot[["Player", "Team", "Opp", "Urgency", "DE Proj", "Our Proj"]].values,
            ))

        if not used.empty:
            fig.add_trace(go.Scatter(
                x=used["series_lose_prob_raw"],
                y=used["Pred"].replace("", 0),
                mode="markers",
                name=f"{team} (used)",
                legendgroup=team,
                showlegend=False,
                marker=dict(color=color, size=9, opacity=0.25, symbol="x",
                            line=dict(color=color, width=2)),
                hovertemplate=(
                    "<b>%{customdata[0]}</b> — already picked<br>"
                    "%{customdata[1]} vs %{customdata[2]}<br>"
                    "Pred: %{y:.1f} | Series Elim%: %{x:.0%}<br>"
                    "<extra></extra>"
                ),
                customdata=used[["Player", "Team", "Opp", "Urgency"]].values,
            ))

    # Iso-urgency curves: urgency = Pred × elim_prob → Pred = U / elim_prob
    _x = np.linspace(0.04, 1.0, 200)
    for u_level, u_color, u_label in [
        (5,  "#dc2626", "Urgency 5"),
        (12, "#ca8a04", "Urgency 12"),
        (25, "#16a34a", "Urgency 25"),
    ]:
        _y = u_level / _x
        _mask = _y <= 58  # clip to plot area
        fig.add_trace(go.Scatter(
            x=_x[_mask], y=_y[_mask],
            mode="lines",
            line=dict(color=u_color, width=1, dash="dot"),
            opacity=0.35,
            showlegend=False,
            hoverinfo="skip",
            name=u_label,
        ))
        # Label at right edge of curve where it's still in the plot area
        _xi = _x[_mask][-1]
        _yi = min(u_level / _xi, 55)
        fig.add_annotation(
            x=_xi, y=_yi, text=str(u_level),
            showarrow=False, font=dict(size=9, color=u_color), opacity=0.6,
            xanchor="left",
        )

    # Vertical reference line at 50%
    fig.add_vline(x=0.5, line_dash="dash", line_color="#555", line_width=1)

    # Quadrant annotations — right = high elim risk = pick now
    fig.add_annotation(x=0.85, y=0.97, xref="paper", yref="paper",
                       text="Pick now →", showarrow=False,
                       font=dict(color="#e74c3c", size=11), opacity=0.7)
    fig.add_annotation(x=0.12, y=0.97, xref="paper", yref="paper",
                       text="← Save for later", showarrow=False,
                       font=dict(color="#2ecc71", size=11), opacity=0.7)

    fig.update_layout(
        template="plotly_white",
        height=420,
        xaxis=dict(
            title=dict(text="Series Elimination Probability", font=dict(size=13)),
            tickformat=".0%",
            tickfont=dict(size=12),
            range=[-0.02, 1.02],
            gridcolor="#f0f0f0",
        ),
        yaxis=dict(
            title=dict(text="Pred PRA", font=dict(size=13)),
            tickfont=dict(size=12),
            gridcolor="#f0f0f0",
        ),
        legend=dict(
            orientation="v",
            x=1.01, y=1,
            font=dict(size=12),
            itemsizing="constant",
        ),
        margin=dict(l=50, r=140, t=20, b=50),
        hovermode="closest",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
    )

    return fig


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2: Player Model Inspector
# ═══════════════════════════════════════════════════════════════════════════

def _model_layout():
    return html.Div([
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(id="model-player-dropdown", placeholder="Search for a player...",
                             style={"color": "#000"}, clearable=True),
                dbc.InputGroup([
                    dbc.InputGroupText("Decay rate"),
                    dbc.Input(id="decay-rate-input", type="number", value=0.82,
                              min=0.70, max=1.00, step=0.02, debounce=True),
                ], className="mt-2", style={"maxWidth": "220px"}),
            ], width=4),
            dbc.Col([
                html.Div(id="model-summary-card"),
            ], width=8),
        ], className="mb-3"),

        dcc.Loading(type="circle", color="#0071e3", delay_show=0,
                    target_components={"pra-history-chart": "figure", "decay-weights-chart": "figure",
                                       "adjustment-waterfall": "figure", "model-summary-card": "children"},
                    children=html.Div([
            dbc.Row([
                dbc.Col(dcc.Graph(id="pra-history-chart", config={"displayModeBar": False}), width=6),
                dbc.Col(dcc.Graph(id="decay-weights-chart", config={"displayModeBar": False}), width=6),
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id="adjustment-waterfall", config={"displayModeBar": False}), width=6),
            ]),
        ])),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([
                html.P("Model Fit vs Decay Rate", className="fw-semibold mb-1"),
                dcc.Graph(id="model-fit-chart", config={"displayModeBar": False}, style={"height": "280px"}),
            ])), width=8),
        ], className="mt-3"),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([
                html.P("EWMA MAE Grid — Decay Rate × Window (playoff eval)", className="fw-semibold mb-1"),
                dcc.Graph(id="decay-distribution-chart", config={"displayModeBar": False}, style={"height": "280px"}),
            ])), width=8),
        ], className="mt-3"),

        # Search store to populate player dropdown
        dcc.Store(id="all-players-store"),
    ])


@app.callback(
    Output("all-players-store", "data"),
    Input("main-tabs", "active_tab"),
)
def load_all_players(tab):
    if tab != "tab-model":
        return []
    players = _get_all_playoff_players()
    return [{"label": p["player_name"], "value": p["player_id"]} for p in players]


@app.callback(
    Output("model-player-dropdown", "options"),
    Input("all-players-store", "data"),
)
def populate_model_dropdown(players):
    return players or []


@app.callback(
    Output("pra-history-chart", "figure"),
    Output("decay-weights-chart", "figure"),
    Output("adjustment-waterfall", "figure"),
    Output("model-summary-card", "children"),
    Output("model-fit-chart", "figure"),
    Input("model-player-dropdown", "value"),
    Input("decay-rate-input", "value"),
    Input("main-tabs", "active_tab"),
)
def update_model_charts(player_id, decay_rate, tab):
    current_round = 1
    empty = go.Figure()
    empty.update_layout(template="plotly_white", height=300,
                        annotations=[dict(text="Select a player above", showarrow=False,
                                          font=dict(color="#999"), xref="paper", yref="paper",
                                          x=0.5, y=0.5)])

    if not player_id or tab != "tab-model":
        return empty, empty, empty, "", empty

    decay_rate = decay_rate if decay_rate is not None else 0.82
    games = get_todays_games()
    def_ratings = get_team_defense_ratings()
    series_standings = get_series_standings()
    per_game_probs = fetch_per_game_win_probs()
    series_win_probs = fetch_series_win_probs(series_standings, per_game_probs)

    # Find which game this player is in
    player_team_id = None
    opp_team_id = None
    team_abbr = None
    for game in games:
        for team_id, opp_id, abbr, _ in [
            (game["home_team_id"], game["away_team_id"],
             game["home_team_abbr"], game["away_team_abbr"]),
            (game["away_team_id"], game["home_team_id"],
             game["away_team_abbr"], game["home_team_abbr"]),
        ]:
            roster = get_active_roster(team_id)
            if any(p["player_id"] == player_id for p in roster):
                player_team_id = team_id
                opp_team_id = opp_id
                team_abbr = abbr
                break
        if player_team_id:
            break

    all_logs_df = get_player_game_logs_365(player_id)
    logs = all_logs_df
    if all_logs_df.empty:
        return empty, empty, empty, html.P("No game log data found.", className="text-muted"), empty

    series_record = {"wins": 0, "losses": 0}
    per_game_win_prob = 0.5
    if team_abbr:
        series_record = get_series_record_for_team(team_abbr, series_standings, TEAM_MAP)
        per_game_win_prob = per_game_probs.get(team_abbr, 0.5)  # single-game prob for Markov chain

    proj = project_player(
        player_id=player_id,
        opponent_team_id=opp_team_id or 0,
        game_logs=logs,
        def_ratings=def_ratings,
        series_record=series_record,
        per_game_win_prob=per_game_win_prob,
        decay_rate=decay_rate,
        current_round=int(current_round or 1),
        include_rolling=True,
    )

    weights = proj["decay_weights"]
    rolling_preds = proj["rolling_predictions"]

    # Use full history for scatter; model-window dates for prediction trace
    model_dates = proj["game_dates"]       # newest-first, 30-game window
    rolling_preds_chron = list(reversed(rolling_preds))
    model_dates_chron = list(reversed(model_dates))

    # Full-history scatter (all stored games, newest-first → reverse for chrono)
    chart_logs = all_logs_df if not all_logs_df.empty else logs
    chart_logs = chart_logs.sort_values("GAME_DATE").reset_index(drop=True)
    all_dates = chart_logs["GAME_DATE"].tolist()
    all_pra = chart_logs["PRA"].tolist()
    all_types = chart_logs["SEASON_TYPE"].tolist()

    playoff_dates = [d for d, t in zip(all_dates, all_types) if t == "Playoffs"]
    playoff_pra = [v for v, t in zip(all_pra, all_types) if t == "Playoffs"]
    reg_dates = [d for d, t in zip(all_dates, all_types) if t != "Playoffs"]
    reg_pra = [v for v, t in zip(all_pra, all_types) if t != "Playoffs"]

    # Full-season rolling prediction trace using all available history
    from src.projections import compute_rolling_predictions as _crp
    _full_newest_first = (all_logs_df if not all_logs_df.empty else logs)
    full_rolling = _crp(_full_newest_first, decay_rate)
    full_dates_chron = list(reversed(_full_newest_first["GAME_DATE"].tolist()))
    full_preds_chron = list(reversed(full_rolling))
    valid_pred_dates = [d for d, p in zip(full_dates_chron, full_preds_chron) if p == p]
    valid_preds = [p for p in full_preds_chron if p == p]

    pra_fig = go.Figure()
    if reg_dates:
        pra_fig.add_trace(go.Scatter(
            x=reg_dates, y=reg_pra, mode="markers",
            name="Actual (Reg Season)", marker=dict(color="#4e9af1", size=8),
        ))
    if playoff_dates:
        pra_fig.add_trace(go.Scatter(
            x=playoff_dates, y=playoff_pra, mode="markers",
            name="Actual (Playoffs)", marker=dict(color="#f39c12", size=10, symbol="star"),
        ))
    if valid_pred_dates:
        pra_fig.add_trace(go.Scatter(
            x=valid_pred_dates, y=valid_preds, mode="lines",
            name="Model prediction (before game)",
            line=dict(color="#2ecc71", dash="dot", width=2),
        ))
    # Today's projection as a single point beyond the last game date
    if model_dates_chron:
        pra_fig.add_trace(go.Scatter(
            x=[date.today()], y=[proj["projected_pra"]], mode="markers+text",
            name=f"Today's proj ({proj['projected_pra']})",
            marker=dict(color="#2ecc71", size=14, symbol="diamond"),
            text=[str(proj["projected_pra"])], textposition="top center",
            textfont=dict(color="#2ecc71"),
        ))
    pra_fig.update_layout(
        template="plotly_white", title="PRA Per Game + Model Prediction Trace",
        xaxis_title="Date", yaxis_title="PRA",
        height=340, legend=dict(orientation="h", y=-0.2),
        plot_bgcolor="white", paper_bgcolor="white",
    )

    # ── Chart 2: Decay weights ───────────────────────────────────────────
    date_labels = [str(d)[:10] if hasattr(d, '__str__') else d for d in model_dates]
    weight_colors = ["#f39c12" if t == "Playoffs" else "#4e9af1"
                     for t in proj["per_game_season_type"]]
    decay_fig = go.Figure(go.Bar(
        x=date_labels, y=weights,
        marker_color=weight_colors,
        text=[f"{w:.3f}" for w in weights],
        textposition="outside",
    ))
    decay_fig.update_layout(
        template="plotly_white", title="Exponential Decay Weights per Game",
        xaxis_title="Game Date", yaxis_title="Weight",
        height=320, xaxis=dict(tickangle=-45),
        plot_bgcolor="white", paper_bgcolor="white",
    )

    # ── Chart 3: Adjustment waterfall ───────────────────────────────────
    opp_adj = proj["after_opponent_adj"] - proj["base_pra"]
    spread_adj = proj["projected_pra"] - proj["after_opponent_adj"]

    waterfall_fig = go.Figure(go.Waterfall(
        orientation="v",
        measure=["absolute", "relative", "relative", "total"],
        x=["Base PRA", "Opp Defense Adj", "Spread Adj", "Final Projection"],
        y=[proj["base_pra"], opp_adj, spread_adj, 0],
        text=[
            f"{proj['base_pra']:.1f}",
            f"{opp_adj:+.1f} (×{proj['opponent_adj_factor']:.2f})",
            f"{spread_adj:+.1f} (×{proj['spread_adj_factor']:.2f})",
            f"{proj['projected_pra']:.1f}",
        ],
        textposition="outside",
        connector=dict(line=dict(color="#555")),
        increasing=dict(marker=dict(color="#2ecc71")),
        decreasing=dict(marker=dict(color="#e74c3c")),
        totals=dict(marker=dict(color="#f39c12")),
    ))
    waterfall_fig.update_layout(
        template="plotly_white", title="Projection Adjustment Breakdown",
        height=320, yaxis_title="PRA",
        plot_bgcolor="white", paper_bgcolor="white",
    )

    # ── Summary card ────────────────────────────────────────────────────
    summary = dbc.Card(dbc.CardBody([
        dbc.Row([
            dbc.Col([html.H4(f"{proj['projected_pra']}", className="mb-0"),
                     html.Small("Projected PRA", className="text-muted")], width=3),
            dbc.Col([html.H4(f"{proj['urgency']}", className="mb-0"),
                     html.Small("Urgency", className="text-muted")], width=3),
            dbc.Col([html.H4(f"{proj['expected_future_games']:.1f}", className="mb-0"),
                     html.Small("Expected Games Left", className="text-muted")], width=3),
            dbc.Col([html.H4(f"{per_game_win_prob:.0%}", className="mb-0"),
                     html.Small("Per-Game Win Prob", className="text-muted")], width=3),
        ]),
    ]), className="mt-2")

    # ── Chart 4: Model Fit vs Decay Rate ────────────────────────────────
    _decay_sweep = [round(d, 2) for d in list(np.arange(0.70, 1.01, 0.02))]
    _fit_df = all_logs_df if not all_logs_df.empty else logs
    _fit_maes = []
    for _d in _decay_sweep:
        _preds = _crp(_fit_df, _d)
        _pairs = [(a, p) for a, p in zip(_fit_df["PRA"].tolist(), _preds) if p == p]
        _fit_maes.append(float(np.mean([abs(a - p) for a, p in _pairs])) if len(_pairs) >= 3 else None)

    _valid_decay = [d for d, m in zip(_decay_sweep, _fit_maes) if m is not None]
    _valid_mae = [m for m in _fit_maes if m is not None]

    fit_fig = go.Figure()
    fit_fig.add_trace(go.Scatter(
        x=_valid_decay, y=_valid_mae, mode="lines+markers",
        line=dict(color="#4e9af1", width=2), marker=dict(size=6),
        name="MAE",
    ))
    # Vertical line at current decay_rate
    if _valid_mae:
        fit_fig.add_vline(x=decay_rate, line=dict(color="#e74c3c", dash="dash", width=2),
                          annotation_text=f"current ({decay_rate})", annotation_position="top right")
    # Vertical line at player's optimal from JSON if available
    _opt_json_path = Path(__file__).parent.parent / "data" / "optimal_decay.json"
    if _opt_json_path.exists():
        import json as _json
        _opt_data = _json.loads(_opt_json_path.read_text())
        _player_opt = next((p["optimal_decay"] for p in _opt_data.get("players", [])
                            if p["player_id"] == player_id), None)
        if _player_opt is not None:
            fit_fig.add_vline(x=_player_opt, line=dict(color="#2ecc71", dash="dot", width=2),
                              annotation_text=f"optimal ({_player_opt})", annotation_position="top left")
    fit_fig.update_layout(
        template="plotly_white", xaxis_title="Decay Rate", yaxis_title="MAE (PRA)",
        height=260, margin=dict(t=20, b=40),
        plot_bgcolor="white", paper_bgcolor="white",
    )

    return pra_fig, decay_fig, waterfall_fig, summary, fit_fig


@app.callback(
    Output("decay-distribution-chart", "figure"),
    Input("main-tabs", "active_tab"),
)
def update_decay_distribution(tab):
    no_data_msg = "Run scripts/optimize_ewma.py to generate data."
    empty = go.Figure()
    empty.update_layout(template="plotly_white", height=300, margin=dict(t=20, b=40),
                        annotations=[dict(text=no_data_msg, showarrow=False,
                                          font=dict(color="#999"), xref="paper", yref="paper",
                                          x=0.5, y=0.5)])
    if tab != "tab-model":
        return empty
    opt_path = Path(__file__).parent.parent / "data" / "optimal_ewma.json"
    if not opt_path.exists():
        return empty
    opt = json.loads(opt_path.read_text())
    grid = opt.get("grid", {})
    decay_vals = grid.get("decay_values", [])
    window_labels = grid.get("window_labels", [])
    mean_mae = grid.get("mean_mae", [])
    if not decay_vals or not mean_mae:
        return empty

    # Build z matrix (decay × window), replace None with NaN
    z = [[v if v is not None else float("nan") for v in row] for row in mean_mae]
    # Round for display
    z_text = [[f"{v:.3f}" if not np.isnan(v) else "" for v in row] for row in z]

    # Find global min for annotation
    arr = np.array(z)
    best_idx = np.unravel_index(np.nanargmin(arr), arr.shape)
    proposed_decay = opt.get("proposed_decay")
    proposed_window = opt.get("proposed_window")

    fig = go.Figure(go.Heatmap(
        z=z,
        x=window_labels,
        y=[str(d) for d in decay_vals],
        text=z_text,
        texttemplate="%{text}",
        colorscale="RdYlGn_r",
        reversescale=False,
        colorbar=dict(title="MAE", thickness=12, len=0.8),
        hovertemplate="decay=%{y}  window=%{x}<br>MAE=%{z:.3f}<extra></extra>",
    ))
    # Mark the optimal cell
    fig.add_trace(go.Scatter(
        x=[window_labels[best_idx[1]]],
        y=[str(decay_vals[best_idx[0]])],
        mode="markers",
        marker=dict(symbol="star", size=14, color="#ffffff", line=dict(color="#333", width=1.5)),
        name=f"optimal ({proposed_decay}, w={proposed_window})",
        showlegend=True,
    ))
    n = opt.get("n_player_seasons", "?")
    seasons = ", ".join(opt.get("seasons_evaluated", []))
    fig.update_layout(
        template="plotly_white",
        xaxis_title="Window (# games)",
        yaxis_title="Decay Rate",
        height=360,
        margin=dict(t=30, b=50),
        legend=dict(orientation="h", y=-0.18),
        annotations=[dict(
            text=f"{n} player-seasons  |  {seasons}",
            xref="paper", yref="paper", x=0, y=1.04, showarrow=False,
            font=dict(size=10, color="#888"), align="left",
        )],
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3: Pick History
# ═══════════════════════════════════════════════════════════════════════════

@app.callback(
    Output("history-table-container", "children"),
    Output("actual-player-dropdown", "options"),
    Output("remove-pick-dropdown", "options"),
    Input("main-tabs", "active_tab"),
    Input("update-actual-btn", "n_clicks"),
    Input("picks-store", "data"),
)
def render_history(tab, _update, _picks):
    if tab != "tab-history":
        return html.Div(), [], []

    history = get_pick_history()
    if not history:
        return html.P("No picks recorded yet.", className="text-muted"), [], []

    rows = []
    for p in history:
        actual = p["actual_pra"]
        proj = p["projected_pra"]
        delta = round(actual - proj, 1) if actual is not None else None
        rows.append({
            "Date": p["pick_date"],
            "Player": p["player_name"],
            "Team": p["team_abbr"],
            "Opp": p["opponent_abbr"],
            "Our Proj": proj,
            "Ext Proj": p.get("external_projected_pra", ""),
            "Actual": actual if actual is not None else "—",
            "Delta": delta if delta is not None else "—",
            "_delta_raw": delta,
            "_player_id": p["player_id"],
            "_date": p["pick_date"],
        })

    df = pd.DataFrame(rows)

    history_col_defs = [
        {"field": "Date", "width": 110},
        {"field": "Player", "width": 160, "cellStyle": {"fontWeight": "500"}},
        {"field": "Team", "width": 70},
        {"field": "Opp", "width": 70},
        {"field": "Our Proj", "width": 95},
        {"field": "Ext Proj", "width": 90},
        {"field": "Actual", "width": 85},
        {"field": "Delta", "width": 80, "cellStyle": {
            "function": "{'color': params.value > 0 ? '#16a34a' : params.value < 0 ? '#dc2626' : '#666', 'fontWeight':'600'}"
        }},
    ]

    table = dag.AgGrid(
        rowData=df[["Date", "Player", "Team", "Opp", "Our Proj", "Ext Proj", "Actual", "Delta"]].to_dict("records"),
        columnDefs=history_col_defs,
        defaultColDef={"sortable": True, "resizable": True},
        dashGridOptions={"rowHeight": 36, "headerHeight": 40, "suppressCellFocus": True},
        style={"height": "400px"},
        className="ag-theme-alpine",
    )

    pending = [p for p in history if p["actual_pra"] is None]
    update_opts = [
        {"label": f"{p['player_name']} ({p['pick_date']})",
         "value": f"{p['player_id']}|{p['pick_date']}"}
        for p in pending
    ]
    remove_opts = [
        {"label": f"{p['player_name']} ({p['pick_date']})", "value": p["player_id"]}
        for p in history
    ]

    return table, update_opts, remove_opts


@app.callback(
    Output("update-actual-status", "children"),
    Input("update-actual-btn", "n_clicks"),
    State("actual-player-dropdown", "value"),
    State("actual-pra-input", "value"),
    prevent_initial_call=True,
)
def update_actual(n_clicks, pick_key, actual_pra):
    if not pick_key or actual_pra is None:
        return "Select a pick and enter the actual PRA."
    player_id_str, pick_date = pick_key.split("|")
    update_actual_pra(int(player_id_str), pick_date, float(actual_pra))
    return "Updated."


# ── Utility callbacks ────────────────────────────────────────────────────


@app.callback(
    Output("cache-status", "children"),
    Input("clear-cache-btn", "n_clicks"),
    prevent_initial_call=True,
)
def handle_clear_cache(n_clicks):
    clear_cache()
    return "Cache cleared — reload the tab to refresh data."


@app.callback(
    Output("last-updated-text", "children"),
    Input("today-data-store", "data"),
)
def update_last_updated(_):
    # Show the most recent data update across all dates (DB freshness indicator)
    from src.db import get_last_updated as _get_latest
    today = date.today().isoformat()
    ts = _get_latest(today)
    if not ts:
        # Fall back to checking yesterday
        from datetime import timedelta
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        ts = _get_latest(yesterday)
    if not ts:
        return ""
    try:
        dt = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
        pt = dt.astimezone(ZoneInfo("America/Los_Angeles"))
        return f"DB updated {pt.strftime('%b %-d, %-I:%M %p PT')}"
    except Exception:
        return f"DB updated {ts[:16]}"


@app.callback(
    Output("team-commitment-chart", "figure"),
    Input("history-subtabs", "active_tab"),
    Input("main-tabs", "active_tab"),
)
def update_team_commitment(subtab, main_tab):
    empty = go.Figure()
    empty.update_layout(template="plotly_white", height=380,
                        annotations=[dict(text="No picks recorded yet", showarrow=False,
                                          font=dict(color="#999"), xref="paper", yref="paper",
                                          x=0.5, y=0.5)])
    if main_tab != "tab-history" or subtab != "history-commitment":
        return empty
    history = get_pick_history()
    all_playoff = _get_all_playoff_players()
    if not all_playoff:
        return empty

    # Count picks per team from history
    picks_per_team: dict[str, int] = {}
    for p in history:
        t = p.get("team_abbr", "")
        picks_per_team[t] = picks_per_team.get(t, 0) + 1

    # Count remaining (not yet picked) players per team
    used_ids = get_used_player_ids()
    remaining_per_team: dict[str, int] = {}
    total_per_team: dict[str, int] = {}
    for p in all_playoff:
        t = p["team_abbr"]
        total_per_team[t] = total_per_team.get(t, 0) + 1
        if p["player_id"] not in used_ids:
            remaining_per_team[t] = remaining_per_team.get(t, 0) + 1

    teams = sorted(total_per_team.keys())
    used_counts = [picks_per_team.get(t, 0) for t in teams]
    remaining_counts = [remaining_per_team.get(t, 0) for t in teams]
    team_colors = [NBA_TEAM_COLORS.get(t, "#aaa") for t in teams]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Picks used",
        x=teams, y=used_counts,
        marker_color=team_colors,
        text=used_counts,
        textposition="inside",
        hovertemplate="%{x}: %{y} picks used<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Remaining eligible",
        x=teams, y=remaining_counts,
        marker_color=[_hex_to_rgba(c, 0.25) for c in team_colors],
        text=remaining_counts,
        textposition="inside",
        hovertemplate="%{x}: %{y} eligible remaining<extra></extra>",
    ))
    fig.update_layout(
        barmode="stack",
        template="plotly_white",
        height=320,
        xaxis_title=None,
        yaxis_title="Players",
        legend=dict(orientation="h", y=1.05, font=dict(size=11)),
        margin=dict(l=30, r=20, t=30, b=40),
        plot_bgcolor="white", paper_bgcolor="white",
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"),
    )
    return fig


# DB refresh is handled by a server-side cron job (see README).
# APScheduler was removed because it doesn't work correctly with multiple gunicorn workers.


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)

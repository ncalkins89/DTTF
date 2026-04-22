"""
Player research engine for tiebreaking between candidate picks.

Two-phase:
  1. compute_local_signals()  — instant, from game logs + schedule + injury data
  2. fetch_web_signals()      — ~15s, Anthropic API with web search tool
"""
import os
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd


# ── Phase 1: Local signals ────────────────────────────────────────────────────

def compute_local_signals(
    player_id: int,
    player_name: str,
    team_abbr: str,
    opp_abbr: str,
    game_date: str,
    logs: pd.DataFrame,
    injury_data: dict,
    schedule: list[dict],
    game_lines: dict | None = None,
) -> list[str]:
    """Return a list of human-readable insight bullets from local data."""
    bullets = []
    if logs.empty:
        bullets.append("No recent game log data available.")
        return bullets

    logs = logs.copy()
    logs["GAME_DATE"] = pd.to_datetime(logs["GAME_DATE"])

    # ── Streak: last 3 vs. last 10 ───────────────────────────────────────
    recent3 = logs.head(3)["PRA"].mean() if len(logs) >= 3 else None
    recent10 = logs.head(10)["PRA"].mean() if len(logs) >= 10 else None
    if recent3 is not None and recent10 is not None:
        diff = recent3 - recent10
        direction = "🔥 Hot" if diff > 4 else "🧊 Cold" if diff < -4 else "➡ Steady"
        bullets.append(
            f"{direction} streak — last 3 avg {recent3:.1f} PRA vs. 10-game avg {recent10:.1f}"
        )

    # ── Home / away split ────────────────────────────────────────────────
    if "MATCHUP" in logs.columns:
        home_logs = logs[~logs["MATCHUP"].str.contains("@", na=False)]
        away_logs = logs[logs["MATCHUP"].str.contains("@", na=False)]
        if len(home_logs) >= 3 and len(away_logs) >= 3:
            h_avg = home_logs["PRA"].mean()
            a_avg = away_logs["PRA"].mean()
            diff = h_avg - a_avg
            if abs(diff) >= 3:
                where = "home" if diff > 0 else "away"
                bullets.append(
                    f"Performs better {where} — home avg {h_avg:.1f} vs. away {a_avg:.1f} PRA"
                )

    # ── Head-to-head vs. tonight's opponent ──────────────────────────────
    if "MATCHUP" in logs.columns:
        vs_opp = logs[logs["MATCHUP"].str.contains(opp_abbr, na=False)]
        if len(vs_opp) >= 2:
            h2h = vs_opp["PRA"].mean()
            overall = logs["PRA"].mean()
            diff = h2h - overall
            tag = "📈" if diff > 3 else "📉" if diff < -3 else ""
            bullets.append(
                f"{tag} vs {opp_abbr}: {h2h:.1f} PRA avg over {len(vs_opp)} games "
                f"({'above' if diff >= 0 else 'below'} season avg by {abs(diff):.1f})"
            )

    # ── Days of rest ─────────────────────────────────────────────────────
    sorted_dates = logs["GAME_DATE"].sort_values(ascending=False).reset_index(drop=True)
    last_game = sorted_dates.iloc[0] if len(sorted_dates) > 0 else pd.NaT
    if pd.notna(last_game):
        rest = (pd.Timestamp(game_date) - last_game).days
        if rest >= 2:
            bullets.append(f"💤 {rest} days rest entering this game")
        elif rest == 1:
            bullets.append("Back-to-back — played yesterday")

    # ── Recent injury / missed games ─────────────────────────────────────
    # Look for gaps ≥6 days between consecutive games in last 10 (skipping
    # the current rest gap before today — that's covered above).
    if len(sorted_dates) >= 3:
        internal_dates = sorted_dates.head(10)
        for i in range(1, len(internal_dates) - 1):
            after_gap = internal_dates.iloc[i - 1]   # more recent (returned)
            before_gap = internal_dates.iloc[i]       # older (last game before absence)
            gap = (after_gap - before_gap).days
            if gap >= 6:
                bullets.append(
                    f"🏥 Missed games recently — {gap}-day gap "
                    f"({before_gap.strftime('%b %-d')} → {after_gap.strftime('%b %-d')})"
                )
                break

    # ── Injury to key teammate ────────────────────────────────────────────
    team_injured = [
        f"{name.title()} ({info['status']})"
        for name, info in injury_data.items()
        if info.get("status") in ("Out", "Day-To-Day")
        # rough team filter: injury comment mentions team abbr
        and team_abbr.lower() in info.get("comment", "").lower()
    ]
    if team_injured:
        bullets.append(f"⚠ Teammate(s) on injury report: {', '.join(team_injured[:3])}")

    # ── Playoff vs. regular season avg ──────────────────────────────────
    if "SEASON_TYPE" in logs.columns:
        pl = logs[logs["SEASON_TYPE"] == "Playoffs"]
        rs = logs[logs["SEASON_TYPE"] == "Regular Season"]
        if len(pl) >= 2 and len(rs) >= 5:
            diff = pl["PRA"].mean() - rs["PRA"].mean()
            if abs(diff) >= 3:
                tag = "📈 Elevates" if diff > 0 else "📉 Regresses"
                bullets.append(
                    f"{tag} in playoffs — PO avg {pl['PRA'].mean():.1f} vs. RS avg {rs['PRA'].mean():.1f}"
                )

    # ── Minutes trend ────────────────────────────────────────────────────
    if "MIN" in logs.columns and len(logs) >= 5:
        min3 = logs.head(3)["MIN"].mean() if len(logs) >= 3 else None
        min10 = logs.head(10)["MIN"].mean() if len(logs) >= 10 else None
        if min3 is not None and min10 is not None:
            diff = min3 - min10
            if diff >= 3:
                bullets.append(f"⏱ Minutes trending UP — last 3 avg {min3:.0f} min vs. 10-game avg {min10:.0f} min")
            elif diff <= -3:
                bullets.append(f"⏱ Minutes trending DOWN — last 3 avg {min3:.0f} min vs. 10-game avg {min10:.0f} min")

    # ── Spread & O/U from Vegas ──────────────────────────────────────────
    if game_lines:
        line = game_lines.get(team_abbr)
        if line:
            spread = line.get("spread")
            total = line.get("total")
            is_home = line.get("is_home", True)

            if spread is not None:
                # negative spread = team is favored (giving points)
                # positive spread = team is underdog (receiving points)
                margin = spread
                if margin <= -15:
                    bullets.append(f"🏆 Heavy favorite — spread {margin:+.1f} (may sit Q4 if comfortable)")
                elif margin <= -8:
                    bullets.append(f"🏆 Favored by {abs(margin):.1f}")
                elif margin >= 15:
                    bullets.append(
                        f"📉 Heavy underdog — spread {margin:+.1f} (blowout risk, expect Q4 benching)"
                    )
                elif margin >= 8:
                    bullets.append(f"⚠ Underdog by {margin:.1f} — moderate blowout risk")
                else:
                    bullets.append(f"⚖ Competitive game — spread {margin:+.1f}")

            if total is not None:
                if total >= 228:
                    bullets.append(f"🎯 High-total game (O/U {total}) — OT candidate, more possessions")
                elif total <= 210:
                    bullets.append(f"🔒 Low-total game (O/U {total}) — slower pace expected")

    return bullets


# ── Phase 2: Web research via Anthropic API ───────────────────────────────────

def fetch_web_signals(players: list[dict], game_date: str) -> dict[int, list[str]]:
    """
    Call Claude with web_search to find props, news, matchup context.
    players: [{"player_id", "player_name", "team", "opp"}, ...]
    Returns {player_id: [bullet, ...]}
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {p["player_id"]: ["⚙ Set ANTHROPIC_API_KEY in .env to enable web research."]
                for p in players}

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    player_lines = "\n".join(
        f"- {p['player_name']} ({p['team']} vs {p['opp']})"
        for p in players
    )

    prompt = f"""You are a sports analyst helping someone decide which NBA player to pick for a daily fantasy-style game (scoring = Points + Rebounds + Assists). Today is {game_date}.

Research each of these players playing tonight and return insights for each:

{player_lines}

For EACH player find:
1. Current Vegas player prop line for PRA or Points (check DraftKings, FanDuel, or any sportsbook)
2. Any injury, load management, or role news in the last 48 hours
3. Who is likely guarding them defensively tonight — and is that defender good or bad?
4. Any relevant coach quotes about their role or expected minutes
5. Is this game projected to be close or a blowout (check the spread)?

Be concise. Return results as structured text with a clear header for each player, then 3-5 bullet points. Only include information you actually find — do not speculate."""

    results: dict[int, list[str]] = {p["player_id"]: [] for p in players}

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}],
            messages=[{"role": "user", "content": prompt}],
        )

        # Extract text content from response
        full_text = ""
        for block in response.content:
            if hasattr(block, "text"):
                full_text += block.text

        # Parse per-player sections — split on player name headers
        current_id = None
        for line in full_text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            # Detect player header
            for p in players:
                if p["player_name"].split()[-1].lower() in stripped.lower() and (
                    stripped.startswith("#") or stripped.startswith("**") or stripped.endswith("**")
                ):
                    current_id = p["player_id"]
                    break
            # Collect bullets
            if current_id is not None and stripped.startswith(("-", "•", "*", "1", "2", "3", "4", "5")):
                clean = stripped.lstrip("-•*0123456789. ").strip("*")
                if clean:
                    results[current_id].append(clean)

        # Fallback: if parsing failed, put full text under first player
        if all(len(v) == 0 for v in results.values()) and full_text:
            bullets = [l.strip().lstrip("-•* ") for l in full_text.splitlines()
                       if l.strip() and l.strip()[0] in "-•*"]
            if players:
                results[players[0]["player_id"]] = bullets or [full_text[:500]]

    except Exception as e:
        for p in players:
            results[p["player_id"]] = [f"Web research error: {e}"]

    return results

"""
compare_series_odds.py — compare DraftKings series prices vs Markov chain.

Prints per-series table showing DK%, Markov%, delta, and staleness flag.
Useful for auditing which series have stale DK lines before running the model.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_fetcher import get_series_standings
from src.db import get_latest_odds
from src.projections import compute_series_win_probability
from src.series_odds import fetch_series_win_probs as dk_fetch
from src.odds import _RAW_BLEND
from nba_api.stats.static import teams as nba_teams


def main(force_refresh: bool = False):
    team_map = {t["id"]: t["abbreviation"] for t in nba_teams.get_teams()}

    series_standings = get_series_standings()
    per_game_probs = get_latest_odds()
    print(f"Per-game probs: {len(per_game_probs)} teams")

    print("\nFetching DraftKings series odds...")
    dk_data = dk_fetch(force_refresh=force_refresh)
    print(f"DK data: {len(dk_data) // 2} series\n")

    header = f"{'Series':<18} {'Record':<8} {'DK%':>7} {'Markov%':>8} {'Delta':>8}  Source"
    print(header)
    print("-" * len(header))

    for s in series_standings:
        ha = team_map.get(s["home_team_id"], "?")
        aa = team_map.get(s["away_team_id"], "?")
        hw, aw = s["home_wins"], s["away_wins"]

        raw_p = per_game_probs.get(ha, 0.5)
        per_game_p = 0.5 + (raw_p - 0.5) * _RAW_BLEND
        markov_prob = compute_series_win_probability(hw, aw, per_game_p)

        dk_entry = dk_data.get(ha)
        if dk_entry is not None:
            dk_home = dk_entry["series_win_prob"]
            dk_odds = dk_entry["american_odds"]
            delta = dk_home - markov_prob
            print(
                f"{ha} vs {aa:<10} {hw}-{aw:<5}  "
                f"{dk_home:>6.1%}  {markov_prob:>7.1%}  {delta:>+7.1%}  DK  [DK {dk_odds:+d}]"
            )
        else:
            print(
                f"{ha} vs {aa:<10} {hw}-{aw:<5}  "
                f"{'n/a':>6}   {markov_prob:>7.1%}  {'n/a':>7}  Markov (no DK)"
            )


if __name__ == "__main__":
    force = "--refresh" in sys.argv
    main(force_refresh=force)

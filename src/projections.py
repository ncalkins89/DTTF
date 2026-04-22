from datetime import date

import numpy as np
import pandas as pd


def compute_decay_weights(
    game_dates: list,
    decay_rate: float = 0.82,
    reference_date=None,
) -> np.ndarray:
    # Decay by games-elapsed (index 0 = most recent game).
    # game_dates is expected newest-first (same order as game_logs).
    n = len(game_dates)
    weights = np.array([decay_rate ** i for i in range(n)])
    total = weights.sum()
    if total == 0:
        return weights
    return weights / total


def compute_base_pra(
    game_logs: pd.DataFrame,
    decay_rate: float = 0.82,
    reference_date=None,
) -> float:
    if game_logs.empty:
        return 0.0
    weights = compute_decay_weights(game_logs["GAME_DATE"].tolist(), decay_rate, reference_date)
    return float(np.dot(weights, game_logs["PRA"].values))


def compute_rolling_predictions(
    game_logs: pd.DataFrame,
    decay_rate: float = 0.82,
    window: int | None = None,
) -> list[float]:
    """
    For each game, predict PRA using only prior (older) games — true out-of-sample.
    game_logs must be sorted newest-first (index 0 = most recent).
    window: max number of prior games to use (None = all available).
    Returns list aligned with game_logs index; NaN where no prior data exists.
    """
    predictions = []
    for i in range(len(game_logs)):
        game_date = game_logs.iloc[i]["GAME_DATE"]
        ref = game_date.date() if hasattr(game_date, "date") else game_date
        prior = game_logs.iloc[i + 1:] if window is None else game_logs.iloc[i + 1: i + 1 + window]
        if prior.empty:
            predictions.append(float("nan"))
        else:
            predictions.append(compute_base_pra(prior, decay_rate, reference_date=ref))
    return predictions


def get_league_avg_def_rating(def_ratings: pd.DataFrame) -> float:
    return float(def_ratings["DEF_RATING"].mean())


def apply_opponent_adjustment(
    base_pra: float,
    opponent_def_rating: float,
    league_avg: float,
) -> float:
    # Disabled — tune later.
    return base_pra


def apply_spread_adjustment(pra: float, expected_margin: float | None) -> float:
    # Disabled — tune later.
    return pra


def expected_series_games_remaining(wins: int, losses: int, per_game_win_prob: float) -> float:
    """Markov chain expected games remaining in current best-of-7 series."""
    p = max(0.001, min(0.999, per_game_win_prob))
    memo = {}

    def ev(w, l):
        if w == 4 or l == 4:
            return 0.0
        if (w, l) in memo:
            return memo[(w, l)]
        val = 1 + p * ev(w + 1, l) + (1 - p) * ev(w, l + 1)
        memo[(w, l)] = val
        return val

    return ev(wins, losses)


def compute_series_win_probability(wins: int, losses: int, per_game_win_prob: float) -> float:
    """Markov chain probability of winning the series from the current record."""
    p = max(0.001, min(0.999, per_game_win_prob))
    memo = {}

    def prob(w, l):
        if w == 4:
            return 1.0
        if l == 4:
            return 0.0
        if (w, l) in memo:
            return memo[(w, l)]
        val = p * prob(w + 1, l) + (1 - p) * prob(w, l + 1)
        memo[(w, l)] = val
        return val

    return prob(wins, losses)


def compute_total_expected_games(
    wins: int,
    losses: int,
    per_game_win_prob: float,
    current_round: int = 1,
) -> float:
    """
    Expected games remaining across ALL remaining playoff rounds.
    current_round: 1=R1, 2=R2, 3=Conf Finals, 4=Finals

    For future rounds, assumes fresh series (0-0) and 50/50 per-game probability.
    Advancement probability for each future round is compounded at 50%.
    """
    current_games = expected_series_games_remaining(wins, losses, per_game_win_prob)
    series_win_prob = compute_series_win_probability(wins, losses, per_game_win_prob)

    future_games = 0.0
    cumulative_advance_prob = series_win_prob
    for _ in range(current_round + 1, 5):  # rounds go up to 4 (Finals)
        fresh_series_games = expected_series_games_remaining(0, 0, 0.5)  # ~5.8
        future_games += cumulative_advance_prob * fresh_series_games
        cumulative_advance_prob *= 0.5  # assume ~50% to advance each future round

    return current_games + future_games


def compute_urgency(projected_pra: float, series_win_prob: float) -> float:
    """
    Urgency = projected_pra × (1 - series_win_prob)

    Measures what you LOSE by not picking this player today:
    - High PRA + team about to be eliminated → pick now (high urgency)
    - High PRA + team likely to deep-run → save for later (low urgency)
    - Low PRA → low urgency regardless

    This replaces the old formula which incorrectly divided PRA by expected
    games, as if the player could be picked multiple times.
    """
    return round(projected_pra * (1.0 - series_win_prob), 2)


def project_player(
    player_id: int,
    opponent_team_id: int,
    game_logs: pd.DataFrame,
    def_ratings: pd.DataFrame,
    series_record: dict,
    per_game_win_prob: float = 0.5,
    expected_margin: float | None = None,
    decay_rate: float = 0.82,
    current_round: int = 1,
) -> dict:
    base_pra = compute_base_pra(game_logs, decay_rate)

    opp_rating_row = def_ratings[def_ratings["TEAM_ID"] == opponent_team_id]
    opponent_def_rating = (
        float(opp_rating_row["DEF_RATING"].iloc[0])
        if not opp_rating_row.empty
        else get_league_avg_def_rating(def_ratings)
    )
    league_avg = get_league_avg_def_rating(def_ratings)

    after_opponent_adj = apply_opponent_adjustment(base_pra, opponent_def_rating, league_avg)
    after_spread_adj = apply_spread_adjustment(after_opponent_adj, expected_margin)

    wins = series_record.get("wins", 0)
    losses = series_record.get("losses", 0)

    series_win_prob = compute_series_win_probability(wins, losses, per_game_win_prob)
    total_games = compute_total_expected_games(wins, losses, per_game_win_prob, current_round)
    urgency = compute_urgency(after_spread_adj, series_win_prob)

    decay_weights = (
        compute_decay_weights(game_logs["GAME_DATE"].tolist(), decay_rate)
        if not game_logs.empty else np.array([])
    )
    rolling_preds = compute_rolling_predictions(game_logs, decay_rate) if not game_logs.empty else []

    return {
        "player_id": player_id,
        "base_pra": round(base_pra, 1),
        "after_opponent_adj": round(after_opponent_adj, 1),
        "projected_pra": round(after_spread_adj, 1),
        "opponent_def_rating": round(opponent_def_rating, 1),
        "league_avg_def_rating": round(league_avg, 1),
        "opponent_adj_factor": round(opponent_def_rating / league_avg if league_avg else 1.0, 3),
        "spread_adj_factor": round(after_spread_adj / after_opponent_adj if after_opponent_adj else 1.0, 3),
        "urgency": urgency,
        "series_win_prob": round(series_win_prob, 3),
        "expected_future_games": round(total_games, 1),
        "games_used": len(game_logs),
        # For visualization
        "game_dates": game_logs["GAME_DATE"].tolist() if not game_logs.empty else [],
        "per_game_pra": game_logs["PRA"].tolist() if not game_logs.empty else [],
        "per_game_season_type": game_logs["SEASON_TYPE"].tolist() if not game_logs.empty else [],
        "decay_weights": decay_weights.tolist() if hasattr(decay_weights, "tolist") else list(decay_weights),
        "rolling_predictions": rolling_preds,
    }

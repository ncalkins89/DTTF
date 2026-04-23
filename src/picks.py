import json
import os
import tempfile
from datetime import date
from pathlib import Path

PICKS_PATH = Path(__file__).parent.parent / "data" / "picks.json"


def load_picks() -> dict:
    PICKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not PICKS_PATH.exists():
        return {"picks": [], "used_player_ids": []}
    with open(PICKS_PATH) as f:
        return json.load(f)


def save_picks(data: dict) -> None:
    PICKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=PICKS_PATH.parent, suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, PICKS_PATH)
    except Exception:
        os.unlink(tmp)
        raise


def record_pick(
    player_id: int,
    player_name: str,
    team_abbr: str,
    opponent_abbr: str,
    projected_pra: float,
    game_id: str,
    external_projected_pra: float | None = None,
) -> None:
    data = load_picks()
    if player_id in data["used_player_ids"]:
        raise ValueError(f"{player_name} (id={player_id}) has already been picked this playoffs.")
    entry = {
        "pick_date": date.today().isoformat(),
        "player_id": player_id,
        "player_name": player_name,
        "team_abbr": team_abbr,
        "opponent_abbr": opponent_abbr,
        "projected_pra": round(projected_pra, 1),
        "external_projected_pra": round(external_projected_pra, 1) if external_projected_pra is not None else None,
        "actual_pra": None,
        "game_id": game_id,
    }
    data["picks"].append(entry)
    data["used_player_ids"].append(player_id)
    save_picks(data)


def get_used_player_ids() -> set[int]:
    return set(load_picks()["used_player_ids"])


def update_actual_pra(player_id: int, pick_date: str, actual_pra: float) -> None:
    data = load_picks()
    for pick in data["picks"]:
        if pick["player_id"] == player_id and pick["pick_date"] == pick_date:
            pick["actual_pra"] = round(actual_pra, 1)
            save_picks(data)
            return
    raise ValueError(f"No pick found for player_id={player_id} on {pick_date}")


def remove_pick(player_id: int) -> str:
    """Remove a pick by player_id. Returns the player name that was removed."""
    data = load_picks()
    match = next((p for p in data["picks"] if p["player_id"] == player_id), None)
    if not match:
        raise ValueError(f"No pick found for player_id={player_id}")
    data["picks"] = [p for p in data["picks"] if p["player_id"] != player_id]
    data["used_player_ids"] = [i for i in data["used_player_ids"] if i != player_id]
    save_picks(data)
    return match["player_name"]


def get_pick_history() -> list[dict]:
    return sorted(load_picks()["picks"], key=lambda p: p["pick_date"], reverse=True)

"""Blend projections using per-subset optimized weights from blend_weights.json."""
import json
import os
from pathlib import Path

_WEIGHTS_PATH = Path(__file__).parent.parent / "data" / "blend_weights.json"
_cache: dict | None = None
_cache_mtime: float = 0.0


def _load() -> dict:
    global _cache, _cache_mtime
    try:
        mtime = os.path.getmtime(_WEIGHTS_PATH)
    except OSError:
        return {}
    if _cache is None or mtime != _cache_mtime:
        try:
            _cache = json.loads(_WEIGHTS_PATH.read_text())
            _cache_mtime = mtime
        except (json.JSONDecodeError, OSError):
            _cache = {}
    return _cache


def blend(
    our: float | None,
    de: float | None,
    fd: float | None,
) -> tuple[float | None, str]:
    """
    Returns (blended_pra, formula_str) for the subset of non-null sources.
    Falls back to equal-weight average when weights file is absent.
    """
    vals = {"our": our, "de": de, "fd": fd}
    available = tuple(s for s in ("our", "de", "fd") if vals[s] is not None)
    if not available:
        return None, ""

    weights = _load()
    key = "+".join(available)
    entry = weights.get(key)

    if entry:
        pred = sum(
            entry["weights"][i] * vals[s]
            for i, s in enumerate(entry["sources"])
        )
        return round(pred, 1), entry["formula"]

    # Fallback: equal average
    avg = sum(vals[s] for s in available) / len(available)
    labels = {"our": "Ours", "de": "DE", "fd": "FD"}
    pct = 100 // len(available)
    formula = " + ".join(f"{pct}% {labels[s]}" for s in available)
    return round(avg, 1), formula

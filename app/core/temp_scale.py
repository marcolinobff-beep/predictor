from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "data": None}


def load_temp_scales(path: str) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return None
    if _CACHE["path"] == str(p) and _CACHE["mtime"] == mtime:
        return _CACHE["data"]
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    _CACHE.update({"path": str(p), "mtime": mtime, "data": data})
    return data


def get_temp_scale(path: str, league: Optional[str]) -> Optional[float]:
    data = load_temp_scales(path)
    if not data or not league:
        return None
    entry = (data.get("by_league") or {}).get(league) or {}
    if not entry or not entry.get("enabled"):
        return None
    temp = entry.get("temp")
    if temp is None:
        return None
    try:
        temp = float(temp)
    except (TypeError, ValueError):
        return None
    if temp <= 0:
        return None
    return temp


def apply_temp_scale_1x2(probs: Dict[str, float], temp: float) -> Dict[str, float]:
    if temp <= 0:
        return probs
    keys = ("home_win", "draw", "away_win")
    if not all(k in probs for k in keys):
        return probs
    eps = 1e-12
    p1 = max(float(probs["home_win"]), eps) ** (1.0 / temp)
    px = max(float(probs["draw"]), eps) ** (1.0 / temp)
    p2 = max(float(probs["away_win"]), eps) ** (1.0 / temp)
    s = p1 + px + p2
    if s <= 0:
        return probs
    out = dict(probs)
    out["home_win"] = p1 / s
    out["draw"] = px / s
    out["away_win"] = p2 / s
    return out

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "data": None}


def load_ensemble_weights(path: str) -> Optional[Dict[str, Any]]:
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


def get_ensemble_weight(path: str, league: Optional[str], default_weight: float) -> float:
    data = load_ensemble_weights(path)
    if not data:
        return float(default_weight)
    if league and isinstance(data, dict) and "by_league" in data:
        entry = (data.get("by_league") or {}).get(league) or {}
        for key in ("best_weight", "weight", "ensemble_weight"):
            if key in entry:
                return float(entry[key])
    for key in ("best_weight", "weight", "ensemble_weight"):
        if key in data:
            return float(data[key])
    return float(default_weight)

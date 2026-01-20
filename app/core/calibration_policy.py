from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "data": None}


def load_calibration_policy(path: str) -> Optional[Dict[str, Any]]:
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


def should_calibrate_1x2(path: str, league: Optional[str], default: bool = True) -> bool:
    data = load_calibration_policy(path)
    if not data or not league:
        return default
    entry = (data.get("by_league") or {}).get(league) or {}
    if "calibrate_1x2" in entry:
        return bool(entry.get("calibrate_1x2"))
    return default

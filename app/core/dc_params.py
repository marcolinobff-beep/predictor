from __future__ import annotations

import json
import os
from typing import Optional, Dict, Any


def load_dc_params(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def get_rho(path: Optional[str], league: Optional[str] = None) -> float:
    params = load_dc_params(path)
    if not params:
        return 0.0
    if league and isinstance(params, dict) and "by_league" in params:
        params = (params.get("by_league") or {}).get(league)
        if not params:
            return 0.0
    try:
        return float(params.get("rho", 0.0))
    except Exception:
        return 0.0

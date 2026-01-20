from __future__ import annotations

import json
import os
from typing import Dict, Any

from app.core.config import settings


DEFAULT_RULES: Dict[str, Any] = {
    "min_edge": 0.03,
    "max_picks": 3,
    "max_ci_width": 0.06,
    "kelly_fraction": 0.25,
    "stake_cap_fraction": 0.02,
    "max_model_market_gap": 0.12,
    "longshot_odds": 5.0,
    "min_edge_longshot": 0.08,
    "max_odds": 6.0,
    "min_books": 2,
    "max_odds_age_hours": 12,
    "min_model_confidence": 0.45,
}


def get_market_rules(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    rules = dict(DEFAULT_RULES)
    path = settings.market_rules_path
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    rules.update(data)
        except Exception:
            pass
    if overrides:
        rules.update(overrides)
    return rules


def get_betting_gate() -> Dict[str, Any]:
    path = settings.betting_gate_path
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {"enabled": False, "reason": "MISSING_GATE_FILE"}


def is_betting_enabled() -> bool:
    gate = get_betting_gate()
    return bool(gate.get("enabled", False))

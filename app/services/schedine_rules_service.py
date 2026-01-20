from __future__ import annotations

import json
import os
from typing import Dict, Any

from app.core.config import settings
from app.services.market_rules_service import get_market_rules


DEFAULT_RULES: Dict[str, Any] = {
    "card_size": 3,
    "markets": ["1X2", "OU_2.5"],
    "min_edge": 0.01,
    "easy": {"min_prob": 0.58, "max_odds": 2.5},
    "medium": {"min_prob": 0.48, "max_odds": 3.6},
    "hard": {"min_prob": 0.35, "min_odds": 2.2, "max_odds": 6.0},
}


def get_schedine_rules(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    rules = dict(DEFAULT_RULES)
    path = settings.schedine_rules_path
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    rules.update(data)
        except Exception:
            pass
    # ensure min_edge follows market_rules if present
    market_rules = get_market_rules()
    if market_rules.get("min_edge") is not None:
        rules["min_edge"] = float(market_rules["min_edge"])
    if overrides:
        rules.update(overrides)
    return rules

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

from app.core.logreg_np import MultinomialLogReg, BinaryLogReg


def implied_probs_from_odds_1x2(odds_home: float, odds_draw: float, odds_away: float) -> Dict[str, float]:
    """Probabilità implicite normalizzate (rimuove overround via rinormalizzazione)."""
    inv = np.array([1.0 / odds_home, 1.0 / odds_draw, 1.0 / odds_away], dtype=float)
    s = float(inv.sum())
    if s <= 0:
        return {"home_win": 0.0, "draw": 0.0, "away_win": 0.0}
    p = inv / s
    return {"home_win": float(p[0]), "draw": float(p[1]), "away_win": float(p[2])}


def implied_probs_from_odds_ou25(odds_over: float, odds_under: float) -> Dict[str, float]:
    inv = np.array([1.0 / odds_over, 1.0 / odds_under], dtype=float)
    s = float(inv.sum())
    if s <= 0:
        return {"over_2_5": 0.0, "under_2_5": 0.0}
    p = inv / s
    return {"over_2_5": float(p[0]), "under_2_5": float(p[1])}


def load_json_model(path: str | Path) -> Optional[dict]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_multinomial(path: str | Path) -> Optional[MultinomialLogReg]:
    d = load_json_model(path)
    if not d or d.get("model_type") != "multinomial_logreg":
        return None
    return MultinomialLogReg.from_dict(d)


def load_binary(path: str | Path) -> Optional[BinaryLogReg]:
    d = load_json_model(path)
    if not d or d.get("model_type") != "binary_logreg":
        return None
    return BinaryLogReg.from_dict(d)

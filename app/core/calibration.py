from __future__ import annotations

import json
import os
from typing import Dict, Any, Optional
from datetime import datetime


def load_calibration(path: str) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def select_league_calibration(cal: Optional[Dict[str, Any]], league: Optional[str]) -> Optional[Dict[str, Any]]:
    if not cal:
        return None
    if league and "by_league" in cal:
        return (cal.get("by_league") or {}).get(league)
    return cal


def _map_prob(bins, p: float) -> float:
    for b in bins:
        if b["min"] <= p < b["max"]:
            return float(b["p"])
    return p


def apply_calibration(probs: Dict[str, float], cal: Dict[str, Any]) -> Dict[str, float]:
    if not cal or "markets" not in cal:
        return probs

    markets = cal["markets"]
    out = dict(probs)

    for key, p in probs.items():
        bins = markets.get(key)
        if bins:
            out[key] = _map_prob(bins, float(p))

    # normalize 1X2
    keys_1x2 = ["home_win", "draw", "away_win"]
    if all(k in out for k in keys_1x2):
        s = sum(out[k] for k in keys_1x2)
        if s > 0:
            for k in keys_1x2:
                out[k] = out[k] / s

    # normalize OU 2.5
    if "over_2_5" in out and "under_2_5" in out:
        s = out["over_2_5"] + out["under_2_5"]
        if s > 0:
            out["over_2_5"] = out["over_2_5"] / s
            out["under_2_5"] = out["under_2_5"] / s

    # normalize BTTS
    if "btts_yes" in out and "btts_no" in out:
        s = out["btts_yes"] + out["btts_no"]
        if s > 0:
            out["btts_yes"] = out["btts_yes"] / s
            out["btts_no"] = out["btts_no"] / s

    return out


def _season_phase(kickoff_utc: Optional[datetime]) -> Optional[str]:
    if not kickoff_utc:
        return None
    m = kickoff_utc.month
    if m in (8, 9, 10):
        return "early"
    if m in (11, 12, 1, 2):
        return "mid"
    if m in (3, 4, 5, 6, 7):
        return "late"
    return None


def select_calibration(
    cal: Optional[Dict[str, Any]],
    season_label: Optional[str],
    kickoff_utc: Optional[datetime],
) -> Optional[Dict[str, Any]]:
    if not cal:
        return None
    if "by_season" not in cal:
        return cal
    by_season = cal.get("by_season") or {}
    season_entry = by_season.get(season_label)
    if not season_entry:
        return cal.get("default") or cal
    phase = _season_phase(kickoff_utc)
    if phase and phase in season_entry:
        return season_entry[phase]
    return season_entry.get("full") or season_entry.get("default") or cal.get("default") or cal

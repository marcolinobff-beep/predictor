from __future__ import annotations

from typing import Dict, Any, Optional, List

from app.db.sqlite import get_conn


def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def _fetch_tactical_stats(match_id: str) -> Optional[dict]:
    with get_conn() as conn:
        if not _table_exists(conn, "tactical_stats"):
            return None
        row = conn.execute(
            """
            SELECT match_id, source,
                   possession_home, possession_away,
                   ppda_home, ppda_away
            FROM tactical_stats
            WHERE match_id = ?
            """,
            (match_id,),
        ).fetchone()
    if not row:
        return None
    return dict(row)


def _style_matchup(tags: List[str]) -> Dict[str, str]:
    home_edge = "neutral"
    away_edge = "neutral"
    reason = None

    if "home_high_press" in tags and "away_low_press" in tags:
        home_edge = "favorable"
        away_edge = "critical"
        reason = "home_press_vs_away_low_press"
    elif "away_high_press" in tags and "home_low_press" in tags:
        home_edge = "critical"
        away_edge = "favorable"
        reason = "away_press_vs_home_low_press"
    elif "home_possession" in tags and "away_low_press" in tags:
        home_edge = "favorable"
        away_edge = "critical"
        reason = "home_possession_vs_away_low_press"
    elif "away_possession" in tags and "home_low_press" in tags:
        home_edge = "critical"
        away_edge = "favorable"
        reason = "away_possession_vs_home_low_press"

    indicator = "neutral"
    if home_edge == "favorable":
        indicator = "home_favorable"
    elif away_edge == "favorable":
        indicator = "away_favorable"

    return {
        "indicator": indicator,
        "home_edge": home_edge,
        "away_edge": away_edge,
        "reason": reason or "balanced",
    }


def _tempo_hint(tags: List[str]) -> str:
    if "high_tempo_proxy" in tags or "home_high_press" in tags or "away_high_press" in tags:
        return "high"
    if "low_tempo_proxy" in tags or ("home_low_press" in tags and "away_low_press" in tags):
        return "low"
    return "neutral"


def _tags_from_stats(stats: dict) -> Dict[str, Any]:
    tags: List[str] = []
    pos_h = stats.get("possession_home")
    pos_a = stats.get("possession_away")
    ppda_h = stats.get("ppda_home")
    ppda_a = stats.get("ppda_away")

    if pos_h is not None and pos_a is not None:
        if pos_h <= 1.0 and pos_a <= 1.0:
            pos_h = float(pos_h) * 100.0
            pos_a = float(pos_a) * 100.0

    if pos_h is not None and pos_a is not None:
        if pos_h >= 55:
            tags.append("home_possession")
        elif pos_h <= 45:
            tags.append("away_possession")

    if ppda_h is not None:
        if ppda_h <= 8:
            tags.append("home_high_press")
        elif ppda_h >= 12:
            tags.append("home_low_press")

    if ppda_a is not None:
        if ppda_a <= 8:
            tags.append("away_high_press")
        elif ppda_a >= 12:
            tags.append("away_low_press")

    matchup = None
    if "home_high_press" in tags and "away_possession" in tags:
        matchup = "press_vs_possession"
    elif "away_high_press" in tags and "home_possession" in tags:
        matchup = "press_vs_possession"
    elif "home_low_press" in tags and "away_low_press" in tags:
        matchup = "low_press_both"

    return {
        "source": stats.get("source") or "tactical_stats",
        "tags": tags,
        "matchup": matchup,
        "style_matchup": _style_matchup(tags),
        "tempo": _tempo_hint(tags),
    }


def _tags_from_proxy(features: dict) -> Dict[str, Any]:
    tags: List[str] = []
    lam_h = float(features.get("lambda_home", 0.0) or 0.0)
    lam_a = float(features.get("lambda_away", 0.0) or 0.0)
    total = lam_h + lam_a
    diff = lam_h - lam_a

    if total >= 2.8:
        tags.append("high_tempo_proxy")
    elif total <= 2.2:
        tags.append("low_tempo_proxy")

    if diff >= 0.45:
        tags.append("home_dominance_proxy")
    elif diff <= -0.45:
        tags.append("away_dominance_proxy")

    matchup = None
    if "high_tempo_proxy" in tags:
        matchup = "high_tempo_match"
    elif "low_tempo_proxy" in tags:
        matchup = "low_tempo_match"

    return {
        "source": "proxy",
        "tags": tags,
        "matchup": matchup,
        "style_matchup": _style_matchup(tags),
        "tempo": _tempo_hint(tags),
    }


def get_tactical_profile(match_id: str, features: dict) -> Dict[str, Any]:
    stats = _fetch_tactical_stats(match_id)
    if stats:
        return _tags_from_stats(stats)
    if features:
        return _tags_from_proxy(features)
    return {"source": "none", "tags": [], "matchup": None, "style_matchup": _style_matchup([]), "tempo": "neutral"}

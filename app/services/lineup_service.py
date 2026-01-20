from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

from app.db.sqlite import get_conn
from app.core.text_utils import clean_person_name, normalize_person_name


@dataclass
class LineupData:
    match_id: str
    source: str
    confidence: float
    home_players: List[str]
    away_players: List[str]
    home_absences: List[str]
    away_absences: List[str]
    fetched_at_utc: str


@dataclass
class LineupAdjustment:
    coverage_home: float
    coverage_away: float
    penalty_home: float
    penalty_away: float
    absence_share_home: float
    absence_share_away: float
    notes: List[str]


def _normalize_name(value: str) -> str:
    return normalize_person_name(value)


def _clean_list(values: List[str]) -> List[str]:
    out: List[str] = []
    for v in values:
        name = clean_person_name(v)
        if name:
            out.append(name)
    return out


def get_latest_lineup(match_id: str) -> Optional[LineupData]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT match_id, source, confidence, home_players_json, away_players_json,
                   home_absences_json, away_absences_json, fetched_at_utc
            FROM probable_lineups
            WHERE match_id = ?
            ORDER BY fetched_at_utc DESC
            LIMIT 1
            """,
            (match_id,),
        ).fetchone()
    if not row:
        return None
    try:
        home_players = json.loads(row["home_players_json"] or "[]")
        away_players = json.loads(row["away_players_json"] or "[]")
        home_absences = json.loads(row["home_absences_json"] or "[]")
        away_absences = json.loads(row["away_absences_json"] or "[]")
    except Exception:
        return None
    home_players = _clean_list(home_players)
    away_players = _clean_list(away_players)
    home_absences = _clean_list(home_absences)
    away_absences = _clean_list(away_absences)
    return LineupData(
        match_id=row["match_id"],
        source=row["source"],
        confidence=float(row["confidence"] or 0.6),
        home_players=home_players,
        away_players=away_players,
        home_absences=home_absences,
        away_absences=away_absences,
        fetched_at_utc=row["fetched_at_utc"],
    )


def _minutes_factor(minutes: Optional[int], games: Optional[int]) -> float:
    if not minutes or not games or games <= 0:
        return 0.85
    mpg = float(minutes) / float(games)
    if mpg <= 0:
        return 0.85
    return max(0.5, min(1.0, mpg / 90.0))


def _team_projection_shares(league: str, season: int, team: str) -> List[Tuple[str, float, float]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT player_name, gi_share, time_minutes, games
            FROM player_projections
            WHERE league = ? AND season = ? AND lower(team_title) = lower(?)
            ORDER BY gi_share DESC
            """,
            (league, season, team),
        ).fetchall()
    out = []
    for r in rows:
        out.append((
            r["player_name"],
            float(r["gi_share"] or 0.0),
            _minutes_factor(r["time_minutes"], r["games"]),
        ))
    return out


def _share_sum(players: List[str], shares: List[Tuple[str, float, float]]) -> float:
    if not players or not shares:
        return 0.0
    player_map: List[Tuple[str, str, float, float]] = []
    for name, share, minutes_factor in shares:
        if not name:
            continue
        name_norm = _normalize_name(name)
        if not name_norm:
            continue
        tokens = name_norm.split()
        surname = tokens[-1] if tokens else ""
        player_map.append((name_norm, surname, share, minutes_factor))
    used = set()
    total = 0.0
    for p in players:
        key = _normalize_name(p)
        if not key:
            continue
        matched = False
        for name_norm, _, share, minutes_factor in player_map:
            if name_norm in used:
                continue
            if key == name_norm or key in name_norm or name_norm in key:
                total += share * minutes_factor
                used.add(name_norm)
                matched = True
                break
        if matched:
            continue
        tokens = key.split()
        if not tokens:
            continue
        surname_key = tokens[-1]
        if len(surname_key) < 4:
            continue
        for name_norm, surname, share, minutes_factor in player_map:
            if name_norm in used:
                continue
            if surname_key == surname and surname_key:
                total += share * minutes_factor
                used.add(name_norm)
                break
    return total


def compute_lineup_adjustment(
    match_id: str,
    league: str,
    season_start: int,
    home_team: str,
    away_team: str,
) -> Tuple[Optional[LineupData], Optional[LineupAdjustment]]:
    lineup = get_latest_lineup(match_id)
    if not lineup:
        return None, None

    shares_home = _team_projection_shares(league, season_start, home_team)
    shares_away = _team_projection_shares(league, season_start, away_team)
    home_players = lineup.home_players[:11]
    away_players = lineup.away_players[:11]
    top_share_home = sum(share * minutes for _, share, minutes in shares_home[:11]) or 0.0
    top_share_away = sum(share * minutes for _, share, minutes in shares_away[:11]) or 0.0

    lineup_share_home = _share_sum(home_players, shares_home)
    lineup_share_away = _share_sum(away_players, shares_away)
    abs_share_home = _share_sum(lineup.home_absences, shares_home)
    abs_share_away = _share_sum(lineup.away_absences, shares_away)

    coverage_home = (lineup_share_home / top_share_home) if top_share_home > 0 else 0.0
    coverage_away = (lineup_share_away / top_share_away) if top_share_away > 0 else 0.0
    absence_share_home = (abs_share_home / top_share_home) if top_share_home > 0 else 0.0
    absence_share_away = (abs_share_away / top_share_away) if top_share_away > 0 else 0.0

    conf = float(lineup.confidence or 0.6)
    conf_scale = max(0.6, min(1.0, 0.6 + 0.4 * conf))
    penalty_home = (0.12 * (1.0 - coverage_home) + 0.25 * absence_share_home) * conf_scale
    penalty_away = (0.12 * (1.0 - coverage_away) + 0.25 * absence_share_away) * conf_scale
    penalty_home = max(0.0, min(0.30, penalty_home))
    penalty_away = max(0.0, min(0.30, penalty_away))

    notes = [
        f"lineup_source={lineup.source}",
        f"coverage_home={coverage_home:.2f}",
        f"coverage_away={coverage_away:.2f}",
        f"absence_share_home={absence_share_home:.2f}",
        f"absence_share_away={absence_share_away:.2f}",
        f"confidence={conf:.2f}",
    ]

    return lineup, LineupAdjustment(
        coverage_home=coverage_home,
        coverage_away=coverage_away,
        penalty_home=penalty_home,
        penalty_away=penalty_away,
        absence_share_home=absence_share_home,
        absence_share_away=absence_share_away,
        notes=notes,
    )

from __future__ import annotations

import re
from typing import List, Optional, Dict

from app.db.sqlite import get_conn
from app.models.schemas import PlayerProjection, PlayerProjectionReport, MatchRef, SimulationOutputs


def _season_start_from_label(label: Optional[str]) -> Optional[int]:
    if not label:
        return None
    m = re.match(r"(\d{4})", str(label))
    if not m:
        return None
    return int(m.group(1))


def _expected_values(lam: Optional[float], xg_share: Optional[float], xa_share: Optional[float]):
    expected_xg = None
    expected_xa = None
    expected_gi = None
    if lam is not None and xg_share is not None:
        expected_xg = lam * xg_share
    if lam is not None and xa_share is not None:
        expected_xa = lam * xa_share
    if expected_xg is not None or expected_xa is not None:
        expected_gi = (expected_xg or 0.0) + (expected_xa or 0.0)
    return expected_xg, expected_xa, expected_gi


def list_player_projections(
    league: str,
    season: int,
    team: Optional[str] = None,
    min_minutes: int = 0,
    limit: int = 200,
) -> List[PlayerProjection]:
    sql = """
        SELECT player_id, player_name, team_title, position, games, time_minutes,
               xg, xa, shots, key_passes,
               xg_per90, xa_per90, shots_per90, key_passes_per90, gi_per90,
               xg_share, xa_share, gi_share
        FROM player_projections
        WHERE league = ? AND season = ?
    """
    params: List[object] = [league, season]
    if team:
        sql += " AND lower(team_title) = lower(?)"
        params.append(team)
    if min_minutes > 0:
        sql += " AND time_minutes >= ?"
        params.append(min_minutes)
    sql += " ORDER BY gi_per90 DESC, xg_per90 DESC LIMIT ?"
    params.append(limit)

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    projections: List[PlayerProjection] = []
    for r in rows:
        projections.append(PlayerProjection(
            player_id=r["player_id"],
            player_name=r["player_name"],
            team=r["team_title"],
            position=r["position"],
            season=season,
            games=r["games"],
            minutes=r["time_minutes"],
            xg=r["xg"],
            xa=r["xa"],
            shots=r["shots"],
            key_passes=r["key_passes"],
            xg_per90=r["xg_per90"],
            xa_per90=r["xa_per90"],
            shots_per90=r["shots_per90"],
            key_passes_per90=r["key_passes_per90"],
            gi_per90=r["gi_per90"],
            xg_share=r["xg_share"],
            xa_share=r["xa_share"],
            gi_share=r["gi_share"],
            expected_xg=None,
            expected_xa=None,
            expected_gi=None,
        ))
    return projections


def _project_team(
    league: str,
    season: int,
    team: str,
    lam: Optional[float],
    limit: int,
    min_minutes: int,
) -> List[PlayerProjection]:
    rows = list_player_projections(
        league=league,
        season=season,
        team=team,
        min_minutes=min_minutes,
        limit=limit,
    )
    projected: List[PlayerProjection] = []
    for p in rows:
        expected_xg, expected_xa, expected_gi = _expected_values(lam, p.xg_share, p.xa_share)
        projected.append(p.model_copy(update={
            "expected_xg": expected_xg,
            "expected_xa": expected_xa,
            "expected_gi": expected_gi,
        }))
    projected.sort(
        key=lambda p: (
            p.expected_gi if p.expected_gi is not None else -1.0,
            p.gi_per90 if p.gi_per90 is not None else -1.0,
            p.xg_per90 if p.xg_per90 is not None else -1.0,
        ),
        reverse=True,
    )
    return projected


def get_player_projections_for_match(
    match: MatchRef,
    sim_out: Optional[SimulationOutputs],
    limit: int = 6,
    min_minutes: int = 450,
) -> PlayerProjectionReport:
    season_start = _season_start_from_label(match.season)
    if not season_start:
        return PlayerProjectionReport(notes=["NO_SEASON_LABEL"])

    lam_h = None
    lam_a = None
    if sim_out and sim_out.diagnostics:
        lam_h = sim_out.diagnostics.get("lambda_home")
        lam_a = sim_out.diagnostics.get("lambda_away")

    home_players = _project_team(match.competition, season_start, match.home.name, lam_h, limit, min_minutes)
    away_players = _project_team(match.competition, season_start, match.away.name, lam_a, limit, min_minutes)

    notes: List[str] = []
    if not home_players:
        notes.append("NO_HOME_PLAYER_PROJECTIONS")
    if not away_players:
        notes.append("NO_AWAY_PLAYER_PROJECTIONS")

    return PlayerProjectionReport(
        home=home_players,
        away=away_players,
        notes=notes,
    )

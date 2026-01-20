from __future__ import annotations

import json
import re
from datetime import datetime, date, timezone, timedelta
from types import SimpleNamespace
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel

from app.core.config import settings
from app.db.sqlite import get_conn
from app.models.schemas import MatchAnalysisReport
from app.services.dashboard_service import get_dashboard_kpis
from app.services.report_service import analyze_match_by_id
from app.services.ui_adapter_service import (
    report_to_ui_analyze,
    build_ui_slate,
)
from app.services.player_projection_service import (
    list_player_projections,
    get_player_projections_for_match,
)
from app.services.context_service import get_match_context_by_id
from app.services.simulation_service import run_match_simulation


router = APIRouter(tags=["ui-api"])


COMPETITION_ALIASES = {
    "serie a": "Serie_A",
    "serie b": "Serie_B",
    "premier league": "EPL",
    "epl": "EPL",
    "bundesliga": "Bundesliga",
    "la liga": "La_Liga",
    "laliga": "La_Liga",
    "ligue 1": "Ligue_1",
}


MARKET_ALIASES = {
    "1x2": "1X2",
    "over/under 2.5": "OU_2.5",
    "ou_2.5": "OU_2.5",
    "btts": "BTTS",
    "doppia chance": "1X2",
}


def _require_token(request: Request) -> None:
    expected = settings.chat_ui_token
    if not expected:
        return
    auth = request.headers.get("Authorization") or ""
    token = None
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
    token = token or request.headers.get("X-Chat-Token")
    if token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower()).strip()


def _match_date_bounds(day: date) -> tuple[str, str]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


def _competition_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = value.strip().lower()
    return COMPETITION_ALIASES.get(key, value.strip().replace(" ", "_"))


def _find_match_id(
    home: str,
    away: str,
    competition: Optional[str],
    match_day: Optional[date],
) -> Optional[str]:
    home_norm = _normalize(home)
    away_norm = _normalize(away)
    comp_code = _competition_code(competition)

    sql = """
        SELECT match_id, kickoff_utc
        FROM matches
        WHERE lower(home) = ? AND lower(away) = ?
    """
    params: List[object] = [home.lower().strip(), away.lower().strip()]
    if comp_code:
        sql += " AND competition = ?"
        params.append(comp_code)
    if match_day:
        start_iso, end_iso = _match_date_bounds(match_day)
        sql += " AND kickoff_utc >= ? AND kickoff_utc < ?"
        params.extend([start_iso, end_iso])
    sql += " ORDER BY kickoff_utc ASC"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        sql = """
            SELECT match_id, kickoff_utc
            FROM matches
            WHERE lower(home) LIKE ? AND lower(away) LIKE ?
        """
        params = [f"%{home_norm}%", f"%{away_norm}%"]
        if comp_code:
            sql += " AND competition = ?"
            params.append(comp_code)
        if match_day:
            start_iso, end_iso = _match_date_bounds(match_day)
            sql += " AND kickoff_utc >= ? AND kickoff_utc < ?"
            params.extend([start_iso, end_iso])
        sql += " ORDER BY kickoff_utc ASC"
        with get_conn() as conn:
            rows = conn.execute(sql, params).fetchall()

    if not rows:
        return None

    now = datetime.now(timezone.utc)
    upcoming = []
    for r in rows:
        kickoff = datetime.fromisoformat(str(r["kickoff_utc"]).replace("Z", "+00:00"))
        if kickoff >= now:
            upcoming.append((kickoff, r["match_id"]))
    if upcoming:
        return sorted(upcoming, key=lambda x: x[0])[0][1]
    return rows[-1]["match_id"]


class AnalyzeUiRequest(BaseModel):
    home_team: str
    away_team: str
    competition: Optional[str] = None
    date: Optional[str] = None


class OddsRiskRequest(BaseModel):
    min_edge: Optional[float] = None
    max_odds: Optional[float] = None
    min_probability: Optional[float] = None
    markets: Optional[List[str]] = None
    competitions: Optional[List[str]] = None
    confidence_level: Optional[float] = None
    bankroll: Optional[float] = None


@router.get("/dashboard/kpis")
def dashboard_kpis(request: Request) -> Dict[str, Any]:
    _require_token(request)
    return get_dashboard_kpis()


@router.post("/analyze")
def analyze_ui(req: AnalyzeUiRequest, request: Request) -> Dict[str, Any]:
    _require_token(request)
    match_day = None
    if req.date:
        try:
            match_day = date.fromisoformat(req.date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format (YYYY-MM-DD).")

    match_id = _find_match_id(req.home_team, req.away_team, req.competition, match_day)
    if not match_id:
        raise HTTPException(status_code=404, detail="Match not found in database.")

    report = analyze_match_by_id(SimpleNamespace(
        match_id=match_id,
        n_sims=50000,
        seed=42,
        bankroll=1000.0,
    ))
    return report_to_ui_analyze(report)


@router.get("/analyze_by_id/{match_id}")
def analyze_by_id_ui(match_id: str, request: Request) -> Dict[str, Any]:
    _require_token(request)
    report = analyze_match_by_id(SimpleNamespace(
        match_id=match_id,
        n_sims=50000,
        seed=42,
        bankroll=1000.0,
    ))
    return report_to_ui_analyze(report)


@router.get("/slate")
def slate_ui(
    request: Request,
    date_utc: Optional[str] = Query(None, alias="date"),
    competition: Optional[str] = Query(None),
) -> Dict[str, Any]:
    _require_token(request)
    day = datetime.now(timezone.utc).date()
    if date_utc:
        try:
            day = date.fromisoformat(date_utc)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format (YYYY-MM-DD).")

    comp_code = _competition_code(competition)
    return build_ui_slate(day, comp_code)


def _risk_rating(prob: float, odds: float) -> str:
    if prob >= 0.6 and odds <= 2.2:
        return "low"
    if prob >= 0.45 and odds <= 3.5:
        return "medium"
    return "high"


def _confidence_interval(report: MatchAnalysisReport, market: str, selection: str) -> List[float]:
    if not report.simulation_outputs:
        return [0.0, 1.0]
    key = None
    if market == "1X2":
        key = {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(selection)
    elif market == "OU_2.5":
        key = {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(selection)
    elif market == "BTTS":
        key = {"YES": "btts_yes", "NO": "btts_no"}.get(selection)
    if not key:
        return [0.0, 1.0]
    interval = report.simulation_outputs.intervals.get(key)
    if not isinstance(interval, dict):
        return [0.0, 1.0]
    return [float(interval.get("lo", 0.0)), float(interval.get("hi", 1.0))]


@router.post("/odds-risk")
def odds_risk(req: OddsRiskRequest, request: Request) -> Dict[str, Any]:
    _require_token(request)
    min_edge = float(req.min_edge or 0.0)
    max_odds = float(req.max_odds or 99.0)
    min_prob = float(req.min_probability or 0.0)
    bankroll = float(req.bankroll or 1000.0)

    comps = None
    if req.competitions:
        comps = [_competition_code(c) for c in req.competitions if c]

    markets = None
    if req.markets:
        markets = [MARKET_ALIASES.get(m.strip().lower(), m) for m in req.markets]

    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=6)).isoformat().replace("+00:00", "Z")
    end = (now + timedelta(days=2)).isoformat().replace("+00:00", "Z")

    sql = """
        SELECT match_id, kickoff_utc, competition, home, away
        FROM matches
        WHERE kickoff_utc >= ? AND kickoff_utc <= ?
    """
    params: List[object] = [start, end]
    if comps:
        sql += f" AND competition IN ({','.join(['?'] * len(comps))})"
        params.extend(comps)
    sql += " ORDER BY kickoff_utc ASC LIMIT 40"

    with get_conn() as conn:
        match_rows = conn.execute(sql, params).fetchall()

    opportunities = []
    for row in match_rows:
        report = analyze_match_by_id(SimpleNamespace(
            match_id=row["match_id"],
            n_sims=20000,
            seed=42,
            bankroll=bankroll,
        ))
        for item in report.market_evaluation or []:
            if markets and item.market not in markets:
                continue
            if item.odds_decimal is None or item.fair_prob is None or item.edge is None:
                continue
            if item.edge < min_edge:
                continue
            if item.odds_decimal > max_odds:
                continue
            if item.fair_prob < min_prob:
                continue

            kelly = 0.0
            if item.fair_prob is not None:
                odds_dec = float(item.odds_decimal)
                if odds_dec > 1.0:
                    b = odds_dec - 1.0
                    kelly = max(0.0, (float(item.fair_prob) * odds_dec - 1.0) / b)

            match_kickoff = datetime.fromisoformat(str(row["kickoff_utc"]).replace("Z", "+00:00"))
            opportunities.append({
                "match": {
                    "id": row["match_id"],
                    "home_team": row["home"],
                    "away_team": row["away"],
                    "competition": row["competition"],
                    "date": match_kickoff.date().isoformat(),
                    "kickoff": match_kickoff.astimezone().strftime("%H:%M"),
                    "status": "scheduled" if match_kickoff >= now else "finished",
                },
                "market": item.market,
                "outcome": item.selection,
                "market_odds": float(item.odds_decimal),
                "fair_odds": float(item.fair_odds or 0.0),
                "probability": float(item.fair_prob),
                "edge": float(item.edge),
                "ev": float(item.ev_per_unit or 0.0),
                "kelly_fraction": kelly,
                "kelly_stake": kelly * bankroll,
                "fractional_kelly_stake": kelly * bankroll * 0.25,
                "confidence_interval": _confidence_interval(report, item.market, item.selection),
                "divergence_from_market": float(item.line_value_pct or 0.0),
                "risk_rating": _risk_rating(float(item.fair_prob), float(item.odds_decimal)),
            })

    return {
        "opportunities": opportunities,
        "filters_applied": req.model_dump(),
        "total_matches_scanned": len(match_rows),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


@router.get("/players/projections")
def players_projections(
    request: Request,
    team: Optional[str] = None,
    match_id: Optional[str] = None,
    competition: Optional[str] = None,
) -> Dict[str, Any]:
    _require_token(request)
    players = []
    match = None
    if match_id:
        ctx = get_match_context_by_id(match_id)
        sim_out = run_match_simulation(
            match_id=ctx["match"].match_id,
            data_snapshot_id=ctx["context"].data_snapshot_id,
            n_sims=10000,
            seed=42,
            model_version="mc_v1",
            model_inputs=ctx["model_inputs"],
        )
        proj = get_player_projections_for_match(ctx["match"], sim_out, limit=10)
        match = {
            "id": ctx["match"].match_id,
            "home_team": ctx["match"].home.name,
            "away_team": ctx["match"].away.name,
            "competition": ctx["match"].competition,
            "date": ctx["match"].kickoff_utc.date().isoformat(),
            "kickoff": ctx["match"].kickoff_utc.astimezone().strftime("%H:%M"),
            "status": "scheduled" if ctx["match"].kickoff_utc >= datetime.now(timezone.utc) else "finished",
        }
        players = (proj.home or []) + (proj.away or [])
    elif team and competition:
        comp_code = _competition_code(competition)
        season = None
        with get_conn() as conn:
            row = conn.execute(
                "SELECT season FROM matches WHERE competition = ? ORDER BY kickoff_utc DESC LIMIT 1",
                (comp_code,),
            ).fetchone()
        if row and row["season"]:
            season = int(str(row["season"]).split("/")[0])
        if season:
            players = list_player_projections(comp_code, season, team=team, limit=25)

    out_players = []
    for p in players:
        out_players.append({
            "id": p.player_id,
            "name": p.player_name,
            "team": p.team,
            "position": p.position or "",
            "xg": float(p.expected_xg or p.xg or 0.0),
            "xa": float(p.expected_xa or p.xa or 0.0),
            "xg_share": float(p.xg_share or 0.0),
            "xa_share": float(p.xa_share or 0.0),
            "expected_gi": float(p.expected_gi or p.gi_per90 or 0.0),
            "minutes_projection": float(p.minutes or 0.0),
            "form_rating": float(p.gi_per90 or 0.0),
        })

    return {"players": out_players, "match": match, "team": team}


@router.get("/audit/history")
def audit_history(
    request: Request,
    page: int = 1,
    per_page: int = 20,
) -> Dict[str, Any]:
    _require_token(request)
    if page < 1 or per_page < 1:
        raise HTTPException(status_code=400, detail="Invalid pagination.")

    offset = (page - 1) * per_page
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT request_id, created_at_utc, payload_json
            FROM audit_log
            ORDER BY created_at_utc DESC
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM audit_log").fetchone()["c"]

    runs = []
    for r in rows:
        try:
            report = MatchAnalysisReport.model_validate_json(r["payload_json"])
        except Exception:
            continue
        ui_report = report_to_ui_analyze(report)
        result = "bet" if report.recommendations else ("no_bet" if report.no_bet else "error")
        runs.append({
            "id": r["request_id"],
            "timestamp": r["created_at_utc"],
            "match": ui_report["match"],
            "result": result,
            "selections_made": len(report.recommendations or []),
            "no_bet_reasons": report.no_bet.reason_codes if report.no_bet else None,
            "snapshot": ui_report["audit_snapshot"],
        })

    return {
        "runs": runs,
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/audit/{run_id}")
def audit_detail(run_id: str, request: Request) -> Dict[str, Any]:
    _require_token(request)
    with get_conn() as conn:
        row = conn.execute(
            "SELECT request_id, payload_json, created_at_utc FROM audit_log WHERE request_id = ?",
            (run_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Audit run not found.")

    report = MatchAnalysisReport.model_validate_json(row["payload_json"])
    ui_report = report_to_ui_analyze(report)
    result = "bet" if report.recommendations else ("no_bet" if report.no_bet else "error")
    run = {
        "id": row["request_id"],
        "timestamp": row["created_at_utc"],
        "match": ui_report["match"],
        "result": result,
        "selections_made": len(report.recommendations or []),
        "no_bet_reasons": report.no_bet.reason_codes if report.no_bet else None,
        "snapshot": ui_report["audit_snapshot"],
    }

    return {"run": run, "full_analysis": ui_report}

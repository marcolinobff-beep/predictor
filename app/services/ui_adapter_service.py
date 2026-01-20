from __future__ import annotations

from datetime import datetime, timezone, date
from typing import Dict, List, Optional, Tuple, Any

from app.models.schemas import MatchAnalysisReport, MatchRef
from app.services.prediction_service import build_day_predictions, summary_for_prediction


COMP_LABELS = {
    "Serie_A": "Serie A",
    "Serie_B": "Serie B",
    "EPL": "Premier League",
    "Bundesliga": "Bundesliga",
    "La_Liga": "La Liga",
    "Ligue_1": "Ligue 1",
}


def _competition_label(code: Optional[str]) -> str:
    if not code:
        return ""
    return COMP_LABELS.get(code, code.replace("_", " "))


def _iso_date(dt: datetime) -> str:
    return dt.date().isoformat()


def _iso_time(dt: datetime) -> str:
    return dt.astimezone().strftime("%H:%M")


def _status_for_kickoff(kickoff: datetime) -> str:
    now = datetime.now(timezone.utc)
    return "scheduled" if kickoff >= now else "finished"


def _market_key(market: str, selection: str) -> Optional[str]:
    if market == "1X2":
        return {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(selection)
    if market == "OU_2.5":
        return {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(selection)
    if market == "BTTS":
        return {"YES": "btts_yes", "NO": "btts_no"}.get(selection)
    return None


def _market_label(market: str) -> str:
    if market == "1X2":
        return "1X2"
    if market == "OU_2.5":
        return "Over/Under 2.5"
    if market == "BTTS":
        return "BTTS"
    return market


def _selection_label(market: str, selection: str) -> str:
    if market == "1X2":
        return {"HOME": "1", "DRAW": "X", "AWAY": "2"}.get(selection, selection)
    if market == "OU_2.5":
        return {"OVER": "Over 2.5", "UNDER": "Under 2.5"}.get(selection, selection)
    if market == "BTTS":
        return {"YES": "BTTS Yes", "NO": "BTTS No"}.get(selection, selection)
    return selection


def _kelly_fraction(p: float, odds: float) -> float:
    if p <= 0 or odds <= 1.0:
        return 0.0
    b = odds - 1.0
    return max(0.0, (p * odds - 1.0) / b)


def match_ref_to_ui(match: MatchRef) -> Dict[str, Any]:
    return {
        "id": match.match_id,
        "home_team": match.home.name,
        "away_team": match.away.name,
        "competition": _competition_label(match.competition),
        "date": _iso_date(match.kickoff_utc),
        "kickoff": _iso_time(match.kickoff_utc),
        "status": _status_for_kickoff(match.kickoff_utc),
    }


def report_to_ui_analyze(report: MatchAnalysisReport) -> Dict[str, Any]:
    match = match_ref_to_ui(report.match)
    probs = (report.simulation_outputs.probs if report.simulation_outputs else {}) or {}
    intervals = (report.simulation_outputs.intervals if report.simulation_outputs else {}) or {}
    derived = (report.model_outputs.derived or {}) if report.model_outputs else {}

    eval_index = {}
    for item in report.market_evaluation or []:
        eval_index[(item.market, item.selection)] = item

    def _prob_entry(outcome: str, key: str, market: str, selection: str) -> Dict[str, Any]:
        p = float(probs.get(key, 0.0) or 0.0)
        fair = (1.0 / p) if p > 0 else 0.0
        item = eval_index.get((market, selection))
        return {
            "outcome": outcome,
            "probability": p,
            "fair_odds": fair,
            "market_odds": item.odds_decimal if item else None,
            "edge": item.edge if item else None,
            "ev": item.ev_per_unit if item else None,
        }

    probabilities = {
        "result_1x2": [
            _prob_entry("1", "home_win", "1X2", "HOME"),
            _prob_entry("X", "draw", "1X2", "DRAW"),
            _prob_entry("2", "away_win", "1X2", "AWAY"),
        ],
        "over_under": [
            _prob_entry("Over 2.5", "over_2_5", "OU_2.5", "OVER"),
            _prob_entry("Under 2.5", "under_2_5", "OU_2.5", "UNDER"),
        ],
        "btts": [
            _prob_entry("BTTS Yes", "btts_yes", "BTTS", "YES"),
            _prob_entry("BTTS No", "btts_no", "BTTS", "NO"),
        ],
    }

    fair_odds = {
        "home": (1.0 / probs["home_win"]) if probs.get("home_win") else 0.0,
        "draw": (1.0 / probs["draw"]) if probs.get("draw") else 0.0,
        "away": (1.0 / probs["away_win"]) if probs.get("away_win") else 0.0,
        "over_2_5": (1.0 / probs["over_2_5"]) if probs.get("over_2_5") else 0.0,
        "under_2_5": (1.0 / probs["under_2_5"]) if probs.get("under_2_5") else 0.0,
        "btts_yes": (1.0 / probs["btts_yes"]) if probs.get("btts_yes") else 0.0,
        "btts_no": (1.0 / probs["btts_no"]) if probs.get("btts_no") else 0.0,
        "implied_prob_home": float(probs.get("home_win", 0.0) or 0.0),
        "implied_prob_draw": float(probs.get("draw", 0.0) or 0.0),
        "implied_prob_away": float(probs.get("away_win", 0.0) or 0.0),
    }

    edge_analysis = []
    for item in report.market_evaluation or []:
        key = _market_key(item.market, item.selection)
        interval = intervals.get(key) if key else None
        ci = (interval.get("lo"), interval.get("hi")) if isinstance(interval, dict) else (None, None)
        fair_prob = float(item.fair_prob) if item.fair_prob is not None else float(probs.get(key, 0.0) or 0.0)
        market_prob = float(item.implied_prob) if item.implied_prob is not None else (
            (1.0 / item.odds_decimal) if item.odds_decimal else 0.0
        )
        kelly = _kelly_fraction(fair_prob, float(item.odds_decimal))
        edge_analysis.append({
            "market": _market_label(item.market),
            "outcome": _selection_label(item.market, item.selection),
            "fair_prob": fair_prob,
            "market_prob": market_prob,
            "edge": float(item.edge) if item.edge is not None else 0.0,
            "ev": float(item.ev_per_unit) if item.ev_per_unit is not None else 0.0,
            "kelly_fraction": kelly,
            "recommended_stake": kelly,
            "confidence_interval": [ci[0] if ci[0] is not None else 0.0, ci[1] if ci[1] is not None else 1.0],
        })

    scorelines = []
    for s in (report.simulation_outputs.scoreline_topk if report.simulation_outputs else []) or []:
        scorelines.append({
            "home_goals": s.get("home_goals"),
            "away_goals": s.get("away_goals"),
            "probability": s.get("p", 0.0),
        })

    data_sources = []
    if report.web_intel:
        for s in report.web_intel.sources or []:
            data_sources.append({
                "name": s.source_id,
                "last_updated": s.fetched_at_utc.isoformat().replace("+00:00", "Z"),
                "coverage": float(s.reliability_score),
            })

    tool_runs = []
    for tr in report.audit.tool_runs:
        tool_runs.append({
            "tool": tr.tool_name,
            "status": "success" if tr.status == "OK" else ("error" if tr.status == "ERROR" else "skipped"),
            "duration_ms": int((tr.ended_at_utc - tr.started_at_utc).total_seconds() * 1000),
            "message": tr.error,
        })

    audit_snapshot = {
        "timestamp": report.audit.generated_at_utc.isoformat().replace("+00:00", "Z"),
        "data_sources": data_sources,
        "model_version": report.model_outputs.model_version if report.model_outputs else "n/a",
        "parameters": report.simulation_outputs.diagnostics if report.simulation_outputs else {},
        "tool_runs": tool_runs,
        "no_bet_reasons": report.no_bet.reason_codes if report.no_bet else None,
    }

    drivers = derived.get("drivers") or []
    if isinstance(drivers, list):
        drivers = drivers[:5]
    else:
        drivers = []

    analysis_pro = {
        "confidence": derived.get("model_confidence"),
        "drivers": drivers,
        "tactical": derived.get("tactical"),
        "lineup": derived.get("lineup"),
        "form": derived.get("form"),
        "schedule": report.match_context.schedule_factors if report.match_context else {},
        "kpi": derived.get("kpi"),
        "scenario": derived.get("scenario_analysis"),
    }

    return {
        "match": match,
        "probabilities": probabilities,
        "fair_odds": fair_odds,
        "edge_analysis": edge_analysis,
        "top_scorelines": scorelines,
        "audit_snapshot": audit_snapshot,
        "analysis_pro": analysis_pro,
    }


def build_ui_slate(
    day_utc: date,
    competition: Optional[str],
    n_sims: int = 50000,
    seed: int = 42,
) -> Dict[str, Any]:
    predictions, schedules = build_day_predictions(day_utc, competition, n_sims, seed)

    matches: List[Dict[str, Any]] = []
    for pred in predictions:
        p = pred.probs or {}
        top_label, top_prob = max(
            [("1", p.get("home_win", 0.0)), ("X", p.get("draw", 0.0)), ("2", p.get("away_win", 0.0))],
            key=lambda x: x[1],
        )
        matches.append({
            "match": {
                "id": pred.match_id,
                "home_team": pred.home,
                "away_team": pred.away,
                "competition": _competition_label(pred.competition),
                "date": pred.kickoff_utc.date().isoformat(),
                "kickoff": _iso_time(pred.kickoff_utc),
                "status": _status_for_kickoff(pred.kickoff_utc),
            },
            "summary": summary_for_prediction(pred),
            "top_pick": {
                "market": "1X2",
                "outcome": top_label,
                "probability": float(top_prob),
                "edge": 0.0,
            },
            "xg_home": float(pred.lambda_home),
            "xg_away": float(pred.lambda_away),
            "form_home": "-----",
            "form_away": "-----",
        })

    def _selection_from_label(label: str) -> Tuple[str, str]:
        if label in ("1", "X", "2"):
            return ("1X2", label)
        if label in ("1X", "X2", "12"):
            return ("Doppia Chance", label)
        if "Over" in label or "Under" in label:
            return ("Over/Under 2.5", label)
        if "BTTS" in label:
            return ("BTTS", label)
        return ("Combo", label)

    def _build_ticket(name: str, difficulty: str, picks: List[Any]) -> Dict[str, Any]:
        selections = []
        combined_odds = 1.0
        combined_prob = 1.0
        for p in picks:
            market, outcome = _selection_from_label(p.label)
            prob = float(p.prob or 0.0)
            odds = (1.0 / prob) if prob > 0 else 0.0
            combined_odds *= odds if odds > 0 else 1.0
            combined_prob *= prob if prob > 0 else 1.0
            selections.append({
                "match": f"{p.home} vs {p.away}",
                "market": market,
                "outcome": outcome,
                "odds": odds,
                "probability": prob,
                "edge": 0.0,
            })
        expected_value = (combined_prob * combined_odds - 1.0) if selections else 0.0
        return {
            "name": name,
            "difficulty": difficulty,
            "selections": selections,
            "combined_odds": combined_odds if selections else 0.0,
            "combined_probability": combined_prob if selections else 0.0,
            "expected_value": expected_value,
            "suggested_stake": max(0.0, expected_value) * 0.02,
        }

    tickets = {
        "easy": _build_ticket("Schedina Facile", "easy", schedules.get("safe", [])),
        "medium": _build_ticket("Schedina Media", "medium", schedules.get("medium", [])),
        "hard": _build_ticket("Schedina Difficile", "hard", schedules.get("risky", [])),
    }

    return {
        "date": day_utc.isoformat(),
        "competition": _competition_label(competition) if competition else "",
        "matches": matches,
        "tickets": tickets,
    }

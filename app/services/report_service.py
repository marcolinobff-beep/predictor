from __future__ import annotations

from datetime import datetime, timezone, timedelta
from uuid import uuid4
from typing import Optional
import json

from fastapi import HTTPException

from app.core.config import settings
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.calibration_policy import should_calibrate_1x2
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs
from app.core.temp_scale import apply_temp_scale_1x2, get_temp_scale
from app.db.sqlite import get_conn
from app.models.schemas import MatchAnalysisReport, Audit, AuditToolRun, NoBet, MatchRef, TeamRef

from app.services.context_service import get_match_context, get_match_context_by_id
from app.services.web_intel_service import get_web_intel
from app.services.simulation_service import run_match_simulation
from app.services.market_eval_service import evaluate_markets
from app.services.market_rules_service import get_market_rules, get_betting_gate, is_betting_enabled
from app.services.player_projection_service import get_player_projections_for_match
from app.services.kpi_service import get_kpi_status


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _relax_rules_for_historical(match_kickoff: datetime | None, rules: dict) -> dict:
    if not match_kickoff or not rules:
        return rules
    max_age = float(rules.get("max_odds_age_hours", 12))
    if match_kickoff < (_now_utc() - timedelta(hours=max_age)):
        relaxed = dict(rules)
        relaxed["max_odds_age_hours"] = 999999
        return relaxed
    return rules


def _scenario_probs(
    lam_h: float,
    lam_a: float,
    competition: str,
    season_label: str | None,
    kickoff_utc: datetime | None,
) -> dict:
    rho = get_rho(settings.dc_params_path, competition)
    probs = match_probs(lam_h, lam_a, cap=8, rho=rho)

    temp_scale = get_temp_scale(settings.temp_scale_path, competition)
    if temp_scale:
        probs = apply_temp_scale_1x2(probs, temp_scale)

    cal = load_calibration(settings.calibration_by_season_path) or load_calibration(settings.calibration_path)
    cal = select_league_calibration(cal, competition)
    cal_sel = select_calibration(cal, season_label, kickoff_utc) if cal else None
    if cal_sel:
        calibrated = apply_calibration(probs, cal_sel)
        if not should_calibrate_1x2(settings.calibration_policy_path, competition, default=True):
            for key in ("home_win", "draw", "away_win"):
                calibrated[key] = probs.get(key, calibrated.get(key))
        probs = calibrated

    return probs


def _scenario_analysis(match_ref: MatchRef, ctx_out: dict, sim_out) -> dict:
    features = (ctx_out.get("model_inputs") or {}).get("features") or {}
    lam_h = float(features.get("lambda_home", 0.0) or 0.0)
    lam_a = float(features.get("lambda_away", 0.0) or 0.0)
    if lam_h <= 0 or lam_a <= 0:
        return {"scenarios": []}

    base_probs = sim_out.probs if sim_out and sim_out.probs else _scenario_probs(
        lam_h, lam_a, match_ref.competition, match_ref.season, match_ref.kickoff_utc
    )

    model_out = ctx_out.get("model_outputs")
    derived = model_out.derived if model_out else {}
    lineup = derived.get("lineup") or {}
    tactical = derived.get("tactical") or {}

    penalty_h = float(lineup.get("penalty_home") or 0.0)
    penalty_a = float(lineup.get("penalty_away") or 0.0)
    extra_h = max(0.05, min(0.12, penalty_h * 0.6)) if lineup else 0.07
    extra_a = max(0.05, min(0.12, penalty_a * 0.6)) if lineup else 0.07

    lam_h_lineup = max(0.2, lam_h * (1.0 - extra_h))
    lam_a_lineup = max(0.2, lam_a * (1.0 - extra_a))

    tempo = tactical.get("tempo")
    tempo_factor = 0.85
    if tempo == "low":
        tempo_factor = 0.82
    elif tempo == "high":
        tempo_factor = 0.88
    lam_h_blocked = max(0.2, lam_h * tempo_factor)
    lam_a_blocked = max(0.2, lam_a * tempo_factor)

    scenarios = [
        {
            "id": "base",
            "label": "base",
            "lambda_home": round(lam_h, 3),
            "lambda_away": round(lam_a, 3),
            "probs": base_probs,
        },
        {
            "id": "lineup_downside",
            "label": "lineup_peggiori",
            "lambda_home": round(lam_h_lineup, 3),
            "lambda_away": round(lam_a_lineup, 3),
            "probs": _scenario_probs(
                lam_h_lineup, lam_a_lineup, match_ref.competition, match_ref.season, match_ref.kickoff_utc
            ),
        },
        {
            "id": "low_tempo",
            "label": "partita_bloccata",
            "lambda_home": round(lam_h_blocked, 3),
            "lambda_away": round(lam_a_blocked, 3),
            "probs": _scenario_probs(
                lam_h_blocked, lam_a_blocked, match_ref.competition, match_ref.season, match_ref.kickoff_utc
            ),
        },
    ]

    keys = ["home_win", "draw", "away_win", "over_2_5", "under_2_5", "btts_yes", "btts_no"]
    ranges: dict = {}
    sensitivity: dict = {}
    for key in keys:
        vals = [s["probs"].get(key) for s in scenarios if s.get("probs") and s["probs"].get(key) is not None]
        if not vals:
            continue
        ranges[key] = {"min": round(min(vals), 3), "max": round(max(vals), 3)}
        base_val = base_probs.get(key)
        if base_val is None:
            continue
        sensitivity[key] = round(max(abs(v - base_val) for v in vals), 3)

    return {
        "scenarios": scenarios,
        "ranges": ranges,
        "sensitivity": sensitivity,
        "base_source": "simulation" if sim_out and sim_out.probs else "dc",
    }


def _fetch_match_row(match_id: str):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM matches WHERE match_id = ?",
            (match_id,),
        ).fetchone()


def _fetch_match_row_by_details(home: str, away: str, competition: str, kickoff_utc: datetime):
    kickoff_iso = kickoff_utc.isoformat().replace("+00:00", "Z")
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM matches
            WHERE competition = ?
              AND home = ?
              AND away = ?
              AND kickoff_utc = ?
            """,
            (competition, home, away, kickoff_iso),
        ).fetchone()


def _row_to_matchref(row) -> MatchRef:
    return MatchRef(
        match_id=row["match_id"],
        competition=row["competition"],
        season=row["season"],
        kickoff_utc=datetime.fromisoformat(row["kickoff_utc"].replace("Z", "+00:00")),
        home=TeamRef(name=row["home"]),
        away=TeamRef(name=row["away"]),
        venue=row["venue"],
    )


def _persist_audit_payload(request_id: str, payload_json: str) -> None:
    """
    Persist audit payload in SQLite. Non deve mai rompere la risposta dell'API.
    """
    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO audit_log (request_id, created_at_utc, payload_json)
                VALUES (?, ?, ?)
                """,
                (request_id, _now_utc().isoformat(), payload_json),
            )
    except Exception:
        # Non blocchiamo l'API per audit logging
        return


def _kpi_payload(kpi_status) -> dict:
    return {
        "status": kpi_status.status,
        "season": kpi_status.season,
        "phase": kpi_status.phase,
        "logloss_1x2": kpi_status.logloss_1x2,
        "brier_1x2": kpi_status.brier_1x2,
        "roi_1x2": kpi_status.roi_1x2,
        "picks_1x2": kpi_status.picks_1x2,
        "brier_by_market": kpi_status.brier_by_market,
        "logloss_by_market": kpi_status.logloss_by_market,
        "roi_by_market": kpi_status.roi_by_market,
        "reasons": kpi_status.reasons,
    }


def _apply_kpi_warning(recs, kpi_status) -> None:
    if not recs or not kpi_status or kpi_status.status != "WARN":
        return
    for rec in recs:
        rec.stake_fraction = float(rec.stake_fraction) * 0.5
        if rec.rationale is None:
            rec.rationale = []
        rec.rationale.append("stake ridotto per warning KPI (ROI recente negativo)")


def _apply_model_confidence(recs, model_conf: dict | None) -> None:
    if not recs or not model_conf:
        return
    score = model_conf.get("score")
    if score is None:
        return
    score = float(score)
    if score >= 0.55:
        return
    factor = 0.6 if score < 0.45 else 0.8
    for rec in recs:
        rec.stake_fraction = float(rec.stake_fraction) * factor
        if rec.rationale is None:
            rec.rationale = []
        rec.rationale.append(f"stake ridotto per bassa confidence modello ({score:.2f})")


def _low_confidence_nobet(model_conf: dict | None, rules: dict) -> Optional[NoBet]:
    if not model_conf or not rules:
        return None
    min_conf = rules.get("min_model_confidence")
    if min_conf is None:
        return None
    try:
        min_conf = float(min_conf)
    except (TypeError, ValueError):
        return None
    if min_conf <= 0:
        return None
    score = model_conf.get("score")
    if score is None:
        return None
    score = float(score)
    if score >= min_conf:
        return None
    return NoBet(
        reason_codes=["LOW_MODEL_CONFIDENCE"],
        explanation=[f"Confidence modello troppo bassa ({score:.2f} < {min_conf:.2f})."],
    )


def analyze_match_by_id(req) -> MatchAnalysisReport:
    request_id = str(uuid4())
    tool_runs: list[AuditToolRun] = []

    match_row = _fetch_match_row(req.match_id)
    kickoff_utc = None
    if match_row:
        kickoff_utc = datetime.fromisoformat(match_row["kickoff_utc"].replace("Z", "+00:00"))

    # 1) web intel (lineups/odds/news) first
    t0 = _now_utc()
    try:
        web_out = get_web_intel(req.match_id, kickoff_utc)
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="web_intel", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="web_intel", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        web_out = None

    # 2) context by id
    t0 = _now_utc()
    try:
        ctx_out = get_match_context_by_id(req.match_id)
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="get_match_context", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="get_match_context", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))

        match_ref = _row_to_matchref(match_row) if match_row else MatchRef(
            match_id=req.match_id,
            competition="UNKNOWN",
            season="UNKNOWN",
            kickoff_utc=_now_utc(),
            home=TeamRef(name="UNKNOWN"),
            away=TeamRef(name="UNKNOWN"),
            venue=None,
        )
        report = MatchAnalysisReport(
            status="MISSING_DATA",
            match=match_ref,
            web_intel=web_out,
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
            ),
            no_bet=NoBet(reason_codes=["MISSING_CONTEXT"], explanation=[str(e)]),
            errors=[str(e)],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    match_ref = ctx_out["match"]

    # 3) simulation
    t0 = _now_utc()
    try:
        sim_out = run_match_simulation(
            match_id=match_ref.match_id,
            data_snapshot_id=ctx_out["context"].data_snapshot_id,
            n_sims=req.n_sims,
            seed=req.seed,
            model_version="mc_v1",
            model_inputs=ctx_out["model_inputs"],
        )
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="run_match_simulation", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="run_match_simulation", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        report = MatchAnalysisReport(
            status="MISSING_DATA",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
            ),
            no_bet=NoBet(
                reason_codes=["MISSING_SIMULATION"],
                explanation=[str(e)]
            ),
            errors=[str(e)],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    # 3b) scenario analysis
    try:
        ctx_out["model_outputs"].derived["scenario_analysis"] = _scenario_analysis(match_ref, ctx_out, sim_out)
    except Exception:
        ctx_out["model_outputs"].derived["scenario_analysis"] = {"scenarios": []}

    # 3c) player projections
    t0 = _now_utc()
    try:
        player_proj = get_player_projections_for_match(match_ref, sim_out)
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="player_projections", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="player_projections", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        player_proj = None

    kpi_status = get_kpi_status(
        match_ref.competition,
        match_ref.season,
        kickoff_utc=match_ref.kickoff_utc,
    )
    if kpi_status:
        ctx_out["model_outputs"].derived["kpi"] = _kpi_payload(kpi_status)
        if kpi_status.status == "BLOCK":
            report = MatchAnalysisReport(
                status="OK",
                match=match_ref,
                match_context=ctx_out["context"],
                web_intel=web_out,
                model_outputs=ctx_out["model_outputs"],
                simulation_outputs=sim_out,
                player_projections=player_proj,
                market_evaluation=[],
                recommendations=[],
                no_bet=NoBet(
                    reason_codes=["KPI_MODEL_UNRELIABLE"] + kpi_status.reasons,
                    explanation=["KPI backtest sotto soglia: betting disabilitato in sicurezza."],
                ),
                audit=Audit(
                    request_id=request_id,
                    generated_at_utc=_now_utc(),
                    service_version=settings.service_version,
                    tool_runs=tool_runs,
                    data_snapshot_id=ctx_out["context"].data_snapshot_id,
                    web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                    simulation_id=sim_out.meta.simulation_id,
                ),
                errors=[],
            )
            _persist_audit_payload(request_id, report.model_dump_json())
            return report

    # --- ODDS RECENCY GATE (MVP) ---
    market_rules = _relax_rules_for_historical(match_ref.kickoff_utc, get_market_rules())
    max_odds_age_hours = float(market_rules.get("max_odds_age_hours", 12))
    odds_list = (web_out.odds if web_out else [])
    stale = False

    def _parse_dt(v):
        # v può essere datetime oppure stringa ISO
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        s = str(v).replace("Z", "+00:00")
        return datetime.fromisoformat(s)

    dts = [_parse_dt(o.retrieved_at_utc) for o in odds_list if getattr(o, "retrieved_at_utc", None)]
    if dts:
        latest_dt = max(dts)
        age_hours = (_now_utc() - latest_dt).total_seconds() / 3600.0
        if age_hours > max_odds_age_hours:
            stale = True
    else:
        # se non hai timestamp, trattalo come stale
        stale = True

    if stale:
        report = MatchAnalysisReport(
            status="OK",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            simulation_outputs=sim_out,
            player_projections=player_proj,
            market_evaluation=[],
            recommendations=[],
            no_bet=NoBet(
                reason_codes=["STALE_ODDS"],
                explanation=[f"Quote troppo vecchie (> {max_odds_age_hours}h): mercato non valutato per evitare edge falsi."]
            ),
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                simulation_id=sim_out.meta.simulation_id,
            ),
            errors=[]
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report




    # --- KICKOFF GATE: se le sole quote disponibili sono post-kickoff, non fare betting ---
    if web_out and any(n == "ODDS_POST_KICKOFF_ONLY" for n in (web_out.notes or [])):
        report = MatchAnalysisReport(
            status="OK",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            simulation_outputs=sim_out,
            player_projections=player_proj,
            market_evaluation=[],
            recommendations=[],
            no_bet=NoBet(
                reason_codes=["POST_KICKOFF_ODDS"],
                explanation=["In DB non risultano quote pre-kickoff per questo match: evito raccomandazioni per prevenire edge falsi."],
            ),
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                simulation_id=sim_out.meta.simulation_id,
            ),
            errors=[],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    model_conf = (ctx_out["model_outputs"].derived or {}).get("model_confidence")
    low_conf = _low_confidence_nobet(model_conf, market_rules)
    if low_conf:
        report = MatchAnalysisReport(
            status="OK",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            simulation_outputs=sim_out,
            player_projections=player_proj,
            market_evaluation=[],
            recommendations=[],
            no_bet=low_conf,
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                simulation_id=sim_out.meta.simulation_id,
            ),
            errors=[],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    # 4) markets
    t0 = _now_utc()
    try:
        market_eval, recs, nobet = evaluate_markets(
            match_id=match_ref.match_id,
            simulation_outputs=sim_out,
            odds=(web_out.odds if web_out else []),
            bankroll=req.bankroll,
            rules=market_rules,
        )
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="evaluate_markets", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="evaluate_markets", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        market_eval, recs = [], []
        nobet = NoBet(reason_codes=["MARKET_EVAL_ERROR"], explanation=[str(e)])

    # --- BETTING GATE (step1) ---
    gate = get_betting_gate()
    if not bool(gate.get("enabled", False)):
        recs = []
        reason = str(gate.get("reason", "DISABLED"))
        if nobet is None:
            nobet = NoBet(reason_codes=["BETTING_DISABLED"], explanation=[reason])
        else:
            try:
                nobet.reason_codes = ["BETTING_DISABLED"] + list(nobet.reason_codes or [])
                nobet.explanation = [reason] + list(nobet.explanation or [])
            except Exception:
                pass

    _apply_kpi_warning(recs, kpi_status)
    _apply_model_confidence(recs, model_conf)

    report = MatchAnalysisReport(
        status="OK",
        match=match_ref,
        match_context=ctx_out["context"],
        web_intel=web_out,
        model_outputs=ctx_out["model_outputs"],
        simulation_outputs=sim_out,
        player_projections=player_proj,
        market_evaluation=market_eval,
        recommendations=recs,
        no_bet=nobet,
        audit=Audit(
            request_id=request_id,
            generated_at_utc=_now_utc(),
            service_version=settings.service_version,
            tool_runs=tool_runs,
            data_snapshot_id=ctx_out["context"].data_snapshot_id,
            web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
            simulation_id=sim_out.meta.simulation_id,
        ),
        errors=[]
    )
    _persist_audit_payload(request_id, report.model_dump_json())
    return report


def analyze_match(req) -> MatchAnalysisReport:
    request_id = str(uuid4())
    tool_runs: list[AuditToolRun] = []

    web_out = None
    match_row = _fetch_match_row_by_details(req.home, req.away, req.competition, req.kickoff_utc)

    # 1) web intel (lineups/odds/news) first if match id is known
    if match_row:
        t0 = _now_utc()
        try:
            web_out = get_web_intel(match_row["match_id"], req.kickoff_utc)
            t1 = _now_utc()
            tool_runs.append(AuditToolRun(
                tool_name="web_intel", status="OK",
                started_at_utc=t0, ended_at_utc=t1
            ))
        except Exception as e:
            t1 = _now_utc()
            tool_runs.append(AuditToolRun(
                tool_name="web_intel", status="ERROR",
                started_at_utc=t0, ended_at_utc=t1, error=str(e)
            ))
            web_out = None

    # 2) get_match_context
    t0 = _now_utc()
    try:
        ctx_out = get_match_context(req.home, req.away, req.competition, req.kickoff_utc)
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="get_match_context", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except HTTPException as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="get_match_context", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e.detail)
        ))
        report = MatchAnalysisReport(
            status="MISSING_DATA",
            match=req.to_match_ref_fallback(),
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
            ),
            no_bet=NoBet(reason_codes=["MISSING_CONTEXT"], explanation=[str(e.detail)]),
            errors=[str(e.detail)],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    match_ref = ctx_out["match"]

    # 2b) web intel (fallback if not loaded)
    if web_out is None:
        t0 = _now_utc()
        try:
            web_out = get_web_intel(match_ref.match_id, match_ref.kickoff_utc)
            t1 = _now_utc()
            tool_runs.append(AuditToolRun(
                tool_name="web_intel", status="OK",
                started_at_utc=t0, ended_at_utc=t1
            ))
        except Exception as e:
            t1 = _now_utc()
            tool_runs.append(AuditToolRun(
                tool_name="web_intel", status="ERROR",
                started_at_utc=t0, ended_at_utc=t1, error=str(e)
            ))
            web_out = None

    # 3) simulation
    t0 = _now_utc()
    try:
        sim_out = run_match_simulation(
            match_id=match_ref.match_id,
            data_snapshot_id=ctx_out["context"].data_snapshot_id,
            n_sims=req.n_sims,
            seed=req.seed,
            model_version="mc_v1",
            model_inputs=ctx_out["model_inputs"],
        )
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="run_match_simulation", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="run_match_simulation", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        report = MatchAnalysisReport(
            status="MISSING_DATA",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
            ),
            no_bet=NoBet(reason_codes=["MISSING_SIMULATION"], explanation=[str(e)]),
            errors=[str(e)],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    # 3b) scenario analysis
    try:
        ctx_out["model_outputs"].derived["scenario_analysis"] = _scenario_analysis(match_ref, ctx_out, sim_out)
    except Exception:
        ctx_out["model_outputs"].derived["scenario_analysis"] = {"scenarios": []}

    # 3c) player projections
    t0 = _now_utc()
    try:
        player_proj = get_player_projections_for_match(match_ref, sim_out)
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="player_projections", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="player_projections", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        player_proj = None

    kpi_status = get_kpi_status(
        match_ref.competition,
        match_ref.season,
        kickoff_utc=match_ref.kickoff_utc,
    )
    if kpi_status:
        ctx_out["model_outputs"].derived["kpi"] = _kpi_payload(kpi_status)
        if kpi_status.status == "BLOCK":
            report = MatchAnalysisReport(
                status="OK",
                match=match_ref,
                match_context=ctx_out["context"],
                web_intel=web_out,
                model_outputs=ctx_out["model_outputs"],
                simulation_outputs=sim_out,
                player_projections=player_proj,
                market_evaluation=[],
                recommendations=[],
                no_bet=NoBet(
                    reason_codes=["KPI_MODEL_UNRELIABLE"] + kpi_status.reasons,
                    explanation=["KPI backtest sotto soglia: betting disabilitato in sicurezza."],
                ),
                audit=Audit(
                    request_id=request_id,
                    generated_at_utc=_now_utc(),
                    service_version=settings.service_version,
                    tool_runs=tool_runs,
                    data_snapshot_id=ctx_out["context"].data_snapshot_id,
                    web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                    simulation_id=sim_out.meta.simulation_id,
                ),
                errors=[],
            )
            _persist_audit_payload(request_id, report.model_dump_json())
            return report

    # --- ODDS RECENCY GATE (MVP) ---
    market_rules = _relax_rules_for_historical(match_ref.kickoff_utc, get_market_rules())
    max_odds_age_hours = float(market_rules.get("max_odds_age_hours", 12))
    odds_list = (web_out.odds if web_out else [])
    stale = False

    def _parse_dt(v):
        # v puo essere datetime oppure stringa ISO
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        s = str(v).replace("Z", "+00:00")
        return datetime.fromisoformat(s)

    dts = [_parse_dt(o.retrieved_at_utc) for o in odds_list if getattr(o, "retrieved_at_utc", None)]
    if dts:
        latest_dt = max(dts)
        age_hours = (_now_utc() - latest_dt).total_seconds() / 3600.0
        if age_hours > max_odds_age_hours:
            stale = True
    else:
        # se non hai timestamp, trattalo come stale
        stale = True

    if stale:
        report = MatchAnalysisReport(
            status="OK",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            simulation_outputs=sim_out,
            player_projections=player_proj,
            market_evaluation=[],
            recommendations=[],
            no_bet=NoBet(
                reason_codes=["STALE_ODDS"],
                explanation=[f"Quote troppo vecchie (> {max_odds_age_hours}h): mercato non valutato per evitare edge falsi."]
            ),
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                simulation_id=sim_out.meta.simulation_id,
            ),
            errors=[]
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    # --- KICKOFF GATE: se le sole quote disponibili sono post-kickoff, non fare betting ---
    if web_out and any(n == "ODDS_POST_KICKOFF_ONLY" for n in (web_out.notes or [])):
        report = MatchAnalysisReport(
            status="OK",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            simulation_outputs=sim_out,
            player_projections=player_proj,
            market_evaluation=[],
            recommendations=[],
            no_bet=NoBet(
                reason_codes=["POST_KICKOFF_ODDS"],
                explanation=["In DB non risultano quote pre-kickoff per questo match: evito raccomandazioni per prevenire edge falsi."],
            ),
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                simulation_id=sim_out.meta.simulation_id,
            ),
            errors=[],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    model_conf = (ctx_out["model_outputs"].derived or {}).get("model_confidence")
    low_conf = _low_confidence_nobet(model_conf, market_rules)
    if low_conf:
        report = MatchAnalysisReport(
            status="OK",
            match=match_ref,
            match_context=ctx_out["context"],
            web_intel=web_out,
            model_outputs=ctx_out["model_outputs"],
            simulation_outputs=sim_out,
            player_projections=player_proj,
            market_evaluation=[],
            recommendations=[],
            no_bet=low_conf,
            audit=Audit(
                request_id=request_id,
                generated_at_utc=_now_utc(),
                service_version=settings.service_version,
                tool_runs=tool_runs,
                data_snapshot_id=ctx_out["context"].data_snapshot_id,
                web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
                simulation_id=sim_out.meta.simulation_id,
            ),
            errors=[],
        )
        _persist_audit_payload(request_id, report.model_dump_json())
        return report

    # 4) markets
    t0 = _now_utc()
    try:
        market_eval, recs, nobet = evaluate_markets(
            match_id=match_ref.match_id,
            simulation_outputs=sim_out,
            odds=(web_out.odds if web_out else []),
            bankroll=req.bankroll,
            rules=market_rules,
        )
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="evaluate_markets", status="OK",
            started_at_utc=t0, ended_at_utc=t1
        ))
    except Exception as e:
        t1 = _now_utc()
        tool_runs.append(AuditToolRun(
            tool_name="evaluate_markets", status="ERROR",
            started_at_utc=t0, ended_at_utc=t1, error=str(e)
        ))
        market_eval, recs = [], []
        nobet = NoBet(reason_codes=["MARKET_EVAL_ERROR"], explanation=[str(e)])

    # --- BETTING GATE (step1) ---
    gate = get_betting_gate()
    if not bool(gate.get("enabled", False)):
        recs = []
        reason = str(gate.get("reason", "DISABLED"))
        if nobet is None:
            nobet = NoBet(reason_codes=["BETTING_DISABLED"], explanation=[reason])
        else:
            try:
                nobet.reason_codes = ["BETTING_DISABLED"] + list(nobet.reason_codes or [])
                nobet.explanation = [reason] + list(nobet.explanation or [])
            except Exception:
                pass

    _apply_kpi_warning(recs, kpi_status)
    _apply_model_confidence(recs, model_conf)

    report = MatchAnalysisReport(
        status="OK",
        match=match_ref,
        match_context=ctx_out["context"],
        web_intel=web_out,
        model_outputs=ctx_out["model_outputs"],
        simulation_outputs=sim_out,
        player_projections=player_proj,
        market_evaluation=market_eval,
        recommendations=recs,
        no_bet=nobet,
        audit=Audit(
            request_id=request_id,
            generated_at_utc=_now_utc(),
            service_version=settings.service_version,
            tool_runs=tool_runs,
            data_snapshot_id=ctx_out["context"].data_snapshot_id,
            web_snapshot_id=(web_out.web_snapshot_id if web_out else None),
            simulation_id=(sim_out.meta.simulation_id if sim_out else None),
        ),
        errors=[]
    )
    _persist_audit_payload(request_id, report.model_dump_json())
    return report

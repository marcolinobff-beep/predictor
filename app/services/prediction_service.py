from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple

from app.db.sqlite import get_conn
from app.services.context_service import get_match_context_by_id
from app.services.kpi_service import get_kpi_status
from app.services.lineup_refresh_service import refresh_lineups_for_day
from app.services.simulation_service import run_match_simulation


@dataclass
class MatchPrediction:
    match_id: str
    competition: Optional[str]
    kickoff_utc: datetime
    home: str
    away: str
    lambda_home: float
    lambda_away: float
    probs: Dict[str, float]
    lineup_info: Optional[Dict[str, object]] = None
    kpi_info: Optional[Dict[str, object]] = None
    schedule_info: Optional[Dict[str, object]] = None
    form_info: Optional[Dict[str, object]] = None
    tactical_info: Optional[Dict[str, object]] = None
    confidence_info: Optional[Dict[str, object]] = None


@dataclass
class PickSuggestion:
    match_id: str
    competition: Optional[str]
    home: str
    away: str
    label: str
    prob: float


LABEL_MAP = {
    "Serie_A": "Serie A",
    "Serie_B": "Serie B",
    "EPL": "Premier League",
    "Bundesliga": "Bundesliga",
    "La_Liga": "La Liga",
    "Ligue_1": "Ligue 1",
}


def _competition_label(competition: Optional[str]) -> str:
    if not competition:
        return "Top Leagues"
    return LABEL_MAP.get(competition, competition.replace("_", " "))


def _poisson_pmf(lam: float, k: int) -> float:
    if k < 0:
        return 0.0
    num = 1.0
    for i in range(1, k + 1):
        num *= lam / i
    return num * (2.718281828459045 ** (-lam))


def _combo_prob(lam_h: float, lam_a: float, cap: int = 8) -> Dict[str, float]:
    p_h = [_poisson_pmf(lam_h, k) for k in range(cap + 1)]
    p_a = [_poisson_pmf(lam_a, k) for k in range(cap + 1)]

    out = {
        "home_over_1_5": 0.0,
        "away_over_1_5": 0.0,
        "draw_under_2_5": 0.0,
    }

    for i, ph in enumerate(p_h):
        for j, pa in enumerate(p_a):
            p = ph * pa
            total = i + j
            if i > j and total >= 2:
                out["home_over_1_5"] += p
            if i < j and total >= 2:
                out["away_over_1_5"] += p
            if i == j and total <= 2:
                out["draw_under_2_5"] += p

    return out


def _iso_z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _list_matches_for_day(day_utc: date, competition: Optional[str]):
    start = datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    start_iso = _iso_z(start)
    end_iso = _iso_z(end)

    sql = """
        SELECT match_id, competition, season, kickoff_utc, home, away, venue
        FROM matches
        WHERE kickoff_utc >= ? AND kickoff_utc < ?
    """
    params: List[object] = [start_iso, end_iso]
    if competition:
        sql += " AND competition = ?"
        params.append(competition)
    sql += " ORDER BY kickoff_utc ASC"

    with get_conn() as conn:
        return conn.execute(sql, params).fetchall()


def _list_competitions_for_day(day_utc: date) -> List[str]:
    start = datetime(day_utc.year, day_utc.month, day_utc.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    start_iso = _iso_z(start)
    end_iso = _iso_z(end)
    sql = """
        SELECT DISTINCT competition
        FROM matches
        WHERE kickoff_utc >= ? AND kickoff_utc < ?
    """
    with get_conn() as conn:
        rows = conn.execute(sql, [start_iso, end_iso]).fetchall()
    return [r["competition"] for r in rows if r and r["competition"]]


def _main_1x2(probs: Dict[str, float]) -> Tuple[str, float]:
    items = [
        ("1", probs.get("home_win", 0.0)),
        ("X", probs.get("draw", 0.0)),
        ("2", probs.get("away_win", 0.0)),
    ]
    return max(items, key=lambda x: x[1])


def _double_chance(probs: Dict[str, float]) -> Tuple[str, float]:
    p1 = probs.get("home_win", 0.0)
    px = probs.get("draw", 0.0)
    p2 = probs.get("away_win", 0.0)
    items = [
        ("1X", p1 + px),
        ("X2", px + p2),
        ("12", p1 + p2),
    ]
    return max(items, key=lambda x: x[1])


def _double_chance_safe(probs: Dict[str, float]) -> Tuple[str, float]:
    p1 = probs.get("home_win", 0.0)
    px = probs.get("draw", 0.0)
    p2 = probs.get("away_win", 0.0)
    items = [
        ("1X", p1 + px),
        ("X2", px + p2),
    ]
    return max(items, key=lambda x: x[1])


def _totals_pick(probs: Dict[str, float]) -> Tuple[str, float]:
    p_over = probs.get("over_2_5", 0.0)
    p_under = probs.get("under_2_5", 0.0)
    if p_under >= p_over:
        return ("Under 2.5", p_under)
    return ("Over 2.5", p_over)


def _btts_pick(probs: Dict[str, float]) -> Tuple[str, float]:
    p_yes = probs.get("btts_yes", 0.0)
    p_no = probs.get("btts_no", 0.0)
    if p_no >= p_yes:
        return ("BTTS No", p_no)
    return ("BTTS Yes", p_yes)


def _analysis_line(pred: MatchPrediction) -> str:
    probs = pred.probs or {}
    p1 = probs.get("home_win", 0.0)
    px = probs.get("draw", 0.0)
    p2 = probs.get("away_win", 0.0)
    top_label, top_prob = _main_1x2(probs)

    if top_label == "1":
        if top_prob >= 0.45:
            base = "Casa favorita"
        else:
            base = "Leggero vantaggio casa"
    elif top_label == "2":
        if top_prob >= 0.45:
            base = "Trasferta favorita"
        else:
            base = "Leggero vantaggio ospite"
    else:
        base = "Gara equilibrata"

    total_label, total_prob = _totals_pick(probs)
    total_hint = ""
    if total_prob >= 0.55:
        total_hint = f" | Tendenza {total_label}"

    btts_label, btts_prob = _btts_pick(probs)
    btts_hint = ""
    if btts_prob >= 0.55:
        btts_hint = f" | {btts_label} probabile"

    form_hint = ""
    form = pred.form_info or {}
    notes = []
    xg_for_h = form.get("xg_for_delta_home")
    xg_for_a = form.get("xg_for_delta_away")
    xg_against_h = form.get("xg_against_delta_home")
    xg_against_a = form.get("xg_against_delta_away")
    fin_h = form.get("finishing_delta_form_home")
    fin_a = form.get("finishing_delta_form_away")

    if xg_for_h is not None and xg_for_h > 0.08:
        notes.append("attacco casa in forma")
    if xg_for_a is not None and xg_for_a > 0.08:
        notes.append("attacco ospite in forma")
    if xg_against_h is not None and xg_against_h < -0.08:
        notes.append("difesa casa solida")
    if xg_against_a is not None and xg_against_a < -0.08:
        notes.append("difesa ospite solida")
    if fin_h is not None and fin_h > 0.35:
        notes.append("casa over xG")
    if fin_a is not None and fin_a > 0.35:
        notes.append("ospite over xG")

    if notes:
        form_hint = " | " + "; ".join(notes[:2])

    p_line = f" | 1/X/2={p1*100:.0f}/{px*100:.0f}/{p2*100:.0f}%"
    return f"{base}{total_hint}{btts_hint}{form_hint}{p_line}"


def summary_for_prediction(pred: MatchPrediction) -> str:
    return _analysis_line(pred)


def _format_prob(p: float) -> str:
    return f"{p * 100:.1f}%"


def _pro_summary_line(pred: MatchPrediction) -> str:
    probs = pred.probs or {}
    p1 = probs.get("home_win")
    px = probs.get("draw")
    p2 = probs.get("away_win")
    parts = [
        f"{pred.home} vs {pred.away}",
        f"xG={pred.lambda_home:.2f}-{pred.lambda_away:.2f}",
    ]
    if p1 is not None and px is not None and p2 is not None:
        parts.append(f"1/X/2={_format_prob(p1)}/{_format_prob(px)}/{_format_prob(p2)}")
        if p1 > 0 and px > 0 and p2 > 0:
            parts.append(f"fair_odds=1 {1/p1:.2f} | X {1/px:.2f} | 2 {1/p2:.2f}")
    return " | ".join(parts)


def _pro_lineup_line(pred: MatchPrediction) -> Optional[str]:
    info = pred.lineup_info or {}
    src = info.get("lineup_source")
    cov_h = info.get("coverage_home")
    cov_a = info.get("coverage_away")
    abs_h = info.get("absence_share_home")
    abs_a = info.get("absence_share_away")
    pen_h = info.get("penalty_home")
    pen_a = info.get("penalty_away")
    parts = []
    if src:
        parts.append(f"src={src}")
    if cov_h is not None and cov_a is not None:
        parts.append(f"coverage={cov_h:.2f}/{cov_a:.2f}")
    if abs_h is not None and abs_a is not None:
        parts.append(f"abs_share={abs_h:.2f}/{abs_a:.2f}")
    if pen_h is not None and pen_a is not None:
        parts.append(f"penalty={pen_h:.2f}/{pen_a:.2f}")
    return "lineup: " + " | ".join(parts) if parts else None


def _pro_kpi_line(pred: MatchPrediction) -> Optional[str]:
    info = pred.kpi_info or {}
    status = info.get("status")
    if not status:
        return None
    parts = [f"status={status}"]
    phase = info.get("phase")
    if phase:
        parts.append(f"phase={phase}")
    if info.get("logloss_1x2") is not None:
        parts.append(f"logloss={info['logloss_1x2']:.3f}")
    if info.get("brier_1x2") is not None:
        parts.append(f"brier={info['brier_1x2']:.3f}")
    if info.get("roi_1x2") is not None:
        parts.append(f"roi={info['roi_1x2']:.3f}")
    brier_by = info.get("brier_by_market") or {}
    logloss_by = info.get("logloss_by_market") or {}
    if brier_by:
        b1 = brier_by.get("1X2")
        bo = brier_by.get("OU_2.5")
        bb = brier_by.get("BTTS")
        bits = []
        if b1 is not None:
            bits.append(f"1X2:{b1:.3f}")
        if bo is not None:
            bits.append(f"OU:{bo:.3f}")
        if bb is not None:
            bits.append(f"BTTS:{bb:.3f}")
        if bits:
            parts.append("brier_mkt=" + ",".join(bits))
    if logloss_by:
        l1 = logloss_by.get("1X2")
        lo = logloss_by.get("OU_2.5")
        lb = logloss_by.get("BTTS")
        bits = []
        if l1 is not None:
            bits.append(f"1X2:{l1:.3f}")
        if lo is not None:
            bits.append(f"OU:{lo:.3f}")
        if lb is not None:
            bits.append(f"BTTS:{lb:.3f}")
        if bits:
            parts.append("logloss_mkt=" + ",".join(bits))
    return "kpi: " + " | ".join(parts)


def _pro_schedule_line(pred: MatchPrediction) -> Optional[str]:
    info = pred.schedule_info or {}
    rest_h = info.get("rest_days_home")
    rest_a = info.get("rest_days_away")
    m7_h = info.get("matches_7d_home")
    m7_a = info.get("matches_7d_away")
    m14_h = info.get("matches_14d_home")
    m14_a = info.get("matches_14d_away")
    parts = []
    if rest_h is not None and rest_a is not None:
        parts.append(f"rest={float(rest_h):.1f}d/{float(rest_a):.1f}d")
    if m7_h is not None and m7_a is not None:
        parts.append(f"last7={int(m7_h)}/{int(m7_a)}")
    if m14_h is not None and m14_a is not None:
        parts.append(f"last14={int(m14_h)}/{int(m14_a)}")
    return "schedule: " + " | ".join(parts) if parts else None


def _pro_form_line(pred: MatchPrediction) -> Optional[str]:
    info = pred.form_info or {}
    xg_for_h = info.get("xg_for_delta_home")
    xg_for_a = info.get("xg_for_delta_away")
    xg_against_h = info.get("xg_against_delta_home")
    xg_against_a = info.get("xg_against_delta_away")
    fin_h = info.get("finishing_delta_form_home")
    fin_a = info.get("finishing_delta_form_away")
    parts = []
    if xg_for_h is not None and xg_for_a is not None:
        parts.append(f"xG_for delta={xg_for_h*100:.0f}%/{xg_for_a*100:.0f}%")
    if xg_against_h is not None and xg_against_a is not None:
        parts.append(f"xG_against delta={xg_against_h*100:.0f}%/{xg_against_a*100:.0f}%")
    if fin_h is not None and fin_a is not None:
        parts.append(f"finishing delta={fin_h:+.2f}/{fin_a:+.2f}")
    return "form: " + " | ".join(parts) if parts else None


def _pro_tactical_line(pred: MatchPrediction) -> Optional[str]:
    info = pred.tactical_info or {}
    tags = info.get("tags") or []
    if not tags:
        return None
    src = info.get("source", "n/a")
    matchup = info.get("matchup")
    parts = [f"src={src}", "tags=" + ", ".join(tags)]
    tempo = info.get("tempo")
    if tempo and tempo != "neutral":
        parts.append(f"tempo={tempo}")
    style = info.get("style_matchup") or {}
    indicator = style.get("indicator")
    if indicator and indicator != "neutral":
        parts.append(f"style={indicator}")
    if matchup:
        parts.append(f"matchup={matchup}")
    return "tactical: " + " | ".join(parts)


def _pro_confidence_line(pred: MatchPrediction) -> Optional[str]:
    info = pred.confidence_info or {}
    score = info.get("score")
    cov = info.get("lineup_coverage")
    stability = info.get("form_stability")
    data_q = info.get("data_quality_score")
    parts = []
    if score is not None:
        parts.append(f"score={float(score):.2f}")
    if cov is not None:
        parts.append(f"lineup={float(cov):.2f}")
    if stability is not None:
        parts.append(f"stability={float(stability):.2f}")
    if data_q is not None:
        parts.append(f"data={float(data_q):.2f}")
    return "confidence: " + " | ".join(parts) if parts else None


def _confidence_score(pred: MatchPrediction) -> float:
    info = pred.confidence_info or {}
    score = info.get("score")
    try:
        return float(score) if score is not None else 0.5
    except (TypeError, ValueError):
        return 0.5


def _confidence_factor(pred: MatchPrediction) -> float:
    info = pred.confidence_info or {}
    score = _confidence_score(pred)
    data_q = info.get("data_quality_score")
    lineup_cov = info.get("lineup_coverage")
    factor = min(1.0, 0.7 + 0.6 * score)
    try:
        if data_q is not None and float(data_q) < 0.6:
            factor *= 0.9
    except (TypeError, ValueError):
        pass
    try:
        if lineup_cov is not None and float(lineup_cov) < 0.3:
            factor *= 0.9
    except (TypeError, ValueError):
        pass
    return max(0.5, min(1.0, factor))


def _low_confidence(pred: MatchPrediction) -> bool:
    info = pred.confidence_info or {}
    score = _confidence_score(pred)
    data_q = info.get("data_quality_score")
    lineup_cov = info.get("lineup_coverage")
    if score < 0.45:
        return True
    try:
        if data_q is not None and float(data_q) < 0.5:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if lineup_cov is not None and float(lineup_cov) < 0.2:
            return True
    except (TypeError, ValueError):
        pass
    return False


def _safe_pick(pred: MatchPrediction) -> Optional[PickSuggestion]:
    if _low_confidence(pred):
        return None
    factor = _confidence_factor(pred)
    d_label, d_prob = _double_chance_safe(pred.probs)
    t_label, t_prob = _totals_pick(pred.probs)
    b_label, b_prob = _btts_pick(pred.probs)
    candidates = []
    if d_prob * factor >= 0.62:
        candidates.append((d_label, d_prob))
    if t_prob * factor >= 0.60:
        candidates.append((t_label, t_prob))
    if b_prob * factor >= 0.60:
        candidates.append((b_label, b_prob))
    if not candidates:
        return None
    label, prob = max(candidates, key=lambda x: x[1])
    return PickSuggestion(pred.match_id, pred.competition, pred.home, pred.away, label, prob)


def _medium_pick(pred: MatchPrediction) -> Optional[PickSuggestion]:
    if _low_confidence(pred):
        return None
    factor = _confidence_factor(pred)
    m_label, m_prob = _main_1x2(pred.probs)
    p1 = pred.probs.get("home_win", 0.0)
    px = pred.probs.get("draw", 0.0)
    p2 = pred.probs.get("away_win", 0.0)
    second_prob = sorted([p1, px, p2], reverse=True)[1] if max(p1, px, p2) > 0 else 0.0
    t_label, t_prob = _totals_pick(pred.probs)
    d_label, d_prob = _double_chance_safe(pred.probs)
    if (m_prob * factor) >= 0.42 and (m_prob - second_prob) >= 0.05:
        return PickSuggestion(pred.match_id, pred.competition, pred.home, pred.away, m_label, m_prob)
    if (t_prob * factor) >= 0.55:
        return PickSuggestion(pred.match_id, pred.competition, pred.home, pred.away, t_label, t_prob)
    if (d_prob * factor) >= 0.55:
        return PickSuggestion(pred.match_id, pred.competition, pred.home, pred.away, d_label, d_prob)
    return None


def _risky_pick(pred: MatchPrediction) -> Optional[PickSuggestion]:
    label, prob = _main_1x2(pred.probs)
    combos = _combo_prob(pred.lambda_home, pred.lambda_away)
    combo_label = label
    combo_prob = prob
    factor = _confidence_factor(pred)
    if label == "1":
        if combos["home_over_1_5"] * factor >= 0.30:
            combo_label = "1 + Over 1.5"
            combo_prob = combos["home_over_1_5"]
    elif label == "2":
        if combos["away_over_1_5"] * factor >= 0.30:
            combo_label = "2 + Over 1.5"
            combo_prob = combos["away_over_1_5"]
    else:
        if combos["draw_under_2_5"] * factor >= 0.22:
            combo_label = "X + Under 2.5"
            combo_prob = combos["draw_under_2_5"]
    return PickSuggestion(pred.match_id, pred.competition, pred.home, pred.away, combo_label, combo_prob)


def build_day_predictions(
    day_utc: date,
    competition: Optional[str],
    n_sims: int,
    seed: int,
) -> Tuple[List[MatchPrediction], Dict[str, object]]:
    if competition:
        try:
            refresh_lineups_for_day(day_utc, competition)
        except Exception:
            pass
    else:
        for comp in _list_competitions_for_day(day_utc):
            try:
                refresh_lineups_for_day(day_utc, comp)
            except Exception:
                pass
    rows = _list_matches_for_day(day_utc, competition)
    predictions: List[MatchPrediction] = []

    for r in rows:
        ctx = get_match_context_by_id(r["match_id"])
        sim_out = run_match_simulation(
            match_id=ctx["match"].match_id,
            data_snapshot_id=ctx["context"].data_snapshot_id,
            n_sims=n_sims,
            seed=seed,
            model_version="mc_v1",
            model_inputs=ctx["model_inputs"],
        )
        lineup_info = (ctx["model_outputs"].derived or {}).get("lineup")
        kpi_status = get_kpi_status(
            ctx["match"].competition,
            ctx["match"].season,
            kickoff_utc=ctx["match"].kickoff_utc,
        )
        kpi_info = None
        if kpi_status:
            kpi_info = {
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
        predictions.append(MatchPrediction(
            match_id=ctx["match"].match_id,
            competition=ctx["match"].competition,
            kickoff_utc=ctx["match"].kickoff_utc,
            home=ctx["match"].home.name,
            away=ctx["match"].away.name,
            lambda_home=float(ctx["model_inputs"]["features"].get("lambda_home", 0.0)),
            lambda_away=float(ctx["model_inputs"]["features"].get("lambda_away", 0.0)),
            probs=sim_out.probs,
            lineup_info=lineup_info,
            kpi_info=kpi_info,
            schedule_info=ctx["context"].schedule_factors if ctx.get("context") else None,
            form_info=(ctx["model_outputs"].derived or {}).get("form") if ctx.get("model_outputs") else None,
            tactical_info=(ctx["model_outputs"].derived or {}).get("tactical") if ctx.get("model_outputs") else None,
            confidence_info=(ctx["model_outputs"].derived or {}).get("model_confidence") if ctx.get("model_outputs") else None,
        ))

    low_conf = [pred.match_id for pred in predictions if _low_confidence(pred)]
    safe = [p for p in (_safe_pick(pred) for pred in predictions) if p]
    medium = [p for p in (_medium_pick(pred) for pred in predictions) if p]
    risky = [p for p in (_risky_pick(pred) for pred in predictions) if p]

    safe_sorted = sorted(safe, key=lambda p: p.prob, reverse=True)[:3]
    medium_sorted = sorted(medium, key=lambda p: p.prob, reverse=True)[:3]
    risky_sorted = sorted(risky, key=lambda p: p.prob)[:3]

    notes: List[str] = []
    if low_conf:
        notes.append(f"Selezioni escluse per bassa confidence: {len(low_conf)} match.")

    return predictions, {
        "safe": safe_sorted,
        "medium": medium_sorted,
        "risky": risky_sorted,
        "notes": notes,
    }


def build_day_prediction_text(
    day_utc: date,
    competition: Optional[str],
    n_sims: int,
    seed: int,
) -> str:
    predictions, schedules = build_day_predictions(day_utc, competition, n_sims, seed)

    if not predictions:
        return "Nessuna partita trovata per la data richiesta."

    comp_label = _competition_label(competition)
    date_label = day_utc.strftime("%d/%m/%Y")
    lines = [
        f"Ecco le partite di {comp_label} in programma oggi, {date_label},",
        "con pronostici e tre schedine divise per difficolta.",
        "",
        f"Partite di {comp_label} oggi ({date_label})",
    ]

    for p in predictions:
        kickoff = p.kickoff_utc.astimezone().strftime("%H:%M")
        if competition:
            lines.append(f"{kickoff} - {p.home} vs {p.away}")
        else:
            lines.append(f"{kickoff} - [{_competition_label(p.competition)}] {p.home} vs {p.away}")

    lines.append("")
    lines.append("Pronostici principali (analisi rapida)")
    for p in predictions:
        line = _analysis_line(p)
        if competition:
            lines.append(f"{p.home} vs {p.away}: {line}")
        else:
            lines.append(f"[{_competition_label(p.competition)}] {p.home} vs {p.away}: {line}")

    lines.append("")
    lines.append("Output Pro (sintesi)")
    for p in predictions:
        summary = _pro_summary_line(p)
        if competition:
            lines.append(summary)
        else:
            lines.append(f"[{_competition_label(p.competition)}] {summary}")
        lineup_line = _pro_lineup_line(p)
        if lineup_line:
            lines.append(f"  {lineup_line}")
        kpi_line = _pro_kpi_line(p)
        if kpi_line:
            lines.append(f"  {kpi_line}")
        schedule_line = _pro_schedule_line(p)
        if schedule_line:
            lines.append(f"  {schedule_line}")
        form_line = _pro_form_line(p)
        if form_line:
            lines.append(f"  {form_line}")
        tactical_line = _pro_tactical_line(p)
        if tactical_line:
            lines.append(f"  {tactical_line}")
        conf_line = _pro_confidence_line(p)
        if conf_line:
            lines.append(f"  {conf_line}")

    lines.append("")
    lines.append("Le tue 3 schedine consigliate")
    for label, picks in (("Schedina Facile", schedules["safe"]), ("Schedina Media", schedules["medium"]), ("Schedina Difficile", schedules["risky"])):
        lines.append(label)
        if not picks:
            lines.append("  Nessuna selezione affidabile trovata.")
            continue
        for pick in picks:
            if competition:
                lines.append(f"  {pick.home} vs {pick.away} -> {pick.label} ({pick.prob*100:.0f}%)")
            else:
                lines.append(
                    f"  [{_competition_label(pick.competition)}] {pick.home} vs {pick.away} -> {pick.label} ({pick.prob*100:.0f}%)"
                )

    notes = schedules.get("notes") or []
    if notes:
        lines.append("")
        lines.append("Note")
        for note in notes:
            lines.append(f"- {note}")

    return "\n".join(lines)

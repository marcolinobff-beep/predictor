from __future__ import annotations

import re
from datetime import date, datetime, timezone, timedelta
from typing import Optional, List
from types import SimpleNamespace

from app.db.sqlite import get_conn
from app.models.schemas import ChatResponse
from app.services.report_service import analyze_match_by_id
from app.services.feedback_service import add_chat_feedback
from app.services.chat_memory_service import ensure_session, update_session, add_message, get_recent_messages
from app.services.llm_service import rewrite_answer
from app.services.slate_service import build_slate_report
from app.core.text_utils import clean_person_name
from app.services.prediction_service import build_day_prediction_text


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _next_match_date(competition: Optional[str]) -> Optional[date]:
    now_iso = _now_utc().isoformat().replace("+00:00", "Z")
    sql = """
        SELECT kickoff_utc
        FROM matches
        WHERE kickoff_utc >= ?
    """
    params: List[object] = [now_iso]
    if competition:
        sql += " AND competition = ?"
        params.append(competition)
    sql += " ORDER BY kickoff_utc ASC LIMIT 1"

    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    dt = datetime.fromisoformat(str(row["kickoff_utc"]).replace("Z", "+00:00"))
    return dt.date()


def _detect_competition(query: str) -> Optional[str]:
    q = query.lower()
    if "serie a" in q or "serie_a" in q:
        return "Serie_A"
    if "serie b" in q or "serie_b" in q:
        return "Serie_B"
    if "premier" in q or "epl" in q or "premier league" in q:
        return "EPL"
    if "bundesliga" in q:
        return "Bundesliga"
    if "la liga" in q or "laliga" in q:
        return "La_Liga"
    if "ligue 1" in q or "ligue1" in q:
        return "Ligue_1"
    return None


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower().replace("_", " ")).strip()


def _list_team_names() -> List[str]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT home AS team FROM matches
            UNION
            SELECT DISTINCT away AS team FROM matches
            """
        ).fetchall()
    return [r["team"] for r in rows if r and r["team"]]


def _detect_team_from_matches(query: str) -> Optional[str]:
    qn = _normalize_text(query)
    for team in _list_team_names():
        if _normalize_text(team) in qn:
            return team
    return None


def _best_team_match(text: str, teams: List[str]) -> Optional[str]:
    best = None
    best_len = 0
    for team in teams:
        tn = _normalize_text(team)
        if not tn:
            continue
        if re.search(rf"\\b{re.escape(tn)}\\b", text):
            if len(tn) > best_len:
                best = team
                best_len = len(tn)
    return best


def _detect_team_pair(query: str) -> Optional[tuple[str, str]]:
    qn = _normalize_text(query)
    teams = _list_team_names()
    for sep in (" vs ", " v ", " - ", " @ "):
        if sep in qn:
            left, right = qn.split(sep, 1)
            t_left = _best_team_match(left, teams)
            t_right = _best_team_match(right, teams)
            if t_left and t_right and t_left != t_right:
                return (t_left, t_right)

    candidates = []
    for team in teams:
        tn = _normalize_text(team)
        if not tn:
            continue
        m = re.search(rf"\\b{re.escape(tn)}\\b", qn)
        if m:
            candidates.append((m.start(), team))
    if len(candidates) >= 2:
        candidates_sorted = sorted(candidates, key=lambda x: x[0])
        return (candidates_sorted[0][1], candidates_sorted[1][1])
    return None


def _match_id_for_pair(team_a: str, team_b: str) -> Optional[str]:
    now_iso = _now_utc().isoformat().replace("+00:00", "Z")
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT match_id
            FROM matches
            WHERE ((home = ? AND away = ?) OR (home = ? AND away = ?))
              AND kickoff_utc >= ?
            ORDER BY kickoff_utc ASC
            LIMIT 1
            """,
            (team_a, team_b, team_b, team_a, now_iso),
        ).fetchone()
        if row:
            return row["match_id"]
        row = conn.execute(
            """
            SELECT match_id
            FROM matches
            WHERE (home = ? AND away = ?) OR (home = ? AND away = ?)
            ORDER BY kickoff_utc DESC
            LIMIT 1
            """,
            (team_a, team_b, team_b, team_a),
        ).fetchone()
    return row["match_id"] if row else None


def _match_id_for_team(team: str) -> Optional[str]:
    now_iso = _now_utc().isoformat().replace("+00:00", "Z")
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT match_id
            FROM matches
            WHERE (home = ? OR away = ?)
              AND kickoff_utc >= ?
            ORDER BY kickoff_utc ASC
            LIMIT 1
            """,
            (team, team, now_iso),
        ).fetchone()
        if not row:
            row = conn.execute(
                """
                SELECT match_id
                FROM matches
                WHERE home = ? OR away = ?
                ORDER BY kickoff_utc DESC
                LIMIT 1
                """,
                (team, team),
            ).fetchone()
    return row["match_id"] if row else None


def _detect_date(query: str, competition: Optional[str]) -> Optional[date]:
    q = query.lower()
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", q)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except ValueError:
            pass
    if "oggi" in q or "stasera" in q:
        return _now_utc().date()
    if "domani" in q:
        return (_now_utc() + timedelta(days=1)).date()
    if "prossima giornata" in q or "prossima" in q:
        return _next_match_date(competition)
    m = re.search(
        r"\b(\d{1,2})\s+"
        r"(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|"
        r"settembre|ottobre|novembre|dicembre)"
        r"(?:\s+(\d{4}))?\b",
        q,
    )
    if m:
        day = int(m.group(1))
        month_name = m.group(2)
        year = int(m.group(3)) if m.group(3) else _now_utc().year
        months = {
            "gennaio": 1,
            "febbraio": 2,
            "marzo": 3,
            "aprile": 4,
            "maggio": 5,
            "giugno": 6,
            "luglio": 7,
            "agosto": 8,
            "settembre": 9,
            "ottobre": 10,
            "novembre": 11,
            "dicembre": 12,
        }
        month = months.get(month_name)
        if month:
            try:
                d = date(year, month, day)
            except ValueError:
                return None
            if not m.group(3) and d < _now_utc().date():
                try:
                    d = date(year + 1, month, day)
                except ValueError:
                    pass
            return d
    return None


def _detect_match_id(query: str) -> Optional[str]:
    m = re.search(r"\b[a-zA-Z0-9_-]+:\d+\b", query)
    return m.group(0) if m else None


def _format_recs(recs) -> List[str]:
    lines: List[str] = []
    for r in recs:
        lines.append(f"{r.market} {r.selection} @ {r.odds_decimal:.2f}")
    return lines


def _format_pick(p) -> str:
    comp = _competition_label(getattr(p, "competition", None))
    prefix = f"[{comp}] " if comp else ""
    return f"{prefix}{p.home} vs {p.away}: {p.market} {p.selection} @ {p.odds_decimal:.2f}"


def _difficulty_label(value: str) -> str:
    return {"safe": "Facile", "medium": "Media", "risky": "Difficile"}.get(value, value)


def _competition_label(competition: Optional[str]) -> str:
    if not competition:
        return ""
    label_map = {
        "Serie_A": "Serie A",
        "Serie_B": "Serie B",
        "EPL": "Premier League",
        "Bundesliga": "Bundesliga",
        "La_Liga": "La Liga",
        "Ligue_1": "Ligue 1",
    }
    return label_map.get(competition, competition.replace("_", " "))


def _format_multiple(m) -> str:
    label = _difficulty_label(m.difficulty)
    prefix = f"Schedina {label}"
    if not m.legs:
        return f"{prefix}: nessuna leg"
    legs = "; ".join(_format_pick(p) for p in m.legs)
    if m.total_odds:
        return f"{prefix}: {legs} | total_odds={m.total_odds:.2f}"
    return f"{prefix}: {legs}"

def _format_prob(p: float) -> str:
    return f"{p * 100:.1f}%"


def _grade_rec(rec) -> str:
    conf = rec.confidence if rec.confidence is not None else 0.5
    edge = rec.expected_edge
    if edge >= 0.06 and conf >= 0.6:
        return "A"
    if edge >= 0.03 and conf >= 0.4:
        return "B"
    return "C"


def _format_rec_detail(rec) -> str:
    edge_pct = rec.expected_edge * 100
    stake_pct = rec.stake_fraction * 100
    conf = f", conf={rec.confidence:.2f}" if rec.confidence is not None else ""
    line_value = ""
    if rec.line_value_pct is not None:
        line_value = f", line={rec.line_value_pct * 100:.1f}%"
    consensus = ""
    if rec.consensus_odds is not None:
        consensus = f", consensus={rec.consensus_odds:.2f}"
    grade = _grade_rec(rec)
    return f"{rec.market} {rec.selection} @ {rec.odds_decimal:.2f} | edge={edge_pct:.1f}% | stake={stake_pct:.2f}%{conf}{line_value}{consensus} | grade={grade}"


def _format_model_snapshot(report) -> Optional[str]:
    sim = report.simulation_outputs
    if not sim:
        return None
    probs = sim.probs or {}
    diag = sim.diagnostics or {}
    lam_h = diag.get("lambda_home")
    lam_a = diag.get("lambda_away")
    parts = []
    if lam_h is not None and lam_a is not None:
        parts.append(f"xG={lam_h:.2f}-{lam_a:.2f}")
    if probs:
        p_home = probs.get("home_win")
        p_draw = probs.get("draw")
        p_away = probs.get("away_win")
        if p_home is not None and p_draw is not None and p_away is not None:
            parts.append(f"P(1/X/2)={_format_prob(p_home)}/{_format_prob(p_draw)}/{_format_prob(p_away)}")
    if sim.scoreline_topk:
        top = sim.scoreline_topk[0]
        parts.append(f"top scoreline {top['home_goals']}-{top['away_goals']} ({_format_prob(top['p'])})")
    return "Model: " + " | ".join(parts) if parts else None


def _fair_odds(p: Optional[float]) -> Optional[float]:
    if p is None or p <= 0:
        return None
    return 1.0 / p


def _format_kpi_line(report) -> Optional[str]:
    if not report or not report.model_outputs:
        return None
    kpi = (report.model_outputs.derived or {}).get("kpi")
    if not kpi:
        return None
    status = kpi.get("status", "n/a")
    phase = kpi.get("phase")
    logloss = kpi.get("logloss_1x2")
    brier = kpi.get("brier_1x2")
    roi = kpi.get("roi_1x2")
    parts = [f"status={status}"]
    if phase:
        parts.append(f"phase={phase}")
    if logloss is not None:
        parts.append(f"logloss={logloss:.3f}")
    if brier is not None:
        parts.append(f"brier={brier:.3f}")
    if roi is not None:
        parts.append(f"roi={roi:.3f}")
    brier_by = kpi.get("brier_by_market") or {}
    logloss_by = kpi.get("logloss_by_market") or {}
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
    return "Model KPI: " + " | ".join(parts)


def _format_lineup_impact(report) -> Optional[str]:
    if not report or not report.model_outputs:
        return None
    lineup = (report.model_outputs.derived or {}).get("lineup")
    if not lineup:
        return None
    src = lineup.get("lineup_source")
    cov_h = lineup.get("coverage_home")
    cov_a = lineup.get("coverage_away")
    abs_h = lineup.get("absence_share_home")
    abs_a = lineup.get("absence_share_away")
    pen_h = lineup.get("penalty_home")
    pen_a = lineup.get("penalty_away")
    parts = []
    if src:
        parts.append(f"src={src}")
    if cov_h is not None and cov_a is not None:
        parts.append(f"coverage={cov_h:.2f}/{cov_a:.2f}")
    if abs_h is not None and abs_a is not None:
        parts.append(f"abs_share={abs_h:.2f}/{abs_a:.2f}")
    if pen_h is not None and pen_a is not None:
        parts.append(f"penalty={pen_h:.2f}/{pen_a:.2f}")
    return "Lineup adj: " + " | ".join(parts) if parts else None


def _format_schedule_line(report) -> Optional[str]:
    ctx = report.match_context if report else None
    if not ctx or not ctx.schedule_factors:
        return None
    sched = ctx.schedule_factors
    rest_h = sched.get("rest_days_home")
    rest_a = sched.get("rest_days_away")
    m7_h = sched.get("matches_7d_home")
    m7_a = sched.get("matches_7d_away")
    m14_h = sched.get("matches_14d_home")
    m14_a = sched.get("matches_14d_away")
    parts = []
    if rest_h is not None and rest_a is not None:
        parts.append(f"rest={float(rest_h):.1f}d/{float(rest_a):.1f}d")
    if m7_h is not None and m7_a is not None:
        parts.append(f"last7={int(m7_h)}/{int(m7_a)}")
    if m14_h is not None and m14_a is not None:
        parts.append(f"last14={int(m14_h)}/{int(m14_a)}")
    return "Schedule: " + " | ".join(parts) if parts else None


def _format_form_line(report) -> Optional[str]:
    if not report or not report.model_outputs:
        return None
    form = (report.model_outputs.derived or {}).get("form")
    if not form:
        return None
    xg_for_h = form.get("xg_for_delta_home")
    xg_for_a = form.get("xg_for_delta_away")
    xg_against_h = form.get("xg_against_delta_home")
    xg_against_a = form.get("xg_against_delta_away")
    fin_h = form.get("finishing_delta_form_home")
    fin_a = form.get("finishing_delta_form_away")
    parts = []
    if xg_for_h is not None and xg_for_a is not None:
        parts.append(f"xG_for delta={xg_for_h*100:.0f}%/{xg_for_a*100:.0f}%")
    if xg_against_h is not None and xg_against_a is not None:
        parts.append(f"xG_against delta={xg_against_h*100:.0f}%/{xg_against_a*100:.0f}%")
    if fin_h is not None and fin_a is not None:
        parts.append(f"finishing delta={fin_h:+.2f}/{fin_a:+.2f}")
    return "Form: " + " | ".join(parts) if parts else None


def _format_tactical_line(report) -> Optional[str]:
    if not report or not report.model_outputs:
        return None
    tactical = (report.model_outputs.derived or {}).get("tactical")
    if not tactical:
        return None
    tags = tactical.get("tags") or []
    if not tags:
        return None
    source = tactical.get("source", "n/a")
    matchup = tactical.get("matchup")
    parts = [f"src={source}", "tags=" + ", ".join(tags)]
    tempo = tactical.get("tempo")
    if tempo and tempo != "neutral":
        parts.append(f"tempo={tempo}")
    style = tactical.get("style_matchup") or {}
    indicator = style.get("indicator")
    if indicator and indicator != "neutral":
        label = "favor casa" if indicator == "home_favorable" else "favor ospite"
        parts.append(f"style={label}")
    if matchup:
        parts.append(f"matchup={matchup}")
    return "Tactical: " + " | ".join(parts)


def _format_confidence_line(report) -> Optional[str]:
    if not report or not report.model_outputs:
        return None
    conf = (report.model_outputs.derived or {}).get("model_confidence")
    if not conf:
        return None
    score = conf.get("score")
    cov = conf.get("lineup_coverage")
    stability = conf.get("form_stability")
    data_q = conf.get("data_quality_score")
    parts = []
    if score is not None:
        parts.append(f"score={float(score):.2f}")
    if cov is not None:
        parts.append(f"lineup={float(cov):.2f}")
    if stability is not None:
        parts.append(f"stability={float(stability):.2f}")
    if data_q is not None:
        parts.append(f"data={float(data_q):.2f}")
    return "Model confidence: " + " | ".join(parts) if parts else None


def _driver_line(d: dict) -> Optional[str]:
    label = d.get("label") or "driver"
    dh = d.get("home_delta_pct")
    da = d.get("away_delta_pct")
    if dh is None and da is None:
        return None
    bits = []
    if dh is not None:
        bits.append(f"casa {dh*100:+.0f}%")
    if da is not None:
        bits.append(f"ospite {da*100:+.0f}%")
    note = d.get("note")
    suffix = f" ({note})" if note else ""
    return f"{label}: " + " / ".join(bits) + suffix


def _delta_pp(value: float | None) -> Optional[str]:
    if value is None:
        return None
    if abs(value) < 0.005:
        return None
    return f"{value*100:+.0f}pp"


def _format_range(label: str, min_v: float | None, max_v: float | None) -> Optional[str]:
    if min_v is None or max_v is None:
        return None
    diff = max_v - min_v
    if diff < 0.005:
        return f"{label}: ~{min_v*100:.0f}%"
    return f"{label}: {min_v*100:.0f}-{max_v*100:.0f}%"


def _format_explainability_block(report) -> Optional[str]:
    if not report or not report.simulation_outputs:
        return None
    probs = report.simulation_outputs.probs or {}
    if not probs:
        return None

    lines = ["Analisi sintetica"]
    p1 = probs.get("home_win")
    px = probs.get("draw")
    p2 = probs.get("away_win")
    pu = probs.get("under_2_5")
    po = probs.get("over_2_5")
    pb_yes = probs.get("btts_yes")
    pb_no = probs.get("btts_no")
    parts = []
    if p1 is not None and px is not None and p2 is not None:
        parts.append(f"1/X/2 {_format_prob(p1)}/{_format_prob(px)}/{_format_prob(p2)}")
    if pu is not None and po is not None:
        parts.append(f"OU2.5 U{_format_prob(pu)} O{_format_prob(po)}")
    if pb_yes is not None and pb_no is not None:
        parts.append(f"BTTS Y{_format_prob(pb_yes)} N{_format_prob(pb_no)}")
    if parts:
        lines.append("Probabilita: " + " | ".join(parts))

    scenario = (report.model_outputs.derived or {}).get("scenario_analysis") if report.model_outputs else None
    if scenario:
        ranges = scenario.get("ranges") or {}
        r1 = ranges.get("home_win")
        line = _format_range("Range scenari (1)", r1.get("min") if r1 else None, r1.get("max") if r1 else None)
        if line:
            lines.append(line)
        r_under = ranges.get("under_2_5")
        line = _format_range(
            "Range scenari (Under2.5)",
            r_under.get("min") if r_under else None,
            r_under.get("max") if r_under else None,
        )
        if line:
            lines.append(line)

    drivers = (report.model_outputs.derived or {}).get("drivers") if report.model_outputs else []
    if drivers:
        lines.append("Motivi:")
        for d in drivers[:3]:
            line = _driver_line(d)
            if line:
                lines.append(f"- {line}")

    tactical = (report.model_outputs.derived or {}).get("tactical") if report.model_outputs else None
    if tactical:
        style = tactical.get("style_matchup") or {}
        indicator = style.get("indicator")
        if indicator and indicator != "neutral":
            label = "favorevole casa" if indicator == "home_favorable" else "favorevole ospite"
            lines.append(f"- stile vs stile: {label}")

    risks = []
    conf = (report.model_outputs.derived or {}).get("model_confidence") if report.model_outputs else None
    if conf:
        score = conf.get("score")
        if score is not None and float(score) < 0.55:
            risks.append(f"confidence bassa (score={float(score):.2f})")
        data_q = conf.get("data_quality_score")
        if data_q is not None and float(data_q) < 0.7:
            risks.append(f"copertura dati limitata (data={float(data_q):.2f})")
        if conf.get("lineup_coverage") is not None and float(conf.get("lineup_coverage")) < 0.35:
            risks.append("lineup coverage bassa")
        if conf.get("finishing_penalty") is not None and float(conf.get("finishing_penalty")) >= 0.05:
            risks.append("finishing volatile (scostamento xG)")

    if scenario:
        sens = scenario.get("sensitivity") or {}
        sens_1x2 = sens.get("home_win")
        if sens_1x2 is not None and float(sens_1x2) >= 0.06:
            risks.append(f"sensibilita scenari alta (±{float(sens_1x2)*100:.0f}pp su 1)")

    if risks:
        lines.append("Rischi:")
        for r in risks[:3]:
            lines.append(f"- {r}")

    if scenario and scenario.get("scenarios"):
        base = next((s for s in scenario["scenarios"] if s.get("id") == "base"), None)
        if base:
            base_probs = base.get("probs") or {}
            flips = []
            for s in scenario["scenarios"]:
                if s.get("id") == "base":
                    continue
                sp = s.get("probs") or {}
                d1 = _delta_pp(sp.get("home_win") - base_probs.get("home_win")) if base_probs.get("home_win") is not None else None
                d2 = _delta_pp(sp.get("under_2_5") - base_probs.get("under_2_5")) if base_probs.get("under_2_5") is not None else None
                if d1 or d2:
                    bits = []
                    if d1:
                        bits.append(f"1 {d1}")
                    if d2:
                        bits.append(f"Under2.5 {d2}")
                    flips.append(f"{s.get('label')}: " + ", ".join(bits))
            if flips:
                lines.append("Cosa puo ribaltare la partita:")
                for item in flips[:2]:
                    lines.append(f"- {item}")

    return "\n".join(lines) if len(lines) > 1 else None


def _format_pro_block(report) -> Optional[str]:
    sim = report.simulation_outputs if report else None
    if not sim:
        return None
    probs = sim.probs or {}
    p1 = probs.get("home_win")
    px = probs.get("draw")
    p2 = probs.get("away_win")
    pu = probs.get("under_2_5")
    po = probs.get("over_2_5")
    pb_yes = probs.get("btts_yes")
    pb_no = probs.get("btts_no")

    lines = ["Output Pro"]
    if p1 is not None and px is not None and p2 is not None:
        lines.append(f"1/X/2: {_format_prob(p1)}/{_format_prob(px)}/{_format_prob(p2)}")
        fo1 = _fair_odds(p1)
        fox = _fair_odds(px)
        fo2 = _fair_odds(p2)
        if fo1 and fox and fo2:
            lines.append(f"Fair odds 1X2: 1 {fo1:.2f} | X {fox:.2f} | 2 {fo2:.2f}")
    if pu is not None and po is not None:
        lines.append(f"OU 2.5: Under {_format_prob(pu)} | Over {_format_prob(po)}")
    if pb_yes is not None and pb_no is not None:
        lines.append(f"BTTS: Yes {_format_prob(pb_yes)} | No {_format_prob(pb_no)}")

    kpi_line = _format_kpi_line(report)
    if kpi_line:
        lines.append(kpi_line)

    lineup_line = _format_lineup_impact(report)
    if lineup_line:
        lines.append(lineup_line)

    schedule_line = _format_schedule_line(report)
    if schedule_line:
        lines.append(schedule_line)

    form_line = _format_form_line(report)
    if form_line:
        lines.append(form_line)

    tactical_line = _format_tactical_line(report)
    if tactical_line:
        lines.append(tactical_line)

    conf_line = _format_confidence_line(report)
    if conf_line:
        lines.append(conf_line)

    if report.recommendations:
        rec_lines = "; ".join(_format_rec_detail(r) for r in report.recommendations)
        lines.append(f"Picks: {rec_lines}")

    return "\n".join(lines)


def _player_sort_key(p) -> tuple:
    return (
        p.expected_gi if p.expected_gi is not None else -1.0,
        p.gi_per90 if p.gi_per90 is not None else -1.0,
        p.xg_per90 if p.xg_per90 is not None else -1.0,
    )


def _format_player(p) -> str:
    bits = []
    if p.expected_gi is not None:
        bits.append(f"expGI={p.expected_gi:.2f}")
    if p.xg_per90 is not None:
        bits.append(f"xG90={p.xg_per90:.2f}")
    if p.xa_per90 is not None:
        bits.append(f"xA90={p.xa_per90:.2f}")
    extra = f" ({p.position})" if p.position else ""
    name = clean_person_name(p.player_name) or p.player_name
    if bits:
        return f"{name}{extra} " + " ".join(bits)
    return f"{name}{extra}"

def _format_lineup_list(players: List[str], limit: int = 11) -> str:
    if not players:
        return "n/a"
    cleaned = []
    for p in players[:limit]:
        name = clean_person_name(p) or p
        cleaned.append(name)
    return ", ".join(cleaned)


def _format_absences(news_items, limit: int = 3) -> Optional[str]:
    if not news_items:
        return None
    event_news = [
        n for n in news_items
        if (n.event_type and n.event_type.lower() in ("injury", "suspension"))
    ]
    if not event_news:
        return None
    titles = "; ".join(clean_person_name(n.title) or n.title for n in event_news[:limit])
    return titles


def _format_lineup_absences(lineup: dict, home_name: str, away_name: str, limit: int = 4) -> Optional[str]:
    home_abs = lineup.get("home_absences") or []
    away_abs = lineup.get("away_absences") or []
    if not home_abs and not away_abs:
        return None
    home_line = ", ".join((clean_person_name(p) or p) for p in home_abs[:limit]) if home_abs else "n/a"
    away_line = ", ".join((clean_person_name(p) or p) for p in away_abs[:limit]) if away_abs else "n/a"
    return f"{home_name}: {home_line} | {away_name}: {away_line}"


def _is_follow_up(query: str) -> bool:
    q = query.strip().lower()
    if not q:
        return False
    return bool(re.search(r"^(e|ed)\b", q) or re.search(r"\b(ancora|stessa|stesso|come prima|di nuovo|continua)\b", q))


def _parse_day_iso(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def answer_query(query: str, n_sims: int, seed: int, bankroll: float, session_id: str | None = None) -> ChatResponse:
    warnings: List[str] = []
    competition = _detect_competition(query)
    match_id = _detect_match_id(query)

    q = query.lower()
    day_hint = _detect_date(query, competition)
    wants_day_list = "partite" in q or "matches" in q
    wants_multiples = "multipla" in q or "multiple" in q
    wants_quotes = "quote" in q or "giocare" in q or "giornata" in q
    follow_up = _is_follow_up(query)
    wants_analysis = "analisi" in q or "analizza" in q or "analysis" in q
    wants_predictions = (
        "prevision" in q
        or "pronostic" in q
        or "schedin" in q
        or (wants_day_list and day_hint is not None)
        or (wants_analysis and day_hint is not None)
    )
    wants_events = (
        "ammon" in q
        or "ammun" in q
        or "cartellin" in q
        or "squalif" in q
        or "infortun" in q
        or "injur" in q
    )
    wants_players = (
        "giocatori" in q
        or "giocatore" in q
        or "player" in q
        or "calciatori" in q
    )
    wants_match = "partita" in q or "match" in q

    session_ctx = None
    if session_id:
        try:
            session_ctx = ensure_session(session_id)
        except Exception:
            session_ctx = None

    if session_ctx:
        last_intent = session_ctx.get("last_intent")
        if not wants_predictions and day_hint and (follow_up or last_intent in ("predictions", "slate")):
            wants_predictions = True
        if not wants_predictions and follow_up and last_intent in ("predictions", "slate") and day_hint is None:
            wants_predictions = True
    else:
        last_intent = None

    if not competition and session_ctx and (wants_predictions or wants_quotes or wants_multiples or day_hint):
        competition = session_ctx.get("last_competition") or competition

    if not match_id and (wants_analysis or wants_events or wants_match):
        pair = _detect_team_pair(query)
        if pair:
            match_id = _match_id_for_pair(pair[0], pair[1])

    if not match_id and (wants_analysis or wants_events or wants_match):
        team = _detect_team_from_matches(query)
        if team:
            match_id = _match_id_for_team(team)
            if match_id:
                warnings.append("TEAM_MATCH_FALLBACK_USED")

    if not match_id and wants_analysis and session_ctx and _is_follow_up(query):
        match_id = session_ctx.get("last_match_id")
        if match_id:
            warnings.append("SESSION_MATCH_FALLBACK_USED")

    if match_id:
        req = SimpleNamespace(match_id=match_id, n_sims=n_sims, seed=seed, bankroll=bankroll)
        report = analyze_match_by_id(req)
        match_label = f"{report.match.home.name} vs {report.match.away.name}"
        answer = f"Ecco l'analisi per {match_label} ({report.match.match_id})."
        absences_added = False

        model_line = _format_model_snapshot(report)
        if model_line:
            answer += f" {model_line}."

        if report.recommendations:
            rec_lines = "; ".join(_format_rec_detail(r) for r in report.recommendations)
            answer += f" Suggerimenti: {rec_lines}."
        else:
            if report.no_bet:
                answer += f" No bet: {', '.join(report.no_bet.reason_codes)}."
            else:
                answer += " Nessuna raccomandazione utile (filtri o dati mancanti)."

        if wants_events and report.web_intel and report.web_intel.news:
            event_news = [
                n for n in report.web_intel.news
                if (n.event_type and n.event_type.lower() in ("cards", "suspension", "injury"))
            ]
            if event_news:
                titles = "; ".join(n.title for n in event_news[:5])
                answer += f" Eventi giocatori: {titles}."
            else:
                answer += " Nessuna news eventi giocatori collegata al match."
        elif wants_events:
            answer += " Nessuna news eventi giocatori trovata in locale."

        if (wants_players or wants_match) and report.player_projections:
            home_players = sorted(report.player_projections.home, key=_player_sort_key, reverse=True)[:3]
            away_players = sorted(report.player_projections.away, key=_player_sort_key, reverse=True)[:3]
            if home_players or away_players:
                home_line = ", ".join(_format_player(p) for p in home_players) if home_players else "n/a"
                away_line = ", ".join(_format_player(p) for p in away_players) if away_players else "n/a"
                answer += f" Giocatori chiave: {report.match.home.name}: {home_line} | {report.match.away.name}: {away_line}."

        if report.web_intel and report.web_intel.predicted_lineups:
            lineup = report.web_intel.predicted_lineups[0]
            home_line = _format_lineup_list(lineup.get("home_players") or [])
            away_line = _format_lineup_list(lineup.get("away_players") or [])
            answer += f" Probabili formazioni: {report.match.home.name}: {home_line} | {report.match.away.name}: {away_line}."
            lineup_absences = _format_lineup_absences(
                lineup,
                report.match.home.name,
                report.match.away.name,
            )
            if lineup_absences:
                answer += f" Top assenze: {lineup_absences}."
                absences_added = True
        if report.web_intel and report.web_intel.news:
            absences = _format_absences(report.web_intel.news)
            if absences and not absences_added:
                answer += f" Top assenze: {absences}."

        pro_block = _format_pro_block(report)
        if pro_block:
            answer += "\n" + pro_block
        explain_block = _format_explainability_block(report)
        if explain_block:
            answer += "\n" + explain_block
        answer += "\nSe vuoi, posso analizzare un'altra partita o la giornata di oggi."

        if session_id:
            try:
                add_message(session_id, "user", query, {"intent": "match_analysis"})
                recent = get_recent_messages(session_id, limit=6)
                answer, _ = rewrite_answer(query, answer, "match_analysis", recent)
                add_message(session_id, "assistant", answer, {"intent": "match_analysis"})
                update_session(
                    session_id,
                    last_match_id=match_id,
                    last_competition=report.match.competition,
                    last_day_utc=report.match.kickoff_utc.date().isoformat(),
                    last_intent="match_analysis",
                )
            except Exception:
                pass
        else:
            answer, _ = rewrite_answer(query, answer, "match_analysis", None)

        return ChatResponse(
            answer=answer,
            resolved_intent="match_analysis",
            warnings=warnings,
            report=report,
            slate=None,
            session_id=session_id,
        )

    if wants_predictions:
        day = _detect_date(query, competition)
        if not day and session_ctx and (wants_predictions or _is_follow_up(query)):
            day = _parse_day_iso(session_ctx.get("last_day_utc"))
        if not day:
            day = _next_match_date(competition) or _now_utc().date()
            warnings.append("DATE_FALLBACK_USED")
        answer = build_day_prediction_text(
            day_utc=day,
            competition=competition,
            n_sims=n_sims,
            seed=seed,
        )
        if wants_analysis:
            answer = "Certo, ecco l'analisi della giornata.\n\n" + answer
        slate = None
        if wants_quotes or wants_multiples:
            slate = build_slate_report(
                day_utc=day,
                competition=competition,
                n_sims=n_sims,
                seed=seed,
                bankroll=bankroll,
            )
            if slate.multiples:
                answer += "\n\nSchedine con quote (pre-kickoff)"
                for m in slate.multiples:
                    answer += "\n" + _format_multiple(m)
            else:
                answer += "\n\nSchedine con quote: nessuna selezione disponibile."

        if session_id:
            try:
                add_message(session_id, "user", query, {"intent": "predictions"})
                recent = get_recent_messages(session_id, limit=6)
                answer, _ = rewrite_answer(query, answer, "predictions", recent)
                add_message(session_id, "assistant", answer, {"intent": "predictions"})
                update_session(
                    session_id,
                    last_day_utc=day.isoformat(),
                    last_competition=competition,
                    last_intent="predictions",
                )
            except Exception:
                pass
        else:
            answer, _ = rewrite_answer(query, answer, "predictions", None)

        return ChatResponse(
            answer=answer,
            resolved_intent="predictions",
            warnings=warnings,
            report=None,
            slate=slate,
            session_id=session_id,
        )

    if wants_quotes or wants_multiples:
        day = _detect_date(query, competition)
        if not day and session_ctx and _is_follow_up(query):
            day = _parse_day_iso(session_ctx.get("last_day_utc"))
        if not day:
            day = _next_match_date(competition) or _now_utc().date()
            warnings.append("DATE_FALLBACK_USED")
        slate = build_slate_report(
            day_utc=day,
            competition=competition,
            n_sims=n_sims,
            seed=seed,
            bankroll=bankroll,
        )
        answer = f"Slate pronto per {day.isoformat()}."
        if competition:
            answer += f" Competizione: {competition}."
        if slate.picks:
            answer += f" Picks: {len(slate.picks)}."
        if slate.picks and wants_quotes:
            top = sorted(slate.picks, key=lambda p: p.expected_edge, reverse=True)[:5]
            answer += " Top picks: " + "; ".join(_format_pick(p) for p in top) + "."
        if wants_multiples:
            answer += " Multiple: " + " | ".join(_format_multiple(m) for m in slate.multiples) + "."

        if session_id:
            try:
                add_message(session_id, "user", query, {"intent": "slate"})
                recent = get_recent_messages(session_id, limit=6)
                answer, _ = rewrite_answer(query, answer, "slate", recent)
                add_message(session_id, "assistant", answer, {"intent": "slate"})
                update_session(
                    session_id,
                    last_day_utc=day.isoformat(),
                    last_competition=competition,
                    last_intent="slate",
                )
            except Exception:
                pass
        else:
            answer, _ = rewrite_answer(query, answer, "slate", None)

        return ChatResponse(
            answer=answer,
            resolved_intent="slate",
            warnings=warnings,
            report=None,
            slate=slate,
            session_id=session_id,
        )

    if wants_analysis:
        answer = (
            "Quale partita vuoi analizzare? "
            "Scrivi per esempio: 'analisi Lazio vs Como' oppure 'analisi Roma Torino'."
        )
        try:
            add_chat_feedback(
                query=query,
                response=answer,
                label="unresolved_match",
                notes="analysis_request_without_match",
                match_id=None,
                meta={"competition": competition},
            )
        except Exception:
            pass
        warnings.append("MATCH_NOT_RESOLVED")
        return ChatResponse(
            answer=answer,
            resolved_intent="clarify_match",
            warnings=warnings,
            report=None,
            slate=None,
            session_id=session_id,
        )

    answer = (
        "Non ho capito la richiesta. "
        "Puoi chiedermi analisi partita (es: 'analisi Lazio vs Como') "
        "oppure pronostici per una data (es: 'partite di oggi')."
    )
    if session_id:
        try:
            add_message(session_id, "user", query, {"intent": "unknown"})
            recent = get_recent_messages(session_id, limit=6)
            answer, _ = rewrite_answer(query, answer, "unknown", recent)
            add_message(session_id, "assistant", answer, {"intent": "unknown"})
            update_session(session_id, last_intent="unknown")
        except Exception:
            pass
    else:
        answer, _ = rewrite_answer(query, answer, "unknown", None)
    warnings.append("INTENT_NOT_RECOGNIZED")
    return ChatResponse(
        answer=answer,
        resolved_intent="unknown",
        warnings=warnings,
        report=None,
        slate=None,
        session_id=session_id,
    )

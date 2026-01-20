from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple
from types import SimpleNamespace
import math

from app.db.sqlite import get_conn
from app.models.schemas import SlateReport, SlatePick, SlateMultiple
from app.services.report_service import analyze_match_by_id
from app.services.market_eval_service import evaluate_markets
from app.services.market_rules_service import get_market_rules
from app.services.schedine_rules_service import get_schedine_rules


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


def _pick_from_match(picks: List[SlatePick], mode: str) -> Optional[SlatePick]:
    if not picks:
        return None
    if mode == "safe":
        return min(picks, key=lambda p: p.odds_decimal)
    if mode == "risky":
        return max(picks, key=lambda p: p.odds_decimal)
    # medium: odds vicino a 2.2
    return min(picks, key=lambda p: abs(p.odds_decimal - 2.2))

def _family(market: str) -> str:
    m = market.upper()
    if m == "1X2":
        return "RESULT"
    if m.startswith("OU_"):
        return "TOTALS"
    if m == "BTTS":
        return "BTTS"
    return m


def _selection_bucket(market: str, selection: str) -> str:
    m = market.upper()
    s = selection.upper()
    if m == "1X2":
        return s
    if m.startswith("OU_"):
        return "OVER" if s == "OVER" else "UNDER"
    if m == "BTTS":
        return "YES" if s == "YES" else "NO"
    return s


def _model_prob_from_pick(p: SlatePick) -> float:
    if p.odds_decimal <= 0:
        return 0.0
    return max(0.0, (p.expected_edge + 1.0) / p.odds_decimal)


def _filter_pick(p: SlatePick, rules: dict) -> bool:
    min_prob = float(rules.get("min_prob", 0.0))
    min_edge = float(rules.get("min_edge", 0.0))
    max_odds = rules.get("max_odds")
    min_odds = rules.get("min_odds")
    prob = _model_prob_from_pick(p)
    if prob < min_prob:
        return False
    if p.expected_edge < min_edge:
        return False
    if max_odds is not None and p.odds_decimal > float(max_odds):
        return False
    if min_odds is not None and p.odds_decimal < float(min_odds):
        return False
    return True


def _build_multiple(picks: List[SlatePick], difficulty: str, legs: int) -> SlateMultiple:
    if difficulty == "safe":
        ordered = sorted(picks, key=lambda p: (p.odds_decimal, -p.expected_edge))
    elif difficulty == "risky":
        ordered = sorted(picks, key=lambda p: (-p.odds_decimal, -p.expected_edge))
    else:
        ordered = sorted(picks, key=lambda p: (abs(p.odds_decimal - 2.2), -p.expected_edge))

    max_per_family = 1 if difficulty in ("safe", "medium") else 2
    chosen: List[SlatePick] = []
    fam_counts: Dict[str, int] = {}
    sel_counts: Dict[Tuple[str, str], int] = {}

    for p in ordered:
        fam = _family(p.market)
        sel = _selection_bucket(p.market, p.selection)
        if fam_counts.get(fam, 0) >= max_per_family:
            continue
        if sel_counts.get((fam, sel), 0) >= 2:
            continue
        chosen.append(p)
        fam_counts[fam] = fam_counts.get(fam, 0) + 1
        sel_counts[(fam, sel)] = sel_counts.get((fam, sel), 0) + 1
        if len(chosen) >= legs:
            break

    if len(chosen) < legs:
        for p in ordered:
            if p in chosen:
                continue
            chosen.append(p)
            if len(chosen) >= legs:
                break

    total_odds = math.prod(p.odds_decimal for p in chosen) if chosen else None
    return SlateMultiple(difficulty=difficulty, legs=chosen, total_odds=total_odds)


def build_slate_report(
    day_utc: date,
    competition: Optional[str],
    n_sims: int,
    seed: int,
    bankroll: float,
    max_picks_per_match: int = 1,
    legs: int = 3,
) -> SlateReport:
    rows = _list_matches_for_day(day_utc, competition)
    sched_rules = get_schedine_rules()
    allowed_markets = {m.upper() for m in sched_rules.get("markets", [])}
    legs = int(sched_rules.get("card_size", legs))
    notes: List[str] = []
    picks: List[SlatePick] = []
    reason_counts: Dict[str, int] = {}
    relaxed_used = 0

    for r in rows:
        req = SimpleNamespace(
            match_id=r["match_id"],
            n_sims=n_sims,
            seed=seed,
            bankroll=bankroll,
        )
        report = analyze_match_by_id(req)

        if report.recommendations:
            for rec in report.recommendations[:max_picks_per_match]:
                picks.append(SlatePick(
                    match_id=report.match.match_id,
                    competition=report.match.competition,
                    kickoff_utc=report.match.kickoff_utc,
                    home=report.match.home.name,
                    away=report.match.away.name,
                    market=rec.market,
                    selection=rec.selection,
                    bookmaker=rec.bookmaker,
                    odds_decimal=rec.odds_decimal,
                    stake_fraction=rec.stake_fraction,
                    expected_edge=rec.expected_edge,
                    expected_ev_per_unit=rec.expected_ev_per_unit,
                ))
        else:
            relax_block = {
                "POST_KICKOFF_ODDS",
                "STALE_ODDS",
                "MISSING_OR_STALE_ODDS",
                "KPI_MODEL_UNRELIABLE",
                "LOW_MODEL_CONFIDENCE",
            }
            no_bet_codes = set(report.no_bet.reason_codes) if report.no_bet else set()
            used_relaxed = False

            if (
                report.simulation_outputs
                and report.web_intel
                and report.web_intel.odds
                and not (no_bet_codes & relax_block)
            ):
                relaxed_rules = dict(get_market_rules())
                relaxed_rules.update({
                    "min_edge": 0.01,
                    "max_picks": 1,
                    "max_ci_width": 0.10,
                    "kelly_fraction": 0.2,
                    "stake_cap_fraction": 0.02,
                    "max_model_market_gap": 0.6,
                    "longshot_odds": 8.0,
                    "min_edge_longshot": 0.04,
                    "max_odds_age_hours": 12,
                    "max_odds": 6.0,
                    "min_books": 2,
                })
                _, relaxed_recs, _ = evaluate_markets(
                    match_id=report.match.match_id,
                    simulation_outputs=report.simulation_outputs,
                    odds=report.web_intel.odds,
                    bankroll=bankroll,
                    rules=relaxed_rules,
                )
                if relaxed_recs:
                    used_relaxed = True
                    relaxed_used += 1
                    rec = relaxed_recs[0]
                    picks.append(SlatePick(
                        match_id=report.match.match_id,
                        competition=report.match.competition,
                        kickoff_utc=report.match.kickoff_utc,
                        home=report.match.home.name,
                        away=report.match.away.name,
                        market=rec.market,
                        selection=rec.selection,
                        bookmaker=rec.bookmaker,
                        odds_decimal=rec.odds_decimal,
                        stake_fraction=rec.stake_fraction,
                        expected_edge=rec.expected_edge,
                        expected_ev_per_unit=rec.expected_ev_per_unit,
                    ))

            if not used_relaxed:
                if report.no_bet:
                    for code in report.no_bet.reason_codes:
                        reason_counts[code] = reason_counts.get(code, 0) + 1
                else:
                    reason_counts["NO_RECOMMENDATIONS"] = reason_counts.get("NO_RECOMMENDATIONS", 0) + 1

    if not rows:
        notes.append("Nessun match trovato per la data richiesta.")
    else:
        notes.append(f"Match analizzati: {len(rows)}")
        notes.append(f"Match con raccomandazioni: {len({p.match_id for p in picks})}")
        if reason_counts:
            top_reasons = ", ".join(f"{k}={v}" for k, v in sorted(reason_counts.items()))
            notes.append(f"No-bet reasons: {top_reasons}")
        if relaxed_used:
            notes.append(f"Relaxed rules used for {relaxed_used} matches (min_edge=0.01).")

    picks_by_match: Dict[str, List[SlatePick]] = {}
    for p in picks:
        if allowed_markets and p.market.upper() not in allowed_markets:
            continue
        picks_by_match.setdefault(p.match_id, []).append(p)

    easy_rules = dict(sched_rules.get("easy") or {})
    medium_rules = dict(sched_rules.get("medium") or {})
    hard_rules = dict(sched_rules.get("hard") or {})
    for r in (easy_rules, medium_rules, hard_rules):
        r.setdefault("min_edge", sched_rules.get("min_edge", 0.0))

    safe_pool = []
    medium_pool = []
    risky_pool = []
    for match_picks in picks_by_match.values():
        easy_filtered = [p for p in match_picks if _filter_pick(p, easy_rules)]
        med_filtered = [p for p in match_picks if _filter_pick(p, medium_rules)]
        hard_filtered = [p for p in match_picks if _filter_pick(p, hard_rules)]

        p_easy = _pick_from_match(easy_filtered, "safe") if easy_filtered else None
        p_med = _pick_from_match(med_filtered, "medium") if med_filtered else None
        p_hard = _pick_from_match(hard_filtered, "risky") if hard_filtered else None

        if p_easy:
            safe_pool.append(p_easy)
        if p_med:
            medium_pool.append(p_med)
        if p_hard:
            risky_pool.append(p_hard)

    multiples = [
        _build_multiple(safe_pool, "safe", legs),
        _build_multiple(medium_pool, "medium", legs),
        _build_multiple(risky_pool, "risky", legs),
    ]

    if any(len(m.legs) < legs for m in multiples):
        notes.append("Pochi match/picks: alcune multiple hanno meno legs del richiesto.")
    else:
        fam_counts = [len({_family(p.market) for p in m.legs}) for m in multiples if m.legs]
        if fam_counts and min(fam_counts) <= 1:
            notes.append("Diversificazione limitata: molte selezioni dallo stesso market family.")

    total_stake = sum(p.stake_fraction for p in picks)
    if total_stake > 0:
        notes.append(f"Stake totale suggerito ~{total_stake * 100:.2f}% bankroll (singole).")

    if sched_rules:
        notes.append(
            "Schedine rules: min_edge={:.3f} easy_min_p={:.2f} medium_min_p={:.2f} hard_min_p={:.2f}".format(
                float(sched_rules.get("min_edge", 0.0)),
                float(easy_rules.get("min_prob", 0.0)),
                float(medium_rules.get("min_prob", 0.0)),
                float(hard_rules.get("min_prob", 0.0)),
            )
        )

    return SlateReport(
        date_utc=day_utc,
        competition=competition,
        picks=picks,
        multiples=multiples,
        notes=notes,
    )

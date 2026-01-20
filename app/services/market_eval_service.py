from __future__ import annotations
from typing import List, Tuple, Optional, Dict, Any
from statistics import median
from uuid import uuid4
from collections import defaultdict
from datetime import datetime, timezone
from app.models.schemas import MarketEvalItem, Recommendation, NoBet, SimulationOutputs, WebOddsQuote

def _interval_key(market: str, selection: str) -> Optional[str]:
    m = market.upper()
    s = selection.upper()

    if m == "1X2":
        return {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(s)
    if m == "OU_2.5":
        return {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(s)
    if m == "BTTS":
        return {"YES": "btts_yes", "NO": "btts_no"}.get(s)
    return None

def _fair_prob(market: str, selection: str, probs: Dict[str, float]) -> Optional[float]:
    key = _interval_key(market, selection)
    return probs.get(key) if key else None

def _kelly(p: float, odds: float) -> float:
    b = odds - 1.0
    if b <= 0:
        return 0.0
    # kelly fraction
    return max(0.0, (p * odds - 1.0) / b)

def _parse_utc(ts: object) -> Optional[datetime]:
    if not ts:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, str):
        try:
            # gestisce "...Z" -> "+00:00"
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _stake_cap_by_odds(odds: float) -> float:
    # cap dinamico: piu' bassa la quota, piu' basso il cap
    if odds < 1.60:
        return 0.01  # 1%
    if odds < 2.50:
        return 0.02  # 2%
    return 0.03      # 3%

def _confidence_score(
    ci_width: Optional[float],
    edge: Optional[float],
    odds: float,
    gap: Optional[float],
    line_value_pct: Optional[float],
    books_count: Optional[int],
    rules: Dict[str, Any],
) -> float:
    max_ci_width = float(rules.get("max_ci_width", 0.06))
    longshot_odds = float(rules.get("longshot_odds", 5.0))
    max_model_market_gap = float(rules.get("max_model_market_gap", 0.12))

    base = 0.5
    if ci_width is not None and max_ci_width > 0:
        base = max(0.1, min(0.9, 1.0 - (ci_width / (max_ci_width * 1.5))))

    if gap is not None and gap > max_model_market_gap:
        base = min(base, 0.2)
    if odds >= longshot_odds:
        base = min(base, 0.25)
    if edge is not None and edge < float(rules.get("min_edge", 0.03)):
        base = min(base, 0.3)

    if line_value_pct is not None:
        if line_value_pct >= 0.02:
            base = min(0.95, base + 0.1)
        elif line_value_pct <= -0.02:
            base = max(0.1, base - 0.1)
    if books_count is not None:
        if books_count >= 4:
            base = min(0.95, base + 0.05)
        elif books_count <= 1:
            base = max(0.1, base - 0.05)

    return round(base, 2)

def evaluate_markets(
    match_id: str,
    simulation_outputs: SimulationOutputs,
    odds: List[WebOddsQuote],
    bankroll: float,
    rules: Dict[str, Any],
    ) -> Tuple[List[MarketEvalItem], List[Recommendation], Optional[NoBet]]:

    # --- freshness gate: scarta quote troppo vecchie (stale) ---
    max_odds_age_hours = float(rules.get("max_odds_age_hours", 999999))  # default: non blocca
    now_utc = datetime.now(timezone.utc)

    fresh_odds: List[WebOddsQuote] = []
    stale_count = 0

    for q in odds:
        ts = getattr(q, "retrieved_at_utc", None)
        if not ts:
            # se non abbiamo timestamp, teniamola (dev-friendly)
            fresh_odds.append(q)
            continue

        dt = _parse_utc(ts)
        if not dt:
            # se non riesco a parsare, la considero "incerta" ma la tengo (dev-friendly)
            fresh_odds.append(q)
            continue

        age_h = (now_utc - dt).total_seconds() / 3600.0
        if age_h > max_odds_age_hours:
            stale_count += 1
            continue

        fresh_odds.append(q)

    odds = fresh_odds


    if not odds:
        return [], [], NoBet(
            reason_codes=["MISSING_OR_STALE_ODDS"],
            explanation=[
                "Nessuna quota utilizzabile: mancano quote oppure sono tutte troppo vecchie (stale).",
                f"Regola: max_odds_age_hours={rules.get('max_odds_age_hours', None)}"
            ]
        )


    probs = simulation_outputs.probs
    intervals = simulation_outputs.intervals or {}

    # regole conservative MVP
    min_edge = float(rules.get("min_edge", 0.03))              # 3%
    max_picks = int(rules.get("max_picks", 3))
    max_ci_width = float(rules.get("max_ci_width", 0.06))     # larghezza CI 95% max
    kelly_fraction = float(rules.get("kelly_fraction", 0.25)) # fractional Kelly
    stake_cap_fraction = float(rules.get("stake_cap_fraction", 0.02)) # cap 2% bankroll
    max_model_market_gap = float(rules.get("max_model_market_gap", 0.12))
    min_edge_longshot = float(rules.get("min_edge_longshot", 0.08))
    longshot_odds = float(rules.get("longshot_odds", 5.0))
    max_odds_cap = rules.get("max_odds")
    min_books = int(rules.get("min_books", 1))


    evaluated: List[MarketEvalItem] = []
    candidates: List[Dict[str, Any]] = []

    by_market = defaultdict(list)
    for q in odds:
        by_market[q.market.upper()].append(q)

    implied = {}     # (market, selection) -> p_norm
    overround = {}   # market -> sum(1/odds)
    consensus = {}   # (market, selection) -> median odds
    book_counts = {} # (market, selection) -> count
    best_odds = {}   # (market, selection) -> max odds

    for m, qs in by_market.items():
        by_sel = defaultdict(list)
        for q in qs:
            o = float(q.odds_decimal)
            if o <= 1.0:
                continue
            by_sel[q.selection.upper()].append(o)

        for sel, odds_list in by_sel.items():
            consensus[(m, sel)] = median(odds_list)
            book_counts[(m, sel)] = len(odds_list)
            best_odds[(m, sel)] = max(odds_list)

        s = sum(1.0 / consensus[(m, sel)] for sel in by_sel.keys())
        overround[m] = s
        if s > 0:
            for sel in by_sel.keys():
                implied[(m, sel)] = (1.0 / consensus[(m, sel)]) / s

    for q in odds:
        market = q.market.upper()
        selection = q.selection.upper()
        odds_dec = float(q.odds_decimal)

        item = MarketEvalItem(
            market=market,
            selection=selection,
            bookmaker=q.bookmaker,
            odds_decimal=odds_dec,
            reasons=[]
        )

        

        p = _fair_prob(market, selection, probs)
        if p is None:
            item.reasons.append("NO_MAPPING_FOR_MARKET")
            evaluated.append(item)
            continue

        fair_odds = (1.0 / p) if p > 0 else None
        edge = p * odds_dec - 1.0
        ev = edge

        item.fair_prob = p
        item.fair_odds = fair_odds
        item.edge = edge
        item.ev_per_unit = ev

        # attach market implied
        item.implied_prob = implied.get((market, selection))
        item.market_overround = overround.get(market)
        item.consensus_odds = consensus.get((market, selection))
        item.bookmakers_count = book_counts.get((market, selection))
        if item.consensus_odds:
            item.line_value_pct = (odds_dec / item.consensus_odds) - 1.0

        # divergence gate: se modello troppo distante dal mercato, blocca
        gap = None
        if item.implied_prob is not None:
            gap = abs(item.fair_prob - item.implied_prob)
            if gap > max_model_market_gap:
                item.reasons.append("MODEL_MARKET_DIVERGENCE")
                # hard-block sui mercati "goal based" (piu' fragili / rumorosi)
                if market in ("OU_2.5", "BTTS"):
                    item.reasons.append("DIVERGENCE_HARD_BLOCK")
                item.uncertainty_flag = True


        # incertezza da CI (sticky: se diventa True non viene mai azzerato qui)
        key = _interval_key(market, selection)
        ci = intervals.get(key) if key else None
        ci_width = None
        if ci and "lo" in ci and "hi" in ci:
            width = float(ci["hi"]) - float(ci["lo"])
            ci_width = width
            if width > max_ci_width:
                item.uncertainty_flag = True
                item.reasons.append("HIGH_UNCERTAINTY_CI")
        

        if edge < min_edge:
            item.reasons.append("EDGE_BELOW_THRESHOLD")

        if odds_dec >= longshot_odds and edge < min_edge_longshot:
            item.reasons.append("EDGE_BELOW_LONGSHOT_THRESHOLD")

        best_for_sel = best_odds.get((market, selection))
        if best_for_sel and odds_dec + 1e-6 < best_for_sel:
            item.reasons.append("NOT_BEST_LINE")

        if max_odds_cap is not None and odds_dec > float(max_odds_cap):
            item.reasons.append("ODDS_ABOVE_CAP")

        if item.bookmakers_count is not None and item.bookmakers_count < min_books:
            item.reasons.append("LOW_LIQUIDITY")

        k = _kelly(p, odds_dec)
        if k <= 0:
            item.reasons.append("KELLY_NON_POSITIVE")


        item.confidence = _confidence_score(
            ci_width,
            edge,
            odds_dec,
            (gap if item.implied_prob is not None else None),
            item.line_value_pct,
            item.bookmakers_count,
            rules,
        )
        evaluated.append(item)

        if (
            "EDGE_BELOW_THRESHOLD" not in item.reasons and
            "EDGE_BELOW_LONGSHOT_THRESHOLD" not in item.reasons and
            "KELLY_NON_POSITIVE" not in item.reasons and
            "MODEL_MARKET_DIVERGENCE" not in item.reasons and
            "DIVERGENCE_HARD_BLOCK" not in item.reasons and
            (not item.uncertainty_flag) and
            "NOT_BEST_LINE" not in item.reasons and
            "ODDS_ABOVE_CAP" not in item.reasons and
            "LOW_LIQUIDITY" not in item.reasons
        ):
            cap_dyn = min(stake_cap_fraction, _stake_cap_by_odds(odds_dec))
            stake_frac = min(cap_dyn, kelly_fraction * k)

            # scarta micro-bet inutili (rumore)
            if stake_frac < 0.0025:  # 0.25%
                item.reasons.append("STAKE_TOO_SMALL")
                continue
            candidates.append({
                "market": market,
                "selection": selection,
                "bookmaker": q.bookmaker,
                "odds": odds_dec,
                "p": p,
                "edge": edge,
                "ev": ev,
                "stake_frac": stake_frac,
                "confidence": item.confidence,
                "line_value_pct": item.line_value_pct,
                "consensus_odds": item.consensus_odds,
            })

    if not candidates:
        any_mapped = any(e.fair_prob is not None for e in evaluated)
        if not any_mapped:
            return evaluated, [], NoBet(
                reason_codes=["NO_MAPPED_MARKETS"],
                explanation=["Nessuna quota appartiene ai mercati implementati (1X2, OU_2.5, BTTS)."]
            )
        return evaluated, [], NoBet(
            reason_codes=["NO_EDGE_OR_HIGH_UNCERTAINTY"],
            explanation=[
                f"Nessuna giocata supera i filtri oggettivi (min_edge={min_edge}, max_ci_width={max_ci_width}).",
                "NO BET è corretto quando non c'è edge reale o l'incertezza è alta."
            ]
        )

    # anti-correlazione MVP: max 1 per famiglia mercato
    def family(m: str) -> str:
        if m == "1X2": return "RESULT"
        if m.startswith("OU_"): return "TOTALS"
        if m == "BTTS": return "BTTS"
        return m

    candidates.sort(key=lambda x: x["ev"], reverse=True)

    chosen = []
    used = set()
    for c in candidates:
        fam = family(c["market"])
        if fam in used:
            continue
        used.add(fam)
        chosen.append(c)
        if len(chosen) >= max_picks:
            break

    recs: List[Recommendation] = []
    for c in chosen:
        recs.append(Recommendation(
            bet_id=str(uuid4()),
            market=c["market"],
            selection=c["selection"],
            bookmaker=c["bookmaker"],
            odds_decimal=c["odds"],
            stake_fraction=float(c["stake_frac"]),
            expected_edge=float(c["edge"]),
            expected_ev_per_unit=float(c["ev"]),
            confidence=c.get("confidence"),
            line_value_pct=c.get("line_value_pct"),
            consensus_odds=c.get("consensus_odds"),
            rationale=[
                "fair_prob derivata esclusivamente dalla simulazione Monte Carlo locale",
                "edge/EV calcolati esclusivamente da fair_prob e quota",
                "stake con Kelly frazionario + cap rischio",
                "line value vs consensus bookmaker"
            ]
        ))

    return evaluated, recs, None

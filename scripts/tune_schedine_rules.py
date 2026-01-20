from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs
from app.services.market_rules_service import get_market_rules
from app.services.schedine_rules_service import DEFAULT_RULES


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _parse_grid(values: str) -> List[float]:
    out: List[float] = []
    for part in values.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return sorted(set(out))


def _market_key(market: str, selection: str) -> str | None:
    m = market.upper()
    s = selection.upper()
    if m == "1X2":
        return {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(s)
    if m == "OU_2.5":
        return {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(s)
    if m == "BTTS":
        return {"YES": "btts_yes", "NO": "btts_no"}.get(s)
    return None


def _market_outcome(market: str, selection: str, hg: int, ag: int) -> bool:
    m = market.upper()
    s = selection.upper()
    if m == "1X2":
        if s == "HOME":
            return hg > ag
        if s == "DRAW":
            return hg == ag
        if s == "AWAY":
            return hg < ag
    if m == "OU_2.5":
        total = hg + ag
        if s == "OVER":
            return total >= 3
        if s == "UNDER":
            return total <= 2
    if m == "BTTS":
        btts = (hg > 0 and ag > 0)
        if s == "YES":
            return btts
        if s == "NO":
            return not btts
    return False


def _select_card(
    candidates: List[Dict[str, object]],
    card_size: int,
    min_prob: float,
    min_edge: float,
    max_odds: float | None,
    min_odds: float | None,
    sort_key,
) -> List[Dict[str, object]]:
    picks: List[Dict[str, object]] = []
    used_matches = set()
    for cand in sorted(candidates, key=sort_key, reverse=True):
        match_id = cand["match_id"]
        if match_id in used_matches:
            continue
        if float(cand["prob"]) < min_prob:
            continue
        if float(cand["edge"]) < min_edge:
            continue
        odds = float(cand["odds"])
        if max_odds is not None and odds > max_odds:
            continue
        if min_odds is not None and odds < min_odds:
            continue
        picks.append(cand)
        used_matches.add(match_id)
        if len(picks) >= card_size:
            break
    return picks


def _evaluate_rules(
    per_day: Dict[str, List[Dict[str, object]]],
    card_size: int,
    min_prob: float,
    min_edge: float,
    max_odds: float | None,
    min_odds: float | None,
    sort_key,
) -> Dict[str, float]:
    cards = 0
    hits = 0
    profit = 0.0
    for candidates in per_day.values():
        picks = _select_card(candidates, card_size, min_prob, min_edge, max_odds, min_odds, sort_key)
        if len(picks) < card_size:
            continue
        cards += 1
        total_odds = 1.0
        all_win = True
        for p in picks:
            total_odds *= float(p["odds"])
            if not bool(p["won"]):
                all_win = False
        if all_win:
            hits += 1
            profit += total_odds - 1.0
        else:
            profit -= 1.0
    hit_rate = (hits / cards) if cards else 0.0
    roi = (profit / cards) if cards else 0.0
    return {"cards": cards, "hit_rate": hit_rate, "roi": roi}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--markets", default="1X2,OU_2.5")
    ap.add_argument("--card-size", type=int, default=3)
    ap.add_argument("--easy-min-p-grid", default="0.52,0.54,0.56,0.58,0.60,0.62")
    ap.add_argument("--easy-max-odds-grid", default="2.1,2.3,2.5,2.7")
    ap.add_argument("--medium-min-p-grid", default="0.44,0.46,0.48,0.50,0.52")
    ap.add_argument("--medium-max-odds-grid", default="2.8,3.2,3.6,4.0")
    ap.add_argument("--hard-min-p-grid", default="0.30,0.34,0.38,0.42")
    ap.add_argument("--hard-min-odds-grid", default="2.0,2.4,2.8,3.2")
    ap.add_argument("--max-odds", type=float, default=6.0)
    ap.add_argument("--min-cards-easy", type=int, default=25)
    ap.add_argument("--min-cards-medium", type=int, default=80)
    ap.add_argument("--min-cards-hard", type=int, default=80)
    ap.add_argument("--out", default="data/config/schedine_rules.json")
    ap.add_argument("--report", default="data/reports/schedine_rules_tuning.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}
    markets = {m.strip().upper() for m in args.markets.split(",") if m.strip()}

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)
    min_edge = float(get_market_rules().get("min_edge", 0.03))

    per_day: Dict[str, List[Dict[str, object]]] = {}

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT m.match_id, m.season, m.kickoff_utc, m.home, m.away, u.home_goals, u.away_goals, f.features_json
            FROM matches m
            JOIN understat_matches u
              ON u.understat_match_id = replace(m.match_id, 'understat:', '')
            JOIN match_features f
              ON f.match_id = m.match_id
            WHERE m.competition = ?
              AND m.match_id LIKE 'understat:%'
              AND f.features_version = ?
            """,
            (args.league, args.features_version),
        ).fetchall()

        for m in matches:
            if m["season"] not in season_labels:
                continue
            hg = m["home_goals"]
            ag = m["away_goals"]
            if hg is None or ag is None:
                continue
            features = json.loads(m["features_json"])
            lam_h = float(features.get("lambda_home", 0.0))
            lam_a = float(features.get("lambda_away", 0.0))
            if lam_h <= 0 or lam_a <= 0:
                continue

            probs = match_probs(lam_h, lam_a, cap=8, rho=rho)
            if cal:
                kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
                cal_sel = select_calibration(cal, m["season"], kickoff)
                if cal_sel:
                    probs = apply_calibration(probs, cal_sel)

            odds_rows = conn.execute(
                """
                SELECT market, selection, odds_decimal, retrieved_at_utc, source_id
                FROM odds_quotes
                WHERE match_id = ?
                  AND retrieved_at_utc <= ?
                """,
                (m["match_id"], m["kickoff_utc"]),
            ).fetchall()
            if not odds_rows:
                continue

            # best odds per market/selection (pre-kickoff only)
            best: Dict[Tuple[str, str], float] = {}
            for r in odds_rows:
                if r["source_id"] and "closing" in str(r["source_id"]):
                    continue
                market = str(r["market"]).upper()
                selection = str(r["selection"]).upper()
                if market not in markets:
                    continue
                odds = float(r["odds_decimal"])
                if odds <= 1.01 or odds > args.max_odds:
                    continue
                key = (market, selection)
                if key not in best or odds > best[key]:
                    best[key] = odds
            if not best:
                continue

            kickoff_date = str(m["kickoff_utc"]).split("T")[0]
            candidates = per_day.setdefault(kickoff_date, [])
            for (market, selection), odds in best.items():
                key = _market_key(market, selection)
                if not key:
                    continue
                p = float(probs.get(key, 0.0))
                edge = p * odds - 1.0
                candidates.append({
                    "match_id": m["match_id"],
                    "home": m["home"],
                    "away": m["away"],
                    "market": market,
                    "selection": selection,
                    "odds": odds,
                    "prob": p,
                    "edge": edge,
                    "won": _market_outcome(market, selection, int(hg), int(ag)),
                })

    easy_min_p = _parse_grid(args.easy_min_p_grid)
    easy_max_odds = _parse_grid(args.easy_max_odds_grid)
    medium_min_p = _parse_grid(args.medium_min_p_grid)
    medium_max_odds = _parse_grid(args.medium_max_odds_grid)
    hard_min_p = _parse_grid(args.hard_min_p_grid)
    hard_min_odds = _parse_grid(args.hard_min_odds_grid)

    results = {"easy": [], "medium": [], "hard": []}

    for mp in easy_min_p:
        for mx in easy_max_odds:
            stats = _evaluate_rules(
                per_day,
                args.card_size,
                mp,
                min_edge,
                mx,
                None,
                sort_key=lambda c: (c["prob"], c["edge"]),
            )
            results["easy"].append({"min_prob": mp, "max_odds": mx, **stats})

    for mp in medium_min_p:
        for mx in medium_max_odds:
            stats = _evaluate_rules(
                per_day,
                args.card_size,
                mp,
                min_edge,
                mx,
                None,
                sort_key=lambda c: (c["edge"], c["prob"]),
            )
            results["medium"].append({"min_prob": mp, "max_odds": mx, **stats})

    for mp in hard_min_p:
        for mn in hard_min_odds:
            stats = _evaluate_rules(
                per_day,
                args.card_size,
                mp,
                min_edge,
                args.max_odds,
                mn,
                sort_key=lambda c: (c["odds"], c["edge"]),
            )
            results["hard"].append({"min_prob": mp, "min_odds": mn, "max_odds": args.max_odds, **stats})

    def _pick_best(items: List[Dict[str, float]], min_cards: int, score_key: str):
        eligible = [i for i in items if i["cards"] >= min_cards]
        if not eligible:
            eligible = items
        return max(eligible, key=lambda x: (x[score_key], x["cards"]))

    best_easy = _pick_best(results["easy"], args.min_cards_easy, "hit_rate")
    best_medium = _pick_best(results["medium"], args.min_cards_medium, "roi")
    best_hard = _pick_best(results["hard"], args.min_cards_hard, "roi")

    rules = dict(DEFAULT_RULES)
    rules["card_size"] = args.card_size
    rules["markets"] = sorted(markets)
    rules["min_edge"] = min_edge
    rules["easy"] = {"min_prob": best_easy["min_prob"], "max_odds": best_easy["max_odds"]}
    rules["medium"] = {"min_prob": best_medium["min_prob"], "max_odds": best_medium["max_odds"]}
    rules["hard"] = {"min_prob": best_hard["min_prob"], "min_odds": best_hard["min_odds"], "max_odds": best_hard["max_odds"]}

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=True, indent=2)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "min_edge": min_edge,
        "selected": {"easy": best_easy, "medium": best_medium, "hard": best_hard},
        "grid": results,
        "output_rules_path": args.out,
    }
    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=True, indent=2)

    print(f"OK: schedine rules saved to {args.out}")
    print(f"Easy: min_prob={best_easy['min_prob']} max_odds={best_easy['max_odds']} hit_rate={best_easy['hit_rate']:.3f} cards={best_easy['cards']}")
    print(f"Medium: min_prob={best_medium['min_prob']} max_odds={best_medium['max_odds']} roi={best_medium['roi']:.3f} cards={best_medium['cards']}")
    print(f"Hard: min_prob={best_hard['min_prob']} min_odds={best_hard['min_odds']} roi={best_hard['roi']:.3f} cards={best_hard['cards']}")
    print(f"Report saved to {args.report}")


if __name__ == "__main__":
    main()

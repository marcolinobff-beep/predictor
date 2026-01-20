from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone, date, timedelta
from typing import Dict, List, Tuple, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs
from app.services.market_rules_service import get_market_rules


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _list_matches(conn, league: str, season_labels: set, start: Optional[str], end: Optional[str]):
    sql = """
        SELECT match_id, season, kickoff_utc, home, away
        FROM matches
        WHERE competition = ?
    """
    params: List[object] = [league]
    if season_labels:
        sql += " AND season IN ({})".format(",".join(["?"] * len(season_labels)))
        params.extend(sorted(season_labels))
    if start:
        sql += " AND kickoff_utc >= ?"
        params.append(start)
    if end:
        sql += " AND kickoff_utc < ?"
        params.append(end)
    sql += " ORDER BY kickoff_utc ASC"
    return conn.execute(sql, params).fetchall()


def _brier(records: List[Tuple[float, int]]) -> float:
    if not records:
        return 0.0
    return sum((p - o) ** 2 for p, o in records) / len(records)


def _logloss_1x2(p_home: float, p_draw: float, p_away: float, outcome: str) -> float:
    eps = 1e-12
    if outcome == "H":
        return -math.log(max(p_home, eps))
    if outcome == "D":
        return -math.log(max(p_draw, eps))
    return -math.log(max(p_away, eps))


def _market_outcome(market: str, selection: str, hg: int, ag: int) -> bool:
    market = market.upper()
    selection = selection.upper()
    if market == "1X2":
        if selection == "HOME":
            return hg > ag
        if selection == "DRAW":
            return hg == ag
        if selection == "AWAY":
            return hg < ag
    if market == "OU_2.5":
        total = hg + ag
        if selection == "OVER":
            return total >= 3
        if selection == "UNDER":
            return total <= 2
    if market == "BTTS":
        btts = (hg > 0 and ag > 0)
        if selection == "YES":
            return btts
        if selection == "NO":
            return not btts
    return False


def _fetch_best_odds(conn, match_id: str, kickoff_utc: str) -> Dict[Tuple[str, str], float]:
    rows = conn.execute(
        """
        SELECT market, selection, odds_decimal
        FROM odds_quotes
        WHERE match_id = ?
          AND retrieved_at_utc <= ?
        """,
        (match_id, kickoff_utc),
    ).fetchall()
    best: Dict[Tuple[str, str], float] = {}
    for r in rows:
        market = str(r["market"]).upper()
        selection = str(r["selection"]).upper()
        odds = float(r["odds_decimal"])
        if odds <= 1.01:
            continue
        key = (market, selection)
        if key not in best or odds > best[key]:
            best[key] = odds
    return best


def _select_card(
    candidates: List[Dict[str, object]],
    card_size: int,
    min_prob: float,
    min_edge: float,
    max_odds: Optional[float],
    min_odds: Optional[float],
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", default="", help="Comma separated season start years, es: 2021,2022")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (exclusive)")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--out", default=None, help="Optional JSON output path")
    ap.add_argument("--build-cards", action="store_true")
    ap.add_argument("--card-size", type=int, default=3)
    ap.add_argument("--markets", default="1X2,OU_2.5")
    ap.add_argument("--min-edge", type=float, default=None)
    ap.add_argument("--max-odds", type=float, default=6.0)
    ap.add_argument("--easy-min-p", type=float, default=0.58)
    ap.add_argument("--easy-max-odds", type=float, default=2.5)
    ap.add_argument("--medium-min-p", type=float, default=0.48)
    ap.add_argument("--medium-max-odds", type=float, default=3.6)
    ap.add_argument("--hard-min-p", type=float, default=0.35)
    ap.add_argument("--hard-min-odds", type=float, default=2.2)
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons} if seasons else set()

    start_iso = None
    end_iso = None
    if args.start:
        start_iso = datetime.combine(_parse_date(args.start), datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if args.end:
        end_iso = datetime.combine(_parse_date(args.end), datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)
    min_edge = float(args.min_edge) if args.min_edge is not None else float(get_market_rules().get("min_edge", 0.03))

    per_day: Dict[str, Dict[str, float]] = {}
    overall = {"matches": 0, "acc": 0, "logloss": [], "brier_home": [], "brier_draw": [], "brier_away": []}
    day_candidates: Dict[str, List[Dict[str, object]]] = {}
    allowed_markets = {m.strip().upper() for m in args.markets.split(",") if m.strip()}

    with get_conn() as conn:
        matches = _list_matches(conn, args.league, season_labels, start_iso, end_iso)
        for m in matches:
            match_id = m["match_id"]
            if not match_id.startswith("understat:"):
                continue

            feat = conn.execute(
                """
                SELECT features_json
                FROM match_features
                WHERE match_id = ? AND features_version = ?
                ORDER BY created_at_utc DESC
                LIMIT 1
                """,
                (match_id, args.features_version),
            ).fetchone()
            if not feat:
                continue

            understat_id = match_id.split(":", 1)[1]
            us = conn.execute(
                """
                SELECT home_goals, away_goals
                FROM understat_matches
                WHERE understat_match_id = ?
                """,
                (understat_id,),
            ).fetchone()
            if not us or us["home_goals"] is None or us["away_goals"] is None:
                continue

            features = json.loads(feat["features_json"])
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

            hg = int(us["home_goals"])
            ag = int(us["away_goals"])
            outcome = "H" if hg > ag else ("D" if hg == ag else "A")
            pred = max((probs["home_win"], "H"), (probs["draw"], "D"), (probs["away_win"], "A"))[1]

            kickoff_date = str(m["kickoff_utc"]).split("T")[0]
            day = per_day.setdefault(kickoff_date, {"matches": 0, "acc": 0, "logloss": []})
            day["matches"] += 1
            if pred == outcome:
                day["acc"] += 1
                overall["acc"] += 1
            day["logloss"].append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))

            overall["matches"] += 1
            overall["logloss"].append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))
            overall["brier_home"].append((probs["home_win"], 1 if outcome == "H" else 0))
            overall["brier_draw"].append((probs["draw"], 1 if outcome == "D" else 0))
            overall["brier_away"].append((probs["away_win"], 1 if outcome == "A" else 0))

            if args.build_cards:
                odds_best = _fetch_best_odds(conn, match_id, m["kickoff_utc"])
                if not odds_best:
                    continue
                for (market, selection), odds in odds_best.items():
                    if market not in allowed_markets:
                        continue
                    key = None
                    if market == "1X2":
                        key = {"HOME": "home_win", "DRAW": "draw", "AWAY": "away_win"}.get(selection)
                    elif market == "OU_2.5":
                        key = {"OVER": "over_2_5", "UNDER": "under_2_5"}.get(selection)
                    elif market == "BTTS":
                        key = {"YES": "btts_yes", "NO": "btts_no"}.get(selection)
                    if not key:
                        continue
                    prob = float(probs.get(key, 0.0))
                    edge = prob * float(odds) - 1.0
                    cand = {
                        "match_id": match_id,
                        "home": m["home"],
                        "away": m["away"],
                        "market": market,
                        "selection": selection,
                        "odds": float(odds),
                        "prob": prob,
                        "edge": edge,
                        "won": _market_outcome(market, selection, hg, ag),
                    }
                    day_candidates.setdefault(kickoff_date, []).append(cand)

    # finalize
    report = {"per_day": {}, "overall": {}}
    for day, stats in sorted(per_day.items()):
        matches_count = stats["matches"]
        acc = (stats["acc"] / matches_count) if matches_count else 0.0
        logloss = (sum(stats["logloss"]) / len(stats["logloss"])) if stats["logloss"] else 0.0
        report["per_day"][day] = {"matches": matches_count, "acc_1x2": acc, "logloss_1x2": logloss}

    if overall["matches"] > 0:
        report["overall"] = {
            "matches": overall["matches"],
            "acc_1x2": overall["acc"] / overall["matches"],
            "logloss_1x2": sum(overall["logloss"]) / len(overall["logloss"]),
            "brier_home": _brier(overall["brier_home"]),
            "brier_draw": _brier(overall["brier_draw"]),
            "brier_away": _brier(overall["brier_away"]),
        }

    if args.build_cards:
        cards_report: Dict[str, Dict[str, object]] = {}
        summary = {
            "easy": {"cards": 0, "hits": 0},
            "medium": {"cards": 0, "hits": 0},
            "hard": {"cards": 0, "hits": 0},
        }

        for day, candidates in sorted(day_candidates.items()):
            if not candidates:
                continue
            cards_for_day: Dict[str, object] = {}

            easy = _select_card(
                candidates,
                args.card_size,
                args.easy_min_p,
                min_edge,
                args.easy_max_odds,
                None,
                sort_key=lambda c: (c["prob"], c["edge"]),
            )
            medium = _select_card(
                candidates,
                args.card_size,
                args.medium_min_p,
                min_edge,
                args.medium_max_odds,
                None,
                sort_key=lambda c: (c["edge"], c["prob"]),
            )
            hard = _select_card(
                candidates,
                args.card_size,
                args.hard_min_p,
                min_edge,
                args.max_odds,
                args.hard_min_odds,
                sort_key=lambda c: (c["odds"], c["edge"]),
            )

            for name, picks in (("easy", easy), ("medium", medium), ("hard", hard)):
                if not picks or len(picks) < args.card_size:
                    continue
                hit = all(bool(p["won"]) for p in picks)
                cards_for_day[name] = {"picks": picks, "hit": hit}
                summary[name]["cards"] += 1
                if hit:
                    summary[name]["hits"] += 1

            if cards_for_day:
                cards_report[day] = cards_for_day

        report["cards"] = cards_report
        report["cards_summary"] = {
            k: {
                "cards": v["cards"],
                "hit_rate": (v["hits"] / v["cards"]) if v["cards"] else 0.0,
            }
            for k, v in summary.items()
        }

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=True, indent=2)

    print("Stress test summary")
    print("Overall:", report.get("overall"))
    print("Days:", len(report["per_day"]))
    if args.build_cards and report.get("cards_summary"):
        print("Cards summary:", report["cards_summary"])


if __name__ == "__main__":
    main()

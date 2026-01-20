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
from app.services.market_rules_service import DEFAULT_RULES


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _parse_grid(values: str) -> List[float]:
    out = []
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--min-edge-grid", default="0.01,0.015,0.02,0.025,0.03,0.035,0.04")
    ap.add_argument("--max-odds", type=float, default=6.0)
    ap.add_argument("--min-picks", type=int, default=300)
    ap.add_argument("--min-model-confidence", type=float, default=None)
    ap.add_argument("--out", default="data/config/market_rules.json")
    ap.add_argument("--report", default="data/reports/market_rules_tuning.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}
    grid = _parse_grid(args.min_edge_grid)
    if not grid:
        raise SystemExit("min-edge-grid is empty.")

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)

    best_picks: List[Dict[str, object]] = []

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT match_id, season, kickoff_utc
            FROM matches
            WHERE competition = ?
            """,
            (args.league,),
        ).fetchall()

        for m in matches:
            if m["season"] not in season_labels:
                continue
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

            odds_rows = conn.execute(
                """
                SELECT market, selection, odds_decimal, source_id
                FROM odds_quotes
                WHERE match_id = ?
                  AND retrieved_at_utc <= ?
                """,
                (match_id, m["kickoff_utc"]),
            ).fetchall()
            if not odds_rows:
                continue

            pre_rows = [
                r for r in odds_rows
                if not (r["source_id"] and "closing" in str(r["source_id"]))
            ]
            if not pre_rows:
                continue

            best = None
            for r in pre_rows:
                odds = float(r["odds_decimal"])
                if odds <= 1.01 or odds > args.max_odds:
                    continue
                key = _market_key(str(r["market"]), str(r["selection"]))
                if not key:
                    continue
                p = float(probs.get(key, 0.0))
                if p <= 0:
                    continue
                edge = p * odds - 1.0
                if best is None or edge > best["edge"]:
                    best = {
                        "edge": edge,
                        "odds": odds,
                        "market": str(r["market"]),
                        "selection": str(r["selection"]),
                        "won": _market_outcome(str(r["market"]), str(r["selection"]), hg, ag),
                    }

            if best is None:
                continue
            best_picks.append(best)

    results = []
    for thr in grid:
        picks = 0
        profit = 0.0
        for item in best_picks:
            if float(item["edge"]) >= thr:
                picks += 1
                profit += (float(item["odds"]) - 1.0) if item["won"] else -1.0
        roi = (profit / picks) if picks else 0.0
        results.append({"min_edge": thr, "picks": picks, "roi": roi})

    eligible = [r for r in results if r["picks"] >= args.min_picks]
    if eligible:
        best = max(eligible, key=lambda r: (r["roi"], r["picks"]))
    else:
        best = max(results, key=lambda r: (r["picks"], r["roi"]))

    tuned = dict(DEFAULT_RULES)
    tuned["min_edge"] = best["min_edge"]
    tuned["max_odds"] = args.max_odds
    tuned["min_edge_longshot"] = max(tuned["min_edge_longshot"], float(best["min_edge"]) + 0.05)
    if args.min_model_confidence is not None:
        tuned["min_model_confidence"] = float(args.min_model_confidence)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(tuned, f, ensure_ascii=True, indent=2)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "min_picks": args.min_picks,
        "grid": results,
        "selected": best,
        "output_rules_path": args.out,
    }
    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=True, indent=2)

    print(f"OK: tuned min_edge={best['min_edge']} picks={best['picks']} roi={best['roi']:.3f}")
    print(f"Rules saved to {args.out}")
    print(f"Report saved to {args.report}")


if __name__ == "__main__":
    main()

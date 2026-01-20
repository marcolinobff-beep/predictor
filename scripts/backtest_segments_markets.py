from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
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


def _logloss_1x2(p_home: float, p_draw: float, p_away: float, outcome: str) -> float:
    eps = 1e-12
    if outcome == "H":
        return -math.log(max(p_home, eps))
    if outcome == "D":
        return -math.log(max(p_draw, eps))
    return -math.log(max(p_away, eps))


def _phase_for_date(dt: datetime) -> str:
    m = dt.month
    if m in (8, 9, 10):
        return "early"
    if m in (11, 12, 1, 2):
        return "mid"
    return "late"


def _segment_labels(feat: dict, kickoff: datetime) -> Dict[str, str]:
    phase = _phase_for_date(kickoff)
    elo_h = float(feat.get("elo_home", 1500))
    elo_a = float(feat.get("elo_away", 1500))
    diff = elo_h - elo_a
    strength = "balanced"
    if diff >= 80:
        strength = "home_fav"
    elif diff <= -80:
        strength = "away_fav"

    if elo_h >= 1600 and elo_a >= 1600:
        big_small = "big_vs_big"
    elif elo_h <= 1500 and elo_a <= 1500:
        big_small = "small_vs_small"
    elif abs(diff) >= 80:
        big_small = "big_vs_small"
    else:
        big_small = "mid_vs_mid"

    rest_h = feat.get("rest_days_home")
    rest_a = feat.get("rest_days_away")
    rest_adv = "even_rest"
    if rest_h is not None and rest_a is not None:
        if rest_h - rest_a >= 3:
            rest_adv = "home_rest_adv"
        elif rest_a - rest_h >= 3:
            rest_adv = "away_rest_adv"

    return {
        "phase": phase,
        "strength": strength,
        "big_small": big_small,
        "rest_adv": rest_adv,
    }


def _best_pick(
    probs: Dict[str, float],
    odds_rows: list,
    min_edge: float,
    max_odds: float,
) -> Optional[dict]:
    pre_rows = [
        r for r in odds_rows
        if not (r["source_id"] and "closing" in str(r["source_id"]))
    ]
    if not pre_rows:
        pre_rows = odds_rows

    best = None
    for r in pre_rows:
        market = str(r["market"]).upper()
        selection = str(r["selection"]).upper()
        odds_dec = float(r["odds_decimal"])
        if odds_dec <= 1.01 or odds_dec > max_odds:
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
        p = probs.get(key)
        if p is None:
            continue
        edge = p * odds_dec - 1.0
        if edge < min_edge:
            continue
        if not best or edge > best["edge"]:
            best = {"market": market, "selection": selection, "odds": odds_dec, "edge": edge}
    return best


def _closing_odds(odds_rows: list, market: str, selection: str) -> Optional[float]:
    closing_rows = [
        r for r in odds_rows
        if r["source_id"] and "closing" in str(r["source_id"])
        and r["market"] == market
        and r["selection"] == selection
    ]
    if closing_rows:
        closing_rows.sort(key=lambda x: x["retrieved_at_utc"])
        return float(closing_rows[-1]["odds_decimal"])

    pre_rows = [
        r for r in odds_rows
        if not (r["source_id"] and "closing" in str(r["source_id"]))
        and r["market"] == market
        and r["selection"] == selection
    ]
    ts_vals = sorted({str(r["retrieved_at_utc"]) for r in pre_rows})
    if len(ts_vals) >= 2:
        last_ts = ts_vals[-1]
        for r in pre_rows:
            if str(r["retrieved_at_utc"]) == last_ts:
                return float(r["odds_decimal"])
    return None


def _market_outcome(market: str, selection: str, hg: int, ag: int) -> bool:
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


def _init_bucket() -> dict:
    return {"matches": 0, "picks": 0, "profit": 0.0, "clv": [], "logloss": []}


def _finalize_bucket(bucket: dict) -> dict:
    picks = bucket["picks"]
    roi = (bucket["profit"] / picks) if picks else 0.0
    clv_vals = bucket["clv"]
    return {
        "matches": bucket["matches"],
        "picks": picks,
        "roi": roi,
        "avg_clv": (sum(clv_vals) / len(clv_vals)) if clv_vals else None,
        "logloss_1x2": (sum(bucket["logloss"]) / len(bucket["logloss"])) if bucket["logloss"] else 0.0,
        "clv_samples": len(clv_vals),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--min-edge", type=float, default=None)
    ap.add_argument("--max-odds", type=float, default=6.0)
    ap.add_argument("--out", default="data/reports/segments_market_report.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)
    min_edge = float(args.min_edge) if args.min_edge is not None else float(get_market_rules().get("min_edge", 0.03))

    buckets = {
        "phase": {},
        "strength": {},
        "big_small": {},
        "rest_adv": {},
    }

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

            feat_row = conn.execute(
                """
                SELECT features_json
                FROM match_features
                WHERE match_id = ? AND features_version = ?
                ORDER BY created_at_utc DESC
                LIMIT 1
                """,
                (match_id, args.features_version),
            ).fetchone()
            if not feat_row:
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

            features = json.loads(feat_row["features_json"])
            lam_h = float(features.get("lambda_home", 0.0))
            lam_a = float(features.get("lambda_away", 0.0))
            if lam_h <= 0 or lam_a <= 0:
                continue

            probs = match_probs(lam_h, lam_a, cap=8, rho=rho)
            kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
            if cal:
                cal_sel = select_calibration(cal, m["season"], kickoff)
                if cal_sel:
                    probs = apply_calibration(probs, cal_sel)

            hg = int(us["home_goals"])
            ag = int(us["away_goals"])
            outcome = "H" if hg > ag else ("D" if hg == ag else "A")

            odds_rows = conn.execute(
                """
                SELECT market, selection, odds_decimal, retrieved_at_utc, source_id
                FROM odds_quotes
                WHERE match_id = ?
                  AND retrieved_at_utc <= ?
                """,
                (match_id, m["kickoff_utc"]),
            ).fetchall()
            if not odds_rows:
                continue

            best = _best_pick(probs, odds_rows, min_edge=min_edge, max_odds=args.max_odds)
            if not best:
                continue

            win = _market_outcome(best["market"], best["selection"], hg, ag)
            profit = (best["odds"] - 1.0) if win else -1.0
            closing = _closing_odds(odds_rows, best["market"], best["selection"])
            clv = (closing / best["odds"]) - 1.0 if closing else None

            segs = _segment_labels(features, kickoff)
            for seg_name, seg_value in segs.items():
                bucket = buckets[seg_name].setdefault(seg_value, _init_bucket())
                bucket["matches"] += 1
                bucket["picks"] += 1
                bucket["profit"] += profit
                bucket["logloss"].append(_logloss_1x2(
                    probs["home_win"], probs["draw"], probs["away_win"], outcome
                ))
                if clv is not None:
                    bucket["clv"].append(clv)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "segments": {},
    }
    for seg, items in buckets.items():
        report["segments"][seg] = {k: _finalize_bucket(v) for k, v in items.items()}

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote segments market report to {args.out}")


if __name__ == "__main__":
    main()

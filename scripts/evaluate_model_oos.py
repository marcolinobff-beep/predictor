from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.calibration import apply_calibration
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs


def _logloss_1x2(p_home: float, p_draw: float, p_away: float, outcome: str) -> float:
    eps = 1e-12
    if outcome == "H":
        return -math.log(max(p_home, eps))
    if outcome == "D":
        return -math.log(max(p_draw, eps))
    return -math.log(max(p_away, eps))


def _temp_scale_1x2(p_home: float, p_draw: float, p_away: float, temp: float) -> Tuple[float, float, float]:
    if temp <= 0:
        return p_home, p_draw, p_away
    eps = 1e-12
    p1 = max(p_home, eps) ** (1.0 / temp)
    px = max(p_draw, eps) ** (1.0 / temp)
    p2 = max(p_away, eps) ** (1.0 / temp)
    s = p1 + px + p2
    if s <= 0:
        return p_home, p_draw, p_away
    return p1 / s, px / s, p2 / s


def _fit_temperature(train_1x2: List[Tuple[float, float, float, str]]) -> Tuple[float, float]:
    if not train_1x2:
        return 1.0, 0.0
    best_t = 1.0
    best_loss = float("inf")
    for i in range(50):
        t = 0.5 + i * 0.05
        loss = 0.0
        for p1, px, p2, outcome in train_1x2:
            s1, sx, s2 = _temp_scale_1x2(p1, px, p2, t)
            loss += _logloss_1x2(s1, sx, s2, outcome)
        loss /= len(train_1x2)
        if loss < best_loss:
            best_loss = loss
            best_t = t
    return best_t, best_loss


def _brier(records: List[Tuple[float, int]]) -> float:
    if not records:
        return 0.0
    return sum((p - o) ** 2 for p, o in records) / len(records)


def _build_bins(records: Dict[str, List[Tuple[float, int]]], bins: List[Tuple[float, float]], min_count: int):
    out = {}
    for key, data in records.items():
        market_bins = []
        if not data:
            out[key] = market_bins
            continue
        overall_rate = sum(o for _, o in data) / len(data)
        for lo, hi in bins:
            bin_items = [o for p, o in data if lo <= p < hi]
            if len(bin_items) < min_count:
                market_bins.append({"min": lo, "max": hi, "p": overall_rate, "count": len(bin_items)})
            else:
                p = sum(bin_items) / len(bin_items)
                market_bins.append({"min": lo, "max": hi, "p": p, "count": len(bin_items)})
        out[key] = market_bins
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--split-date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--bins", type=int, default=20)
    ap.add_argument("--min-count", type=int, default=30)
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    args = ap.parse_args()

    split_dt = datetime.fromisoformat(args.split_date).replace(tzinfo=timezone.utc)
    rho = get_rho(args.dc_params, args.league)

    train_records = {
        "home_win": [],
        "draw": [],
        "away_win": [],
        "over_2_5": [],
        "under_2_5": [],
        "btts_yes": [],
        "btts_no": [],
    }

    train_1x2 = []
    test_brier = {k: [] for k in train_records}
    test_logloss = []
    test_1x2 = []

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT m.match_id, m.kickoff_utc, u.home_goals, u.away_goals, f.features_json
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
        hg = m["home_goals"]
        ag = m["away_goals"]
        if hg is None or ag is None:
            continue
        kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
        features = json.loads(m["features_json"])
        lam_h = float(features.get("lambda_home", 0.0))
        lam_a = float(features.get("lambda_away", 0.0))
        if lam_h <= 0 or lam_a <= 0:
            continue
        probs = match_probs(lam_h, lam_a, cap=8, rho=rho)

        if kickoff < split_dt:
            train_records["home_win"].append((probs["home_win"], 1 if hg > ag else 0))
            train_records["draw"].append((probs["draw"], 1 if hg == ag else 0))
            train_records["away_win"].append((probs["away_win"], 1 if hg < ag else 0))
            train_records["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
            train_records["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
            train_records["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
            train_records["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))
            outcome = "H" if hg > ag else ("D" if hg == ag else "A")
            train_1x2.append((probs["home_win"], probs["draw"], probs["away_win"], outcome))
        else:
            outcome = "H" if hg > ag else ("D" if hg == ag else "A")
            test_logloss.append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))
            test_1x2.append((probs["home_win"], probs["draw"], probs["away_win"], outcome))
            test_brier["home_win"].append((probs["home_win"], 1 if outcome == "H" else 0))
            test_brier["draw"].append((probs["draw"], 1 if outcome == "D" else 0))
            test_brier["away_win"].append((probs["away_win"], 1 if outcome == "A" else 0))
            test_brier["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
            test_brier["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
            test_brier["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
            test_brier["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))

    bins = [(i / args.bins, (i + 1) / args.bins) for i in range(args.bins)]
    cal = {"markets": _build_bins(train_records, bins, args.min_count)}

    # apply calibration to test metrics
    calibrated_brier = {k: [] for k in test_brier}
    calibrated_logloss = []

    # temperature scaling for 1X2
    temp_t, temp_loss = _fit_temperature(train_1x2)
    temp_logloss = []
    temp_brier = {k: [] for k in ("home_win", "draw", "away_win")}

    # calibrate 1X2 using full vector
    for a, b, c, outcome in test_1x2:
        cal_probs = apply_calibration({"home_win": a, "draw": b, "away_win": c}, cal)
        calibrated_logloss.append(_logloss_1x2(
            cal_probs["home_win"], cal_probs["draw"], cal_probs["away_win"], outcome
        ))
        calibrated_brier["home_win"].append((cal_probs["home_win"], 1 if outcome == "H" else 0))
        calibrated_brier["draw"].append((cal_probs["draw"], 1 if outcome == "D" else 0))
        calibrated_brier["away_win"].append((cal_probs["away_win"], 1 if outcome == "A" else 0))

        t1, tx, t2 = _temp_scale_1x2(a, b, c, temp_t)
        temp_logloss.append(_logloss_1x2(t1, tx, t2, outcome))
        temp_brier["home_win"].append((t1, 1 if outcome == "H" else 0))
        temp_brier["draw"].append((tx, 1 if outcome == "D" else 0))
        temp_brier["away_win"].append((t2, 1 if outcome == "A" else 0))

    # calibrate OU/BTTS independently
    for k in ("over_2_5", "under_2_5", "btts_yes", "btts_no"):
        for p, o in test_brier[k]:
            probs = apply_calibration({k: p}, cal)
            calibrated_brier[k].append((probs.get(k, p), o))

    out = {
        "split_date": args.split_date,
        "features_version": args.features_version,
        "dc_rho": rho,
        "temp_scale_1x2": temp_t,
        "temp_train_logloss_1x2": temp_loss,
        "train_counts": {k: len(v) for k, v in train_records.items()},
        "test_counts": {k: len(v) for k, v in test_brier.items()},
        "test_logloss_1x2": sum(test_logloss) / len(test_logloss) if test_logloss else 0.0,
        "test_logloss_1x2_calibrated": sum(calibrated_logloss) / len(calibrated_logloss) if calibrated_logloss else 0.0,
        "test_logloss_1x2_temp": sum(temp_logloss) / len(temp_logloss) if temp_logloss else 0.0,
        "test_brier": {k: _brier(v) for k, v in test_brier.items()},
        "test_brier_calibrated": {k: _brier(v) for k, v in calibrated_brier.items()},
        "test_brier_temp_1x2": {k: _brier(v) for k, v in temp_brier.items()},
    }

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

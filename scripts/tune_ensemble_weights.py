from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.core.calibration import (
    apply_calibration,
    load_calibration,
    select_calibration,
    select_league_calibration,
)
from app.core.config import settings
from app.core.dc_params import get_rho
from app.core.gbm_light import load_model, predict_probs
from app.core.probabilities import match_probs
from app.db.sqlite import get_conn


DEFAULT_LEAGUES = ["Serie_A", "EPL", "Bundesliga", "La_Liga", "Ligue_1"]


def _parse_list(value: Optional[str]) -> List[str]:
    if not value:
        return DEFAULT_LEAGUES
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_weights(step: float) -> List[float]:
    if step <= 0:
        step = 0.05
    n = int(round(1.0 / step))
    weights = [round(i * step, 4) for i in range(n + 1)]
    if weights[-1] != 1.0:
        weights.append(1.0)
    return weights


def _logloss_1x2(p_home: float, p_draw: float, p_away: float, outcome: str) -> float:
    eps = 1e-12
    if outcome == "H":
        return -math.log(max(p_home, eps))
    if outcome == "D":
        return -math.log(max(p_draw, eps))
    return -math.log(max(p_away, eps))


def _blend_probs(base: Dict[str, float], extra: Dict[str, float], weight: float) -> Dict[str, float]:
    w = max(0.0, min(1.0, weight))
    out = dict(base)
    for k, p in (extra or {}).items():
        if k in out:
            out[k] = (1.0 - w) * out[k] + w * float(p)
        else:
            out[k] = float(p)
    keys_1x2 = ["home_win", "draw", "away_win"]
    if all(k in out for k in keys_1x2):
        s = sum(out[k] for k in keys_1x2)
        if s > 0:
            for k in keys_1x2:
                out[k] = out[k] / s
    return out


def _evaluate(
    records: List[Tuple[Dict[str, float], Dict[str, float], str, Optional[Dict[str, object]]]],
    weight: float,
    apply_cal: bool,
) -> float:
    if not records:
        return 0.0
    total = 0.0
    count = 0
    for probs_dc, probs_gbm, outcome, cal_sel in records:
        probs = _blend_probs(probs_dc, probs_gbm, weight)
        if apply_cal and cal_sel:
            probs = apply_calibration(probs, cal_sel)
        total += _logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome)
        count += 1
    return total / count if count else 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--split-date", default="2024-07-01")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--step", type=float, default=0.05)
    ap.add_argument("--gbm-model", default=None)
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--calibration", default=None)
    ap.add_argument("--calibration-by-season", default=None)
    ap.add_argument("--skip-calibration", action="store_true")
    ap.add_argument("--out", default="data/calibration/ensemble_weights.json")
    args = ap.parse_args()

    split_dt = datetime.fromisoformat(args.split_date).replace(tzinfo=timezone.utc)
    weights = _parse_weights(args.step)

    gbm_path = args.gbm_model or settings.gbm_model_path
    cal_by_season = args.calibration_by_season or settings.calibration_by_season_path
    cal_base = args.calibration or settings.calibration_path
    cal = load_calibration(cal_by_season) or load_calibration(cal_base)

    results = {}
    errors = {}
    for league in _parse_list(args.leagues):
        try:
            rho = get_rho(args.dc_params, league)
            gbm_model = load_model(gbm_path, league)
            if not gbm_model:
                errors[league] = "missing_gbm_model"
                continue

            league_cal = select_league_calibration(cal, league) if cal and not args.skip_calibration else None

            records = []
            with get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT m.match_id, m.kickoff_utc, m.season,
                           u.home_goals, u.away_goals, f.features_json
                    FROM matches m
                    JOIN understat_matches u
                      ON u.understat_match_id = replace(m.match_id, 'understat:', '')
                    JOIN match_features f
                      ON f.match_id = m.match_id
                    WHERE m.competition = ?
                      AND m.match_id LIKE 'understat:%'
                      AND f.features_version = ?
                    """,
                    (league, args.features_version),
                ).fetchall()

            for row in rows:
                hg = row["home_goals"]
                ag = row["away_goals"]
                if hg is None or ag is None:
                    continue
                kickoff = datetime.fromisoformat(str(row["kickoff_utc"]).replace("Z", "+00:00"))
                features = json.loads(row["features_json"])
                lam_h = float(features.get("lambda_home", 0.0))
                lam_a = float(features.get("lambda_away", 0.0))
                if lam_h <= 0 or lam_a <= 0:
                    continue

                probs_dc = match_probs(lam_h, lam_a, cap=8, rho=rho)
                probs_gbm = predict_probs(gbm_model, features)
                if not all(k in probs_gbm for k in ("home_win", "draw", "away_win")):
                    continue

                outcome = "H" if hg > ag else ("D" if hg == ag else "A")
                cal_sel = select_calibration(league_cal, row["season"], kickoff) if league_cal else None
                records.append((probs_dc, probs_gbm, outcome, cal_sel, kickoff))

            if not records:
                errors[league] = "no_records"
                continue

            train = [(a, b, c, d) for a, b, c, d, k in records if k < split_dt]
            test = [(a, b, c, d) for a, b, c, d, k in records if k >= split_dt]
            if not train:
                errors[league] = "no_train_records"
                continue

            best_weight = None
            best_loss = float("inf")
            for w in weights:
                loss = _evaluate(train, w, not args.skip_calibration)
                if loss < best_loss:
                    best_loss = loss
                    best_weight = w

            test_loss = _evaluate(test, best_weight, not args.skip_calibration) if test else 0.0
            base_dc = _evaluate(test or train, 0.0, not args.skip_calibration)
            base_gbm = _evaluate(test or train, 1.0, not args.skip_calibration)

            results[league] = {
                "best_weight": best_weight,
                "train_logloss_1x2": best_loss,
                "test_logloss_1x2": test_loss,
                "baseline_dc_logloss_1x2": base_dc,
                "baseline_gbm_logloss_1x2": base_gbm,
                "n_train": len(train),
                "n_test": len(test),
            }
        except Exception as exc:
            errors[league] = str(exc)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "split_date": args.split_date,
        "features_version": args.features_version,
        "weights": weights,
        "by_league": results,
        "errors": errors,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote ensemble weights to {args.out}")


if __name__ == "__main__":
    main()

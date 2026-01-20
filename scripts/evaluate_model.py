from __future__ import annotations

import argparse
import json
import math
from typing import Dict, List, Tuple

from app.db.sqlite import get_conn
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


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


def _init_bucket() -> Dict[str, object]:
    return {
        "count": 0,
        "brier_records": {k: [] for k in [
            "home_win", "draw", "away_win", "over_2_5", "under_2_5", "btts_yes", "btts_no"
        ]},
        "logloss": [],
        "acc_hits": 0,
        "mae_home": 0.0,
        "mae_away": 0.0,
        "mae_total": 0.0,
    }


def _update_bucket(bucket: Dict[str, object], probs: Dict[str, float], hg: int, ag: int) -> None:
    bucket["count"] += 1

    bucket["brier_records"]["home_win"].append((probs["home_win"], 1 if hg > ag else 0))
    bucket["brier_records"]["draw"].append((probs["draw"], 1 if hg == ag else 0))
    bucket["brier_records"]["away_win"].append((probs["away_win"], 1 if hg < ag else 0))
    bucket["brier_records"]["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
    bucket["brier_records"]["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
    bucket["brier_records"]["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
    bucket["brier_records"]["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))

    outcome = "H" if hg > ag else ("D" if hg == ag else "A")
    bucket["logloss"].append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))

    pred = max((probs["home_win"], "H"), (probs["draw"], "D"), (probs["away_win"], "A"))[1]
    if pred == outcome:
        bucket["acc_hits"] += 1


def _finalize_bucket(name: str, bucket: Dict[str, object]) -> None:
    count = bucket["count"]
    brier = {k: _brier(v) for k, v in bucket["brier_records"].items()}
    logloss = sum(bucket["logloss"]) / len(bucket["logloss"]) if bucket["logloss"] else 0.0
    acc = bucket["acc_hits"] / count if count else 0.0
    mae_home = bucket["mae_home"] / count if count else 0.0
    mae_away = bucket["mae_away"] / count if count else 0.0
    mae_total = bucket["mae_total"] / count if count else 0.0

    print(f"[{name}] matches={count}")
    print("  Brier:", {k: round(v, 4) for k, v in brier.items()})
    print("  LogLoss_1X2:", round(logloss, 4))
    print("  Acc_1X2:", round(acc, 4))
    print("  MAE_goals:", {"home": round(mae_home, 4), "away": round(mae_away, 4), "total": round(mae_total, 4)})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2023,2024,2025")
    ap.add_argument("--features-version", default="understat_v4")
    ap.add_argument("--calibration", default="data/calibration/calibration_v1.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--no-calibration", action="store_true")
    ap.add_argument("--cap", type=int, default=8)
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    calibration = None
    if not args.no_calibration:
        calibration = load_calibration(args.calibration)
        calibration = select_league_calibration(calibration, args.league)
        if not calibration:
            print("WARN: calibration file not found or empty, proceeding without calibration.")

    rho = get_rho(args.dc_params, args.league)

    buckets: Dict[str, Dict[str, object]] = {"overall": _init_bucket()}
    for s in seasons:
        buckets[_season_label(s)] = _init_bucket()

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT match_id, season
            FROM matches
            WHERE competition = ?
            """,
            (args.league,),
        ).fetchall()

        for m in matches:
            season_label = m["season"]
            if season_label not in season_labels:
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

            probs = match_probs(lam_h, lam_a, cap=args.cap, rho=rho)
            if calibration:
                kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
                cal_sel = select_calibration(calibration, m["season"], kickoff)
                if cal_sel:
                    probs = apply_calibration(probs, cal_sel)

            hg = int(us["home_goals"])
            ag = int(us["away_goals"])

            for key in ("overall", season_label):
                bucket = buckets[key]
                _update_bucket(bucket, probs, hg, ag)
                bucket["mae_home"] += abs(lam_h - hg)
                bucket["mae_away"] += abs(lam_a - ag)
                bucket["mae_total"] += abs((lam_h + lam_a) - (hg + ag))

    print("Model evaluation (no odds required)")
    print(f"League: {args.league} | Seasons: {', '.join(sorted(season_labels))}")
    print(f"Calibration: {'on' if calibration else 'off'} | Features: {args.features_version} | dc_rho={rho}")
    _finalize_bucket("overall", buckets["overall"])
    for s in sorted(season_labels):
        _finalize_bucket(s, buckets[s])


if __name__ == "__main__":
    main()

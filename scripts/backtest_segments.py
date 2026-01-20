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
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _phase_for_date(dt: datetime) -> str:
    m = dt.month
    if m in (8, 9, 10):
        return "early"
    if m in (11, 12, 1, 2):
        return "mid"
    return "late"


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


def _init_bucket() -> dict:
    return {
        "count": 0,
        "logloss": [],
        "brier_home": [],
        "brier_draw": [],
        "brier_away": [],
    }


def _update(bucket: dict, probs: Dict[str, float], outcome: str):
    bucket["count"] += 1
    bucket["logloss"].append(_logloss_1x2(probs["home_win"], probs["draw"], probs["away_win"], outcome))
    bucket["brier_home"].append((probs["home_win"], 1 if outcome == "H" else 0))
    bucket["brier_draw"].append((probs["draw"], 1 if outcome == "D" else 0))
    bucket["brier_away"].append((probs["away_win"], 1 if outcome == "A" else 0))


def _finalize(bucket: dict) -> dict:
    if bucket["count"] == 0:
        return {"matches": 0}
    return {
        "matches": bucket["count"],
        "logloss_1x2": sum(bucket["logloss"]) / len(bucket["logloss"]),
        "brier_home": _brier(bucket["brier_home"]),
        "brier_draw": _brier(bucket["brier_draw"]),
        "brier_away": _brier(bucket["brier_away"]),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--calibration", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    ap.add_argument("--out", default="data/reports/segments_report.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    cal = load_calibration(args.calibration)
    cal = select_league_calibration(cal, args.league)
    rho = get_rho(args.dc_params, args.league)

    buckets = {
        "phase": {},
        "strength": {},
        "big_small": {},
        "rest_adv": {},
    }

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT m.match_id, m.season, m.kickoff_utc, f.features_json
            FROM matches m
            JOIN match_features f ON f.match_id = m.match_id
            WHERE m.competition = ?
              AND m.match_id LIKE 'understat:%'
              AND f.features_version = ?
            """,
            (args.league, args.features_version),
        ).fetchall()

        for m in matches:
            if m["season"] not in season_labels:
                continue
            match_id = m["match_id"]
            feat = json.loads(m["features_json"])
            lam_h = float(feat.get("lambda_home", 0.0))
            lam_a = float(feat.get("lambda_away", 0.0))
            if lam_h <= 0 or lam_a <= 0:
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

            probs = match_probs(lam_h, lam_a, cap=8, rho=rho)
            kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
            cal_sel = select_calibration(cal, m["season"], kickoff) if cal else None
            if cal_sel:
                probs = apply_calibration(probs, cal_sel)

            hg = int(us["home_goals"])
            ag = int(us["away_goals"])
            outcome = "H" if hg > ag else ("D" if hg == ag else "A")

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

            for key, val in (("phase", phase), ("strength", strength), ("big_small", big_small), ("rest_adv", rest_adv)):
                buckets[key].setdefault(val, _init_bucket())
                _update(buckets[key][val], probs, outcome)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "segments": {},
    }
    for seg, items in buckets.items():
        report["segments"][seg] = {k: _finalize(v) for k, v in items.items()}

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote segments report to {args.out}")


if __name__ == "__main__":
    main()

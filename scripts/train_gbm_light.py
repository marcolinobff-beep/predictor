from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.gbm_light import train_binary


FEATURES = [
    "lambda_home",
    "lambda_away",
    "home_xg_for_form",
    "home_xg_against_form",
    "away_xg_for_form",
    "away_xg_against_form",
    "home_xg_for_season",
    "home_xg_against_season",
    "away_xg_for_season",
    "away_xg_against_season",
    "league_avg_team_xg",
    "form_weight_home",
    "form_weight_away",
    "elo_home",
    "elo_away",
    "elo_diff",
    "rest_days_home",
    "rest_days_away",
    "matches_7d_home",
    "matches_7d_away",
    "matches_14d_home",
    "matches_14d_away",
    "schedule_factor_home",
    "schedule_factor_away",
    "overall_xg_for_form_home",
    "overall_xg_against_form_home",
    "overall_xg_for_form_away",
    "overall_xg_against_form_away",
    "overall_xg_for_season_home",
    "overall_xg_against_season_home",
    "overall_xg_for_season_away",
    "overall_xg_against_season_away",
    "finishing_delta_form_home",
    "finishing_delta_form_away",
    "defense_delta_form_home",
    "defense_delta_form_away",
    "form_attack_factor_home",
    "form_attack_factor_away",
    "form_defense_factor_home",
    "form_defense_factor_away",
    "xg_for_form_std_home",
    "xg_against_form_std_home",
    "xg_for_form_std_away",
    "xg_against_form_std_away",
]


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _load_rows(league: str, seasons: List[int], features_version: str):
    season_labels = {_season_label(s) for s in seasons}
    rows: List[Dict[str, float]] = []
    y_1x2: List[int] = []
    y_ou: List[int] = []
    y_btts: List[int] = []

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT m.match_id, m.season, u.home_goals, u.away_goals, f.features_json
            FROM matches m
            JOIN understat_matches u
              ON u.understat_match_id = replace(m.match_id, 'understat:', '')
            JOIN match_features f
              ON f.match_id = m.match_id
            WHERE m.competition = ?
              AND m.match_id LIKE 'understat:%'
              AND f.features_version = ?
            """,
            (league, features_version),
        ).fetchall()

    for r in matches:
        if r["season"] not in season_labels:
            continue
        hg = r["home_goals"]
        ag = r["away_goals"]
        if hg is None or ag is None:
            continue
        features = json.loads(r["features_json"])
        row = {f: float(features.get(f, 0.0) or 0.0) for f in FEATURES}
        rows.append(row)
        if hg > ag:
            y_1x2.append(0)
        elif hg == ag:
            y_1x2.append(1)
        else:
            y_1x2.append(2)
        y_ou.append(1 if (hg + ag) >= 3 else 0)
        y_btts.append(1 if (hg > 0 and ag > 0) else 0)

    return rows, y_1x2, y_ou, y_btts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2023,2024,2025")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--n-estimators", type=int, default=60)
    ap.add_argument("--learning-rate", type=float, default=0.1)
    ap.add_argument("--max-bins", type=int, default=8)
    ap.add_argument("--min-leaf", type=int, default=25)
    ap.add_argument("--out", default="data/models/gbm_light.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    rows, y_1x2, y_ou, y_btts = _load_rows(args.league, seasons, args.features_version)
    if not rows:
        raise SystemExit("No training rows found (check features_version and seasons).")

    y_home = [1 if y == 0 else 0 for y in y_1x2]
    y_draw = [1 if y == 1 else 0 for y in y_1x2]
    y_away = [1 if y == 2 else 0 for y in y_1x2]

    params = {
        "n_estimators": args.n_estimators,
        "learning_rate": args.learning_rate,
        "max_bins": args.max_bins,
        "min_leaf": args.min_leaf,
    }

    model_1x2 = {
        "home": train_binary(rows, y_home, FEATURES, **params),
        "draw": train_binary(rows, y_draw, FEATURES, **params),
        "away": train_binary(rows, y_away, FEATURES, **params),
    }
    model_ou = train_binary(rows, y_ou, FEATURES, **params)
    model_btts = train_binary(rows, y_btts, FEATURES, **params)

    out = {
        "version": "gbm_light_v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "features": FEATURES,
        "params": params,
        "models": {
            "1x2": model_1x2,
            "ou_2_5": model_ou,
            "btts": model_btts,
        },
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    existing: Dict[str, object] = {}
    if os.path.exists(args.out):
        try:
            with open(args.out, "r", encoding="utf-8") as f:
                existing = json.load(f) or {}
        except Exception:
            existing = {}

    by_league: Dict[str, object] = {}
    if isinstance(existing, dict) and "by_league" in existing:
        by_league = dict(existing.get("by_league") or {})
    elif isinstance(existing, dict) and existing.get("league") and existing.get("models"):
        by_league = {str(existing.get("league")): existing}

    by_league[str(args.league)] = out
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "by_league": by_league,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)

    print(f"OK: wrote GBM light model to {args.out} league={args.league}")


if __name__ == "__main__":
    main()

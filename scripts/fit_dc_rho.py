from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import List, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn
from app.core.probabilities import scoreline_prob


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def _collect_matches(conn, league: str, season_labels: set, features_version: str) -> Tuple[List[Tuple[float, float, int, int]], int]:
    rows = conn.execute(
        """
        SELECT match_id, season
        FROM matches
        WHERE competition = ?
        """,
        (league,),
    ).fetchall()

    samples: List[Tuple[float, float, int, int]] = []
    max_goal = 0

    for r in rows:
        if r["season"] not in season_labels:
            continue
        match_id = r["match_id"]
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
            (match_id, features_version),
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

        hg = int(us["home_goals"])
        ag = int(us["away_goals"])
        max_goal = max(max_goal, hg, ag)
        samples.append((lam_h, lam_a, hg, ag))

    return samples, max_goal


def _grid(start: float, end: float, step: float) -> List[float]:
    vals = []
    v = start
    while v <= end + 1e-9:
        vals.append(round(v, 4))
        v += step
    return vals


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2021,2022,2023")
    ap.add_argument("--features-version", default="understat_v4")
    ap.add_argument("--rho-min", type=float, default=-0.2)
    ap.add_argument("--rho-max", type=float, default=0.2)
    ap.add_argument("--rho-step", type=float, default=0.01)
    ap.add_argument("--out", default="data/calibration/dc_params.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    with get_conn() as conn:
        samples, max_goal = _collect_matches(conn, args.league, season_labels, args.features_version)

    if not samples:
        raise SystemExit("No samples found to fit rho.")

    cap = max(8, max_goal)
    best_rho = 0.0
    best_ll = -1e18
    eps = 1e-12

    for rho in _grid(args.rho_min, args.rho_max, args.rho_step):
        ll = 0.0
        for lam_h, lam_a, hg, ag in samples:
            p = scoreline_prob(lam_h, lam_a, hg, ag, cap=cap, rho=rho)
            ll += math.log(max(p, eps))
        if ll > best_ll:
            best_ll = ll
            best_rho = rho

    out = {
        "version": "dc_rho_v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "rho": best_rho,
        "log_likelihood": best_ll,
        "cap": cap,
        "samples": len(samples),
        "grid": {"min": args.rho_min, "max": args.rho_max, "step": args.rho_step},
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    existing: dict = {}
    if os.path.exists(args.out):
        try:
            with open(args.out, "r", encoding="utf-8") as f:
                existing = json.load(f) or {}
        except Exception:
            existing = {}

    by_league: dict = {}
    if isinstance(existing, dict) and "by_league" in existing:
        by_league = dict(existing.get("by_league") or {})
    elif isinstance(existing, dict) and existing.get("league") and existing.get("rho") is not None:
        by_league = {str(existing.get("league")): existing}

    by_league[str(args.league)] = out
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "by_league": by_league,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)

    print(f"OK: fitted rho={best_rho} log_likelihood={best_ll:.2f} samples={len(samples)}")


if __name__ == "__main__":
    main()

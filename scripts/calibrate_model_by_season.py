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


def _init_records() -> Dict[str, List[Tuple[float, int]]]:
    return {
        "home_win": [],
        "draw": [],
        "away_win": [],
        "over_2_5": [],
        "under_2_5": [],
        "btts_yes": [],
        "btts_no": [],
    }


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
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2023,2024,2025")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--bins", type=int, default=20)
    ap.add_argument("--min-count", type=int, default=30)
    ap.add_argument("--out", default="data/calibration/calibration_by_season.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    rho = get_rho(args.dc_params, args.league)

    all_records = _init_records()
    by_season: Dict[str, Dict[str, Dict[str, List[Tuple[float, int]]]]] = {}

    with get_conn() as conn:
        matches = conn.execute(
            """
            SELECT m.match_id, m.season, m.kickoff_utc
            FROM matches m
            WHERE m.competition = ?
              AND m.match_id LIKE 'understat:%'
            """,
            (args.league,),
        ).fetchall()

        for m in matches:
            if m["season"] not in season_labels:
                continue
            match_id = m["match_id"]
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
            hg = int(us["home_goals"])
            ag = int(us["away_goals"])

            kickoff = datetime.fromisoformat(str(m["kickoff_utc"]).replace("Z", "+00:00"))
            phase = _phase_for_date(kickoff)
            season_key = m["season"]

            by_season.setdefault(season_key, {})
            by_season[season_key].setdefault("full", _init_records())
            by_season[season_key].setdefault(phase, _init_records())

            targets = [
                (all_records, probs),
                (by_season[season_key]["full"], probs),
                (by_season[season_key][phase], probs),
            ]

            for rec, p in targets:
                rec["home_win"].append((p["home_win"], 1 if hg > ag else 0))
                rec["draw"].append((p["draw"], 1 if hg == ag else 0))
                rec["away_win"].append((p["away_win"], 1 if hg < ag else 0))
                rec["over_2_5"].append((p["over_2_5"], 1 if (hg + ag) >= 3 else 0))
                rec["under_2_5"].append((p["under_2_5"], 1 if (hg + ag) <= 2 else 0))
                rec["btts_yes"].append((p["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
                rec["btts_no"].append((p["btts_no"], 1 if (hg == 0 or ag == 0) else 0))

    bins = [(i / args.bins, (i + 1) / args.bins) for i in range(args.bins)]

    out = {
        "version": "calibration_v2",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "dc_rho": rho,
        "markets": _build_bins(all_records, bins, args.min_count),
        "default": {
            "markets": _build_bins(all_records, bins, args.min_count),
        },
        "by_season": {},
    }

    for season_key, segments in by_season.items():
        season_out = {}
        for seg, recs in segments.items():
            season_out[seg] = {"markets": _build_bins(recs, bins, args.min_count)}
        out["by_season"][season_key] = season_out

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    existing: Dict[str, Any] = {}
    if os.path.exists(args.out):
        try:
            with open(args.out, "r", encoding="utf-8") as f:
                existing = json.load(f) or {}
        except Exception:
            existing = {}

    by_league: Dict[str, Any] = {}
    if isinstance(existing, dict) and "by_league" in existing:
        by_league = dict(existing.get("by_league") or {})
    elif isinstance(existing, dict) and existing.get("league") and existing.get("by_season"):
        by_league = {str(existing.get("league")): existing}

    by_league[str(args.league)] = out
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "by_league": by_league,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote calibration to {args.out} league={args.league}")


if __name__ == "__main__":
    main()

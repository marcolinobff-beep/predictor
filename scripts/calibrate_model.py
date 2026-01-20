from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

from app.db.sqlite import get_conn
from app.core.dc_params import get_rho
from app.core.probabilities import match_probs


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--seasons", required=True, help="Comma separated season start years, es: 2023,2024,2025")
    ap.add_argument("--features-version", default="understat_v4")
    ap.add_argument("--bins", type=int, default=20)
    ap.add_argument("--min-count", type=int, default=30)
    ap.add_argument("--out", default="data/calibration/calibration_v1.json")
    ap.add_argument("--dc-params", default="data/calibration/dc_params.json")
    args = ap.parse_args()

    seasons = [int(s.strip()) for s in args.seasons.split(",") if s.strip()]
    season_labels = {_season_label(s) for s in seasons}

    records: Dict[str, List[Tuple[float, int]]] = {
        "home_win": [],
        "draw": [],
        "away_win": [],
        "over_2_5": [],
        "under_2_5": [],
        "btts_yes": [],
        "btts_no": [],
    }

    rho = get_rho(args.dc_params, args.league)

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
            hg = int(us["home_goals"])
            ag = int(us["away_goals"])

            records["home_win"].append((probs["home_win"], 1 if hg > ag else 0))
            records["draw"].append((probs["draw"], 1 if hg == ag else 0))
            records["away_win"].append((probs["away_win"], 1 if hg < ag else 0))
            records["over_2_5"].append((probs["over_2_5"], 1 if (hg + ag) >= 3 else 0))
            records["under_2_5"].append((probs["under_2_5"], 1 if (hg + ag) <= 2 else 0))
            records["btts_yes"].append((probs["btts_yes"], 1 if (hg > 0 and ag > 0) else 0))
            records["btts_no"].append((probs["btts_no"], 1 if (hg == 0 or ag == 0) else 0))

    bins = []
    for i in range(args.bins):
        bins.append((i / args.bins, (i + 1) / args.bins))

    out = {
        "version": "calibration_v1",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "league": args.league,
        "seasons": seasons,
        "features_version": args.features_version,
        "dc_rho": rho,
        "markets": {},
    }

    for key, data in records.items():
        market_bins = []
        if not data:
            out["markets"][key] = market_bins
            continue
        overall_rate = sum(o for _, o in data) / len(data)

        for lo, hi in bins:
            bin_items = [o for p, o in data if lo <= p < hi]
            if len(bin_items) < args.min_count:
                market_bins.append({"min": lo, "max": hi, "p": overall_rate, "count": len(bin_items)})
            else:
                p = sum(bin_items) / len(bin_items)
                market_bins.append({"min": lo, "max": hi, "p": p, "count": len(bin_items)})
        out["markets"][key] = market_bins

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote calibration to {args.out}")


if __name__ == "__main__":
    main()

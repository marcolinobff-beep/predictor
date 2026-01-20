from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


DEFAULT_LEAGUES = ["Serie_A", "EPL", "Bundesliga", "La_Liga", "Ligue_1"]


def _parse_list(value: Optional[str]) -> List[str]:
    if not value:
        return DEFAULT_LEAGUES
    return [v.strip() for v in value.split(",") if v.strip()]


def _pct(num: int, den: int) -> float:
    return (num / den * 100.0) if den else 0.0


def _iso_range(day: datetime, days_ahead: int) -> tuple[str, str]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=days_ahead)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


def _safe_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--days-ahead", type=int, default=7)
    ap.add_argument("--stale-hours", type=int, default=24)
    ap.add_argument("--check-lambda", action="store_true")
    ap.add_argument("--out", default="data/reports/data_quality.json")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    day_start_iso, day_end_iso = _iso_range(now, args.days_ahead)
    past_cutoff = (now - timedelta(days=1)).date()
    past_cutoff_iso, _ = _iso_range(datetime(past_cutoff.year, past_cutoff.month, past_cutoff.day, tzinfo=timezone.utc), 1)

    by_league: Dict[str, object] = {}
    with get_conn() as conn:
        for league in _parse_list(args.leagues):
            latest_result = conn.execute(
                """
                SELECT MAX(datetime_utc) AS max_dt
                FROM understat_matches
                WHERE league = ?
                  AND home_goals IS NOT NULL
                  AND away_goals IS NOT NULL
                """,
                (league,),
            ).fetchone()
            latest_result_dt = _safe_dt(latest_result["max_dt"]) if latest_result else None
            if latest_result_dt:
                latest_result_iso = latest_result_dt.isoformat().replace("+00:00", "Z")
                result_cutoff_iso = min(past_cutoff_iso, latest_result_iso)
            else:
                latest_result_iso = None
                result_cutoff_iso = past_cutoff_iso

            total = conn.execute(
                "SELECT COUNT(*) AS c FROM matches WHERE competition = ? AND match_id LIKE 'understat:%'",
                (league,),
            ).fetchone()["c"]

            with_features = conn.execute(
                """
                SELECT COUNT(DISTINCT m.match_id) AS c
                FROM matches m
                JOIN match_features f
                  ON f.match_id = m.match_id AND f.features_version = ?
                WHERE m.competition = ? AND m.match_id LIKE 'understat:%'
                """,
                (args.features_version, league),
            ).fetchone()["c"]

            with_tactical = conn.execute(
                """
                SELECT COUNT(DISTINCT m.match_id) AS c
                FROM matches m
                JOIN tactical_stats t ON t.match_id = m.match_id
                WHERE m.competition = ? AND m.match_id LIKE 'understat:%'
                """,
                (league,),
            ).fetchone()["c"]

            with_lineups = conn.execute(
                """
                SELECT COUNT(DISTINCT m.match_id) AS c
                FROM matches m
                JOIN probable_lineups p ON p.match_id = m.match_id
                WHERE m.competition = ? AND m.match_id LIKE 'understat:%'
                """,
                (league,),
            ).fetchone()["c"]

            upcoming = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM matches
                WHERE competition = ?
                  AND kickoff_utc >= ? AND kickoff_utc < ?
                """,
                (league, day_start_iso, day_end_iso),
            ).fetchone()["c"]

            upcoming_with_lineups = conn.execute(
                """
                SELECT COUNT(DISTINCT m.match_id) AS c
                FROM matches m
                JOIN probable_lineups p ON p.match_id = m.match_id
                WHERE m.competition = ?
                  AND m.kickoff_utc >= ? AND m.kickoff_utc < ?
                """,
                (league, day_start_iso, day_end_iso),
            ).fetchone()["c"]

            rows = conn.execute(
                """
                SELECT m.match_id, MAX(p.fetched_at_utc) AS last_ts
                FROM matches m
                LEFT JOIN probable_lineups p ON p.match_id = m.match_id
                WHERE m.competition = ?
                  AND m.kickoff_utc >= ? AND m.kickoff_utc < ?
                GROUP BY m.match_id
                """,
                (league, day_start_iso, day_end_iso),
            ).fetchall()

            stale_lineups = 0
            for r in rows:
                last_ts = _safe_dt(r["last_ts"])
                if not last_ts:
                    stale_lineups += 1
                    continue
                age_hours = (now - last_ts).total_seconds() / 3600.0
                if age_hours >= args.stale_hours:
                    stale_lineups += 1

            missing_results = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM matches m
                JOIN understat_matches u
                  ON u.understat_match_id = replace(m.match_id, 'understat:', '')
                WHERE m.competition = ?
                  AND m.kickoff_utc < ?
                  AND (u.home_goals IS NULL OR u.away_goals IS NULL)
                """,
                (league, result_cutoff_iso),
            ).fetchone()["c"]

            bad_lambda = 0
            if args.check_lambda:
                rows = conn.execute(
                    """
                    SELECT f.features_json
                    FROM match_features f
                    JOIN matches m ON m.match_id = f.match_id
                    WHERE m.competition = ?
                      AND f.features_version = ?
                    """,
                    (league, args.features_version),
                ).fetchall()
                for row in rows:
                    try:
                        feats = json.loads(row["features_json"])
                    except Exception:
                        bad_lambda += 1
                        continue
                    lam_h = float(feats.get("lambda_home", 0.0))
                    lam_a = float(feats.get("lambda_away", 0.0))
                    if lam_h <= 0 or lam_a <= 0:
                        bad_lambda += 1

            by_league[league] = {
                "matches_total": total,
                "features": {
                    "count": with_features,
                    "pct": round(_pct(with_features, total), 2),
                },
                "tactical": {
                    "count": with_tactical,
                    "pct": round(_pct(with_tactical, total), 2),
                },
                "lineups": {
                    "count": with_lineups,
                    "pct": round(_pct(with_lineups, total), 2),
                },
                "upcoming": {
                    "count": upcoming,
                    "with_lineups": upcoming_with_lineups,
                    "stale_or_missing_lineups": stale_lineups,
                },
                "missing_results_past": missing_results,
                "latest_result_utc": latest_result_iso,
                "bad_lambda_count": bad_lambda if args.check_lambda else None,
            }

    payload = {
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "features_version": args.features_version,
        "days_ahead": args.days_ahead,
        "stale_hours": args.stale_hours,
        "by_league": by_league,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote data quality report to {args.out}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


DEFAULT_LEAGUES = ["Serie_A", "EPL", "Bundesliga", "La_Liga", "Ligue_1"]


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return DEFAULT_LEAGUES
    return [v.strip() for v in value.split(",") if v.strip()]


def _run_oos(league: str, split_date: str, features_version: str) -> dict:
    cmd = [
        sys.executable,
        "scripts/evaluate_model_oos.py",
        "--league",
        league,
        "--split-date",
        split_date,
        "--features-version",
        features_version,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or "evaluate_model_oos failed")
    return json.loads(res.stdout)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--split-date", default="2024-07-01")
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--out", default="data/reports/oos_backtest.json")
    args = ap.parse_args()

    results = {}
    errors = {}
    for league in _parse_list(args.leagues):
        try:
            results[league] = _run_oos(league, args.split_date, args.features_version)
        except Exception as exc:
            errors[league] = str(exc)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "split_date": args.split_date,
        "features_version": args.features_version,
        "by_league": results,
        "errors": errors,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)

    print(f"OK: wrote OOS backtest to {args.out}")


if __name__ == "__main__":
    main()

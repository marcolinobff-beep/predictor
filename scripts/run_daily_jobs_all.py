from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone, date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


LEAGUE_CONFIG = {
    "Serie_A": {"division": "I1"},
    "EPL": {"division": "E0"},
    "Bundesliga": {"division": "D1"},
    "La_Liga": {"division": "SP1"},
    "Ligue_1": {"division": "F1"},
}


def _parse_date(value: str | None) -> date:
    if not value:
        return datetime.now(timezone.utc).date()
    return date.fromisoformat(value)


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return list(LEAGUE_CONFIG.keys())
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--skip-odds", action="store_true")
    ap.add_argument("--closing", action="store_true")
    ap.add_argument("--run-kpi", action="store_true")
    ap.add_argument("--max-seasons", type=int, default=5)
    ap.add_argument("--oddsapi", action="store_true")
    ap.add_argument("--oddsapi-markets", default="h2h,totals")
    ap.add_argument("--oddsapi-regions", default="eu")
    ap.add_argument("--oddsapi-bookmaker", default=None)
    ap.add_argument("--oddsapi-sport-key", default=None)
    ap.add_argument("--run-model-update", action="store_true")
    ap.add_argument("--model-max-seasons", type=int, default=2)
    ap.add_argument("--features-version", default="understat_v5")
    ap.add_argument("--skip-gbm", action="store_true")
    ap.add_argument("--skip-calibration", action="store_true")
    ap.add_argument("--skip-rho", action="store_true")
    ap.add_argument("--skip-tactical", action="store_true")
    ap.add_argument("--tactical-days-back", type=int, default=2)
    ap.add_argument("--tactical-days-ahead", type=int, default=0)
    ap.add_argument("--tactical-sleep", type=float, default=0.7)
    ap.add_argument("--tactical-timeout", type=int, default=25)
    ap.add_argument("--tactical-update-existing", action="store_true")
    ap.add_argument("--tactical-max-matches", type=int, default=0)
    ap.add_argument("--run-oos", action="store_true")
    ap.add_argument("--oos-split-date", default="2024-07-01")
    ap.add_argument("--oos-out", default="data/reports/oos_backtest.json")
    ap.add_argument("--run-temp-scale", action="store_true")
    ap.add_argument("--run-calibration-policy", action="store_true")
    ap.add_argument("--run-quality", action="store_true")
    ap.add_argument("--quality-out", default="data/reports/data_quality.json")
    ap.add_argument("--quality-days-ahead", type=int, default=7)
    ap.add_argument("--quality-stale-hours", type=int, default=24)
    args = ap.parse_args()

    day = _parse_date(args.date)
    leagues = _parse_list(args.leagues)

    for league in leagues:
        cfg = LEAGUE_CONFIG.get(league)
        if not cfg:
            print(f"WARN: unknown league {league}, skipping.")
            continue
        if args.run_model_update:
            model_cmd = [
                sys.executable,
                "scripts/run_daily_model_update.py",
                "--league",
                league,
                "--max-seasons",
                str(args.model_max_seasons),
                "--features-version",
                args.features_version,
            ]
            if args.skip_gbm:
                model_cmd.append("--skip-gbm")
            if args.skip_calibration:
                model_cmd.append("--skip-calibration")
            if args.skip_rho:
                model_cmd.append("--skip-rho")
            subprocess.run(model_cmd, check=True)
        cmd = [
            sys.executable,
            "scripts/run_daily_jobs.py",
            "--league",
            league,
            "--date",
            day.isoformat(),
            "--division",
            cfg["division"],
            "--max-seasons",
            str(args.max_seasons),
        ]
        if args.skip_odds:
            cmd.append("--skip-odds")
        if args.closing:
            cmd.append("--closing")
        if args.oddsapi:
            cmd.append("--oddsapi")
            cmd.extend(["--oddsapi-markets", args.oddsapi_markets])
            cmd.extend(["--oddsapi-regions", args.oddsapi_regions])
            if args.oddsapi_bookmaker:
                cmd.extend(["--oddsapi-bookmaker", args.oddsapi_bookmaker])
            if args.oddsapi_sport_key:
                cmd.extend(["--oddsapi-sport-key", args.oddsapi_sport_key])
        if args.run_kpi:
            cmd.append("--run-kpi")
        subprocess.run(cmd, check=True)

    if not args.skip_tactical:
        tac_cmd = [
            sys.executable,
            "scripts/run_daily_tactical_refresh.py",
            "--leagues",
            ",".join(leagues),
            "--days-back",
            str(args.tactical_days_back),
            "--days-ahead",
            str(args.tactical_days_ahead),
            "--sleep",
            str(args.tactical_sleep),
            "--timeout",
            str(args.tactical_timeout),
        ]
        if args.tactical_update_existing:
            tac_cmd.append("--update-existing")
        if args.tactical_max_matches:
            tac_cmd.extend(["--max-matches", str(args.tactical_max_matches)])
        subprocess.run(tac_cmd, check=True)

    if args.run_oos:
        oos_cmd = [
            sys.executable,
            "scripts/run_oos_backtest.py",
            "--split-date",
            args.oos_split_date,
            "--features-version",
            args.features_version,
            "--out",
            args.oos_out,
        ]
        subprocess.run(oos_cmd, check=True)
        if args.run_temp_scale:
            subprocess.run([
                sys.executable,
                "scripts/build_temp_scale.py",
                "--oos",
                args.oos_out,
            ], check=True)
        if args.run_calibration_policy:
            subprocess.run([
                sys.executable,
                "scripts/build_calibration_policy.py",
                "--oos",
                args.oos_out,
            ], check=True)

    if args.run_quality:
        quality_cmd = [
            sys.executable,
            "scripts/data_quality_report.py",
            "--leagues",
            ",".join(leagues),
            "--features-version",
            args.features_version,
            "--days-ahead",
            str(args.quality_days_ahead),
            "--stale-hours",
            str(args.quality_stale_hours),
            "--out",
            args.quality_out,
        ]
        subprocess.run(quality_cmd, check=True)

    print("OK: daily jobs completed for leagues:", ", ".join(leagues))


if __name__ == "__main__":
    main()

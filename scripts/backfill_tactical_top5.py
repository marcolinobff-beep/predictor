from __future__ import annotations

import argparse
import subprocess
import sys


DEFAULT_LEAGUES = ["Serie_A", "EPL", "Bundesliga", "La_Liga", "Ligue_1"]


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return DEFAULT_LEAGUES
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--seasons", default=None, help="Comma separated season start years")
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--max-matches", type=int, default=0)
    ap.add_argument("--days-back", type=int, default=None)
    ap.add_argument("--days-ahead", type=int, default=0)
    ap.add_argument("--update-existing", action="store_true")
    args = ap.parse_args()

    leagues = _parse_list(args.leagues)
    for league in leagues:
        cmd = [
            sys.executable,
            "scripts/ingest_tactical_understat.py",
            "--league",
            league,
            "--sleep",
            str(args.sleep),
            "--timeout",
            str(args.timeout),
        ]
        if args.days_back is not None:
            cmd.extend(["--days-back", str(args.days_back)])
        if args.days_ahead is not None:
            cmd.extend(["--days-ahead", str(args.days_ahead)])
        if args.seasons:
            cmd.extend(["--seasons", args.seasons])
        else:
            cmd.append("--all-seasons")
        if args.max_matches:
            cmd.extend(["--max-matches", str(args.max_matches)])
        if args.update_existing:
            cmd.append("--update-existing")
        subprocess.run(cmd, check=True)

    print("OK: tactical backfill completed for leagues:", ", ".join(leagues))


if __name__ == "__main__":
    main()

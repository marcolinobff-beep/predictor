from __future__ import annotations

import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


DEFAULT_LEAGUES = ["Serie_A", "EPL", "Bundesliga", "La_Liga", "Ligue_1"]


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return DEFAULT_LEAGUES
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leagues", default=None, help="Comma separated (default top 5)")
    ap.add_argument("--days-back", type=int, default=2)
    ap.add_argument("--days-ahead", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--update-existing", action="store_true")
    ap.add_argument("--max-matches", type=int, default=0)
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
            "--days-back",
            str(args.days_back),
            "--days-ahead",
            str(args.days_ahead),
        ]
        if args.update_existing:
            cmd.append("--update-existing")
        if args.max_matches:
            cmd.extend(["--max-matches", str(args.max_matches)])
        subprocess.run(cmd, check=True)

    print("OK: daily tactical refresh completed.")


if __name__ == "__main__":
    main()

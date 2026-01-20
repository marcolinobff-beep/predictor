import argparse
import json
import os
from uuid import uuid4

import requests


def _default_headers(league: str, season: int) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://understat.com/league/{league}/{season}",
        "X-Requested-With": "XMLHttpRequest",
    }


def fetch_league_data(league: str, season: int, timeout: int = 20) -> dict:
    url = f"https://understat.com/getLeagueData/{league}/{season}"
    resp = requests.get(url, headers=_default_headers(league, season), timeout=timeout)
    resp.raise_for_status()
    return json.loads(resp.text)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, help='Understat league name, es: "Serie_A"')
    ap.add_argument("--season", required=True, type=int, help="Season start year, es 2024 per 2024/25")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--out-dir", default=None, help="Override output cache dir")
    args = ap.parse_args()

    data = fetch_league_data(args.league, args.season, timeout=args.timeout)
    dates = data.get("dates") or []
    teams_raw = data.get("teams") or {}
    players = data.get("players") or []
    teams = list(teams_raw.values()) if isinstance(teams_raw, dict) else teams_raw

    run_id = str(uuid4())
    cache_base = args.out_dir
    if not cache_base:
        cache_base = os.path.join("data", "cache", "understat", args.league, str(args.season), run_id)
    ensure_dir(cache_base)

    with open(os.path.join(cache_base, "league_results.json"), "w", encoding="utf-8") as f:
        json.dump(dates, f, ensure_ascii=False)
    with open(os.path.join(cache_base, "teams.json"), "w", encoding="utf-8") as f:
        json.dump(teams, f, ensure_ascii=False)
    with open(os.path.join(cache_base, "players.json"), "w", encoding="utf-8") as f:
        json.dump(players, f, ensure_ascii=False)

    print(
        "OK: cached understat league data",
        f"league={args.league}",
        f"season={args.season}",
        f"matches={len(dates)}",
        f"teams={len(teams)}",
        f"players={len(players)}",
        f"cache_dir={cache_base}",
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, date, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.core.ids import stable_hash
from app.db.sqlite import get_conn


LIST_URL_DEFAULT = "https://www.gazzetta.it/Calcio/prob_form/"
API_BASE = "https://api-matches-lineups.gazzetta.it/api/lineups/"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower().replace("_", " ")).strip()


def _load_aliases(path: Optional[str]) -> Dict[str, List[str]]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise SystemExit("Aliases file must be a dict: team -> [aliases].")
    return {k: v for k, v in data.items() if isinstance(v, list)}


def _team_alias_map(conn, aliases: Dict[str, List[str]]) -> Dict[str, str]:
    rows = conn.execute(
        """
        SELECT DISTINCT home AS team FROM matches
        UNION
        SELECT DISTINCT away AS team FROM matches
        """
    ).fetchall()
    teams = [r["team"] for r in rows if r and r["team"]]

    alias_map: Dict[str, str] = {}
    for team in teams:
        alias_map[_normalize_text(team)] = team
        for a in aliases.get(team, []):
            alias_map[_normalize_text(a)] = team
    return alias_map


def _resolve_team(value: str, alias_map: Dict[str, str]) -> str:
    return alias_map.get(_normalize_text(value), value)


def _parse_kickoff_date(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.date()
    except ValueError:
        return None


def _load_matches(conn, competition: str, day_filter: Optional[date]) -> List[dict]:
    sql = """
        SELECT match_id, kickoff_utc, home, away
        FROM matches
        WHERE competition = ?
    """
    params: List[object] = [competition]
    if day_filter:
        start = datetime(day_filter.year, day_filter.month, day_filter.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        sql += " AND kickoff_utc >= ? AND kickoff_utc < ?"
        params.extend([
            start.isoformat().replace("+00:00", "Z"),
            end.isoformat().replace("+00:00", "Z"),
        ])
    rows = conn.execute(sql, params).fetchall()
    out = []
    for r in rows:
        out.append({
            "match_id": r["match_id"],
            "day": _parse_kickoff_date(r["kickoff_utc"]),
            "home": r["home"],
            "away": r["away"],
        })
    return out


def _build_match_index(rows: List[dict], alias_map: Dict[str, str]):
    by_key: Dict[Tuple[Optional[date], str, str], str] = {}
    by_pair: Dict[Tuple[str, str], List[Tuple[Optional[date], str]]] = {}
    for row in rows:
        day = row["day"]
        home = _normalize_text(_resolve_team(row["home"], alias_map))
        away = _normalize_text(_resolve_team(row["away"], alias_map))
        by_key[(day, home, away)] = row["match_id"]
        by_pair.setdefault((home, away), []).append((day, row["match_id"]))
    return by_key, by_pair


def _find_match_id(
    by_key: Dict[Tuple[Optional[date], str, str], str],
    by_pair: Dict[Tuple[str, str], List[Tuple[Optional[date], str]]],
    alias_map: Dict[str, str],
    home: str,
    away: str,
    day: Optional[date],
) -> Optional[str]:
    home_norm = _normalize_text(_resolve_team(home, alias_map))
    away_norm = _normalize_text(_resolve_team(away, alias_map))
    if day is not None:
        match_id = by_key.get((day, home_norm, away_norm))
        if match_id:
            return match_id
    candidates = by_pair.get((home_norm, away_norm), [])
    if not candidates:
        return None
    if day is None:
        return candidates[0][1]
    candidates_sorted = sorted(
        candidates,
        key=lambda x: abs((x[0] - day).days) if x[0] else 999,
    )
    return candidates_sorted[0][1]


def _fetch_match_links(list_url: str) -> List[str]:
    resp = requests.get(list_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    links = re.findall(r"/Calcio/prob_form/[^\"']+/\\d+", resp.text)
    seen = set()
    out: List[str] = []
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        out.append(f"https://www.gazzetta.it{link}")
    return out


def _extract_match_id_from_link(link: str) -> Optional[str]:
    parts = [p for p in link.split("/") if p]
    if not parts:
        return None
    return parts[-1]


def _fetch_lineups(match_id: str) -> dict:
    url = f"{API_BASE}{match_id}?patchV=true"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    return resp.json()


def _dedupe(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _parse_absence_blob(value: Optional[str]) -> List[str]:
    if not value:
        return []
    cleaned = value.strip()
    if not cleaned or cleaned.lower() in ("nessuno", "-", "n/a"):
        return []
    return [p.strip() for p in cleaned.split(",") if p.strip()]


def _parse_absences(team: dict) -> List[str]:
    parts: List[str] = []
    parts.extend(_parse_absence_blob(team.get("disqualified")))
    parts.extend(_parse_absence_blob(team.get("unavailable")))
    parts.extend(_parse_absence_blob(team.get("banned")))
    parts.extend(_parse_absence_blob(team.get("others")))
    return _dedupe(parts)


def _parse_players(team: dict) -> List[str]:
    players = team.get("players") or []
    def _place(p: dict) -> int:
        try:
            return int(p.get("formationPlace") or 99)
        except (TypeError, ValueError):
            return 99
    players_sorted = sorted(players, key=_place)
    names = [p.get("name") for p in players_sorted if p.get("name")]
    return names


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--competition", default="Serie_A")
    ap.add_argument("--list-url", default=LIST_URL_DEFAULT)
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--aliases", default="news_team_aliases.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    day_filter = date.fromisoformat(args.date) if args.date else None

    links = _fetch_match_links(args.list_url)
    if not links:
        raise SystemExit("No match links found on Gazzetta list page.")

    inserted = 0
    skipped = 0

    with get_conn() as conn:
        alias_map = _team_alias_map(conn, _load_aliases(args.aliases))
        match_rows = _load_matches(conn, args.competition, day_filter)
        by_key, by_pair = _build_match_index(match_rows, alias_map)

        for link in links:
            gazzetta_match_id = _extract_match_id_from_link(link)
            if not gazzetta_match_id:
                skipped += 1
                continue

            try:
                data = _fetch_lineups(gazzetta_match_id)
            except Exception:
                skipped += 1
                continue

            home_team = data.get("homeTeam", {})
            away_team = data.get("awayTeam", {})
            home_name = home_team.get("teamName")
            away_name = away_team.get("teamName")
            if not home_name or not away_name:
                skipped += 1
                continue

            match_day = None
            if data.get("date"):
                try:
                    match_day = date.fromisoformat(str(data["date"]))
                except ValueError:
                    match_day = None
            if day_filter and match_day and match_day != day_filter:
                skipped += 1
                continue

            match_id = _find_match_id(
                by_key,
                by_pair,
                alias_map,
                home_name,
                away_name,
                match_day or day_filter,
            )
            if not match_id:
                skipped += 1
                continue

            home_players = _parse_players(home_team)
            away_players = _parse_players(away_team)
            if len(home_players) < 9 or len(away_players) < 9:
                skipped += 1
                continue

            home_absences = _parse_absences(home_team)
            away_absences = _parse_absences(away_team)

            lineup_id = stable_hash({
                "source": "Gazzetta.it",
                "match_id": match_id,
                "gazzetta_match_id": gazzetta_match_id,
                "home_players": home_players,
                "away_players": away_players,
            })

            if args.dry_run:
                inserted += 1
                continue

            api_url = f"{API_BASE}{gazzetta_match_id}?patchV=true"
            conn.execute(
                """
                INSERT OR REPLACE INTO probable_lineups
                  (lineup_id, match_id, source, fetched_at_utc, confidence,
                   home_players_json, away_players_json,
                   home_absences_json, away_absences_json,
                   notes, raw_ref)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lineup_id,
                    match_id,
                    "Gazzetta.it",
                    _now_utc().isoformat().replace("+00:00", "Z"),
                    0.85,
                    json.dumps(home_players, ensure_ascii=True),
                    json.dumps(away_players, ensure_ascii=True),
                    json.dumps(home_absences, ensure_ascii=True),
                    json.dumps(away_absences, ensure_ascii=True),
                    "gazzetta_lineups_api",
                    api_url,
                ),
            )
            inserted += 1

        if not args.dry_run:
            conn.commit()

    print(f"OK: inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()

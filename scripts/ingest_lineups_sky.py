from __future__ import annotations

import argparse
import html
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


SKY_URL_DEFAULT = "https://sport.sky.it/calcio/serie-a/probabili-formazioni"


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


def _fetch_model(url: str) -> dict:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    m = re.search(r"model='({.*?})'", resp.text, re.DOTALL)
    if not m:
        raise RuntimeError("Sky model JSON not found in page.")
    blob = html.unescape(m.group(1))
    return json.loads(blob)


def _dedupe(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _parse_player_name(player: dict) -> Optional[str]:
    for key in ("fullName", "fullname", "name"):
        val = player.get(key)
        if val:
            return str(val)
    name = " ".join([p for p in [player.get("name"), player.get("surname")] if p])
    return name or None


def _parse_absences_list(items: List[dict]) -> List[str]:
    parts: List[str] = []
    for item in items or []:
        fullname = item.get("fullname") or item.get("fullName") or item.get("name")
        if fullname:
            parts.extend([p.strip() for p in str(fullname).split(",") if p.strip()])
    return _dedupe(parts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--competition", default="Serie_A")
    ap.add_argument("--url", default=SKY_URL_DEFAULT)
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--aliases", default="news_team_aliases.json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    day_filter = date.fromisoformat(args.date) if args.date else None
    model = _fetch_model(args.url)
    matches = model.get("matchList", [])

    inserted = 0
    skipped = 0

    with get_conn() as conn:
        alias_map = _team_alias_map(conn, _load_aliases(args.aliases))
        match_rows = _load_matches(conn, args.competition, day_filter)
        by_key, by_pair = _build_match_index(match_rows, alias_map)

        for m in matches:
            match_date = _parse_kickoff_date(m.get("date"))
            if day_filter and match_date and match_date != day_filter:
                skipped += 1
                continue
            home = m.get("home", {})
            away = m.get("away", {})
            home_name = home.get("name")
            away_name = away.get("name")
            if not home_name or not away_name:
                skipped += 1
                continue

            match_id = _find_match_id(
                by_key,
                by_pair,
                alias_map,
                home_name,
                away_name,
                match_date or day_filter,
            )
            if not match_id:
                skipped += 1
                continue

            home_list = home.get("playerList", {}) or {}
            away_list = away.get("playerList", {}) or {}
            home_players = [_parse_player_name(p) for p in home_list.get("startingLineup", [])]
            away_players = [_parse_player_name(p) for p in away_list.get("startingLineup", [])]
            home_players = [p for p in home_players if p]
            away_players = [p for p in away_players if p]
            if len(home_players) < 9 or len(away_players) < 9:
                skipped += 1
                continue

            home_absences = _parse_absences_list(home_list.get("unavailables", []))
            home_absences.extend(_parse_absences_list(home_list.get("disqualifieds", [])))
            away_absences = _parse_absences_list(away_list.get("unavailables", []))
            away_absences.extend(_parse_absences_list(away_list.get("disqualifieds", [])))

            lineup_id = stable_hash({
                "source": "Sky Sport",
                "match_id": match_id,
                "home_players": home_players,
                "away_players": away_players,
            })

            if args.dry_run:
                inserted += 1
                continue

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
                    "Sky Sport",
                    _now_utc().isoformat().replace("+00:00", "Z"),
                    0.82,
                    json.dumps(home_players, ensure_ascii=True),
                    json.dumps(away_players, ensure_ascii=True),
                    json.dumps(_dedupe(home_absences), ensure_ascii=True),
                    json.dumps(_dedupe(away_absences), ensure_ascii=True),
                    "sky_predicted_lineups",
                    args.url,
                ),
            )
            inserted += 1

        if not args.dry_run:
            conn.commit()

    print(f"OK: inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    main()

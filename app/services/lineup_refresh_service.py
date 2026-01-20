from __future__ import annotations

import html
import json
import re
from datetime import datetime, date, timezone, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests

from app.core.config import settings
from app.core.text_utils import clean_person_name
from app.core.ids import stable_hash
from app.db.sqlite import get_conn


SKY_URL = "https://sport.sky.it/calcio/serie-a/probabili-formazioni"
GAZZETTA_LIST_URL = "https://www.gazzetta.it/Calcio/prob_form/"
GAZZETTA_API_BASE = "https://api-matches-lineups.gazzetta.it/api/lineups/"
DEFAULT_DIRETTA_LEAGUE_URLS = {
    "Serie_A": "https://www.diretta.it/calcio/italia/serie-a/",
    "EPL": "https://www.diretta.it/calcio/inghilterra/premier-league/",
    "Bundesliga": "https://www.diretta.it/calcio/germania/bundesliga/",
    "La_Liga": "https://www.diretta.it/calcio/spagna/laliga/",
    "Ligue_1": "https://www.diretta.it/calcio/francia/ligue-1/",
}
FOOTBALL_DATA_COMP_CODES = {
    "Serie_A": "SA",
    "EPL": "PL",
    "Bundesliga": "BL1",
    "La_Liga": "PD",
    "Ligue_1": "FL1",
}
API_FOOTBALL_LEAGUE_IDS = {
    "Serie_A": 135,
    "EPL": 39,
    "Bundesliga": 78,
    "La_Liga": 140,
    "Ligue_1": 61,
}

ROOT = Path(__file__).resolve().parents[2]
ALIASES_DEFAULT = str(ROOT / "news_team_aliases.json")

FIELD_SEP = chr(172)
KV_SEP = chr(247)

TEAM_STOP_TOKENS = {
    "fc", "cf", "sc", "ac", "afc", "ssc",
    "club", "calcio", "football", "futbol", "cd",
}

_LAST_REFRESH: Dict[str, datetime] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", value.lower().replace("_", " ")).strip()

def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _load_aliases(path: str) -> Dict[str, List[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if isinstance(v, list)}


def _alias_variants(name: str) -> List[str]:
    cleaned = _normalize_text(name)
    if not cleaned:
        return []
    parts = cleaned.split()
    drop = {"fc", "cf", "sc", "ac", "afc", "ssc"}
    variants = set()
    if parts and parts[-1] in drop:
        variants.add(" ".join(parts[:-1]))
    if parts and parts[0] in drop:
        variants.add(" ".join(parts[1:]))
    if len(parts) > 2 and parts[0] in drop and parts[-1] in drop:
        variants.add(" ".join(parts[1:-1]))
    return [v for v in variants if v]


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
        for variant in _alias_variants(team):
            alias_map[_normalize_text(variant)] = team
        for a in aliases.get(team, []):
            alias_map[_normalize_text(a)] = team
            for variant in _alias_variants(a):
                alias_map[_normalize_text(variant)] = team
    return alias_map


def _resolve_team(value: str, alias_map: Dict[str, str]) -> str:
    return alias_map.get(_normalize_text(value), value)


def _team_key(value: str, alias_map: Dict[str, str]) -> str:
    resolved = _resolve_team(value, alias_map)
    tokens = _normalize_text(resolved).split()
    if not tokens:
        return ""
    cleaned = [t for t in tokens if t not in TEAM_STOP_TOKENS]
    if not cleaned:
        cleaned = tokens
    return " ".join(cleaned)


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
        home = _team_key(row["home"], alias_map)
        away = _team_key(row["away"], alias_map)
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
    home_norm = _team_key(home, alias_map)
    away_norm = _team_key(away, alias_map)
    if day is not None:
        match_id = by_key.get((day, home_norm, away_norm))
        if match_id:
            return match_id
    candidates = by_pair.get((home_norm, away_norm), [])
    if not candidates:
        best_key = None
        best_score = 0.0
        for key in by_pair.keys():
            score = _similarity(home_norm, key[0]) + _similarity(away_norm, key[1])
            if score > best_score:
                best_score = score
                best_key = key
        if best_key and best_score >= 1.72:
            candidates = by_pair.get(best_key, [])
    if not candidates:
        return None
    if day is None:
        return candidates[0][1]
    candidates_sorted = sorted(
        candidates,
        key=lambda x: abs((x[0] - day).days) if x[0] else 999,
    )
    return candidates_sorted[0][1]


def _dedupe(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _clean_list(values: List[str]) -> List[str]:
    out: List[str] = []
    for v in values:
        name = clean_person_name(v)
        if not name:
            continue
        out.append(name)
    return out


def _parse_absence_blob(value: Optional[str]) -> List[str]:
    if not value:
        return []
    cleaned = value.strip()
    if not cleaned or cleaned.lower() in ("nessuno", "-", "n/a"):
        return []
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    return _clean_list(parts)


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
    return _clean_list(names)


def _parse_player_name(player: dict) -> Optional[str]:
    for key in ("fullName", "fullname", "name"):
        val = player.get(key)
        if val:
            return clean_person_name(val)
    name = " ".join([p for p in [player.get("name"), player.get("surname")] if p])
    return clean_person_name(name) if name else None


def _parse_absences_list(items: List[dict]) -> List[str]:
    parts: List[str] = []
    for item in items or []:
        fullname = item.get("fullname") or item.get("fullName") or item.get("name")
        if fullname:
            parts.extend([p.strip() for p in str(fullname).split(",") if p.strip()])
    return _dedupe(_clean_list(parts))


def _football_data_headers() -> Dict[str, str]:
    api_key = settings.football_data_api_key
    if not api_key:
        return {}
    return {"X-Auth-Token": api_key, "User-Agent": "Mozilla/5.0"}


def _fd_player_name(player: Any) -> Optional[str]:
    if isinstance(player, str):
        return clean_person_name(player)
    if not isinstance(player, dict):
        return None
    for key in ("name", "fullName", "fullname"):
        val = player.get(key)
        if val:
            return clean_person_name(val)
    info = player.get("player")
    if isinstance(info, dict):
        for key in ("name", "fullName", "fullname"):
            val = info.get(key)
            if val:
                return clean_person_name(val)
        first = info.get("firstName")
        last = info.get("lastName")
        if first or last:
            return clean_person_name(" ".join([p for p in [first, last] if p]))
    first = player.get("firstName")
    last = player.get("lastName")
    if first or last:
        return clean_person_name(" ".join([p for p in [first, last] if p]))
    return None


def _extract_players(blob: Any) -> List[str]:
    if not blob:
        return []
    if isinstance(blob, list):
        names: List[str] = []
        for item in blob:
            name = _fd_player_name(item)
            if name:
                names.append(name)
        return _dedupe(_clean_list(names))
    if isinstance(blob, dict):
        for key in ("startXI", "startingXI", "lineup", "startingLineup", "players"):
            val = blob.get(key)
            if val:
                return _extract_players(val)
        return []
    return []


def _sportmonks_headers() -> Dict[str, str]:
    if not settings.sportmonks_api_key:
        return {}
    return {"User-Agent": "Mozilla/5.0"}


def _load_json_map(path: str) -> Dict[str, int]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, int] = {}
    for k, v in data.items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out


def _load_str_map(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in data.items():
        if isinstance(v, str) and v.strip():
            out[str(k)] = v.strip()
    return out


def _diretta_league_urls() -> Dict[str, str]:
    mapping = dict(DEFAULT_DIRETTA_LEAGUE_URLS)
    extra = _load_str_map(settings.diretta_leagues_path)
    mapping.update(extra)
    return mapping


def _sportmonks_league_id(competition: str) -> Optional[int]:
    mapping = _load_json_map(settings.sportmonks_leagues_path)
    league_id = mapping.get(competition)
    if league_id:
        return league_id
    return None


def _sportmonks_request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    base_url = settings.sportmonks_base_url.rstrip("/")
    url = f"{base_url}/{path.lstrip('/')}"
    payload = dict(params)
    payload["api_token"] = settings.sportmonks_api_key
    resp = requests.get(url, headers=_sportmonks_headers(), params=payload, timeout=25)
    resp.raise_for_status()
    data = resp.json()
    errors = data.get("errors") or data.get("error")
    if errors:
        raise RuntimeError(f"sportmonks_error:{errors}")
    return data


def _sm_pick_participants(fixture: Dict[str, Any]) -> Tuple[Optional[dict], Optional[dict]]:
    participants = fixture.get("participants") or fixture.get("participants", {}).get("data")
    if not isinstance(participants, list):
        return None, None
    home = None
    away = None
    for p in participants:
        meta = p.get("meta") or {}
        location = meta.get("location")
        if location == "home":
            home = p
        elif location == "away":
            away = p
    return home, away


def _sm_extract_lineup(lineups: Any, team_id: Optional[int]) -> List[str]:
    if not lineups:
        return []
    if isinstance(lineups, dict):
        lineups = lineups.get("data") or lineups.get("lineups") or []
    if not isinstance(lineups, list):
        return []
    names: List[str] = []
    for item in lineups:
        if not isinstance(item, dict):
            continue
        if team_id is not None:
            item_team = item.get("team_id") or item.get("participant_id")
            if item_team and int(item_team) != int(team_id):
                continue
        name = item.get("player_name") or item.get("name")
        if not name:
            player = item.get("player")
            if isinstance(player, dict):
                name = player.get("name") or player.get("display_name")
        if name:
            names.append(name)
    return _dedupe(_clean_list(names))


def _ingest_sportmonks(
    conn,
    alias_map: Dict[str, str],
    by_key,
    by_pair,
    day_filter: Optional[date],
    competition: str,
) -> Tuple[int, int]:
    league_id = _sportmonks_league_id(competition)
    if not league_id or not day_filter:
        return 0, 0
    if not settings.sportmonks_api_key:
        return 0, 0

    payload = _sportmonks_request(
        f"fixtures/date/{day_filter.isoformat()}",
        {"include": "lineups,participants", "filters[league_id]": league_id},
    )
    fixtures = payload.get("data") or payload.get("response") or []

    inserted = 0
    skipped = 0
    for fixture in fixtures:
        if not isinstance(fixture, dict):
            continue
        fx_league = fixture.get("league_id")
        if fx_league and int(fx_league) != int(league_id):
            continue

        home_part, away_part = _sm_pick_participants(fixture)
        home_name = None
        away_name = None
        home_id = None
        away_id = None
        if home_part:
            home_name = home_part.get("name")
            home_id = home_part.get("id")
        if away_part:
            away_name = away_part.get("name")
            away_id = away_part.get("id")

        if not home_name or not away_name:
            teams = fixture.get("teams") or {}
            home_name = home_name or (teams.get("home") or {}).get("name")
            away_name = away_name or (teams.get("away") or {}).get("name")

        if not home_name or not away_name:
            skipped += 1
            continue

        match_day = _parse_kickoff_date(fixture.get("starting_at")) or day_filter
        match_id = _find_match_id(by_key, by_pair, alias_map, home_name, away_name, match_day)
        if not match_id:
            skipped += 1
            continue

        lineups = fixture.get("lineups")
        home_players = _sm_extract_lineup(lineups, home_id)
        away_players = _sm_extract_lineup(lineups, away_id)
        if len(home_players) < 9 or len(away_players) < 9:
            skipped += 1
            continue

        ok = _insert_lineup(
            conn,
            match_id,
            "SportMonks",
            0.85,
            home_players,
            away_players,
            [],
            [],
            "sportmonks_lineups",
            f"sportmonks:fixture:{fixture.get('id')}",
        )
        if ok:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


def _api_football_headers() -> Dict[str, str]:
    api_key = settings.api_football_key
    if not api_key:
        return {}
    return {"x-apisports-key": api_key, "User-Agent": "Mozilla/5.0"}


def _season_start_from_label(label: Optional[str]) -> Optional[int]:
    if not label:
        return None
    s = str(label).strip()
    if "/" in s:
        left = s.split("/", 1)[0]
        if left.isdigit():
            return int(left)
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _season_for_day(conn, competition: str, day_filter: date) -> Optional[int]:
    start = datetime(day_filter.year, day_filter.month, day_filter.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    row = conn.execute(
        """
        SELECT season
        FROM matches
        WHERE competition = ?
          AND kickoff_utc >= ? AND kickoff_utc < ?
        ORDER BY kickoff_utc ASC
        LIMIT 1
        """,
        (
            competition,
            start.isoformat().replace("+00:00", "Z"),
            end.isoformat().replace("+00:00", "Z"),
        ),
    ).fetchone()
    if row and row["season"]:
        season = _season_start_from_label(row["season"])
        if season:
            return season
    if day_filter.month >= 7:
        return day_filter.year
    return day_filter.year - 1


def _match_team_side(
    team_name: Optional[str],
    home: str,
    away: str,
    alias_map: Dict[str, str],
) -> Optional[str]:
    if not team_name:
        return None
    team_norm = _team_key(team_name, alias_map)
    home_norm = _team_key(home, alias_map)
    away_norm = _team_key(away, alias_map)
    if team_norm == home_norm:
        return "home"
    if team_norm == away_norm:
        return "away"
    score_home = _similarity(team_norm, home_norm)
    score_away = _similarity(team_norm, away_norm)
    if score_home >= 0.82 and score_home >= score_away + 0.05:
        return "home"
    if score_away >= 0.82 and score_away >= score_home + 0.05:
        return "away"
    return None


def _parse_fd_lineups(
    payload: Dict[str, Any],
    home: str,
    away: str,
    alias_map: Dict[str, str],
) -> Tuple[List[str], List[str], Optional[str]]:
    match = payload.get("match")
    if not isinstance(match, dict):
        match = payload if isinstance(payload, dict) else {}
    status = match.get("status") if isinstance(match, dict) else None
    home_players: List[str] = []
    away_players: List[str] = []
    lineups = match.get("lineups") if isinstance(match, dict) else None

    if isinstance(lineups, list):
        for entry in lineups:
            if not isinstance(entry, dict):
                continue
            team = entry.get("team") or {}
            team_name = None
            if isinstance(team, dict):
                team_name = team.get("name") or team.get("shortName")
            if not team_name:
                team_name = entry.get("teamName") or entry.get("team")
            players = _extract_players(
                entry.get("startXI")
                or entry.get("startingXI")
                or entry.get("lineup")
                or entry.get("startingLineup")
                or entry.get("players")
            )
            side = _match_team_side(team_name, home, away, alias_map)
            if side == "home" and players:
                home_players = players
            elif side == "away" and players:
                away_players = players

    if not home_players:
        home_blob = match.get("homeTeam") if isinstance(match, dict) else {}
        home_players = _extract_players(home_blob)
    if not away_players:
        away_blob = match.get("awayTeam") if isinstance(match, dict) else {}
        away_players = _extract_players(away_blob)

    return _dedupe(home_players), _dedupe(away_players), status


def _insert_lineup(
    conn,
    match_id: str,
    source: str,
    confidence: float,
    home_players: List[str],
    away_players: List[str],
    home_absences: List[str],
    away_absences: List[str],
    notes: str,
    raw_ref: Optional[str],
) -> bool:
    home_players = _clean_list(home_players)
    away_players = _clean_list(away_players)
    home_absences = _clean_list(home_absences)
    away_absences = _clean_list(away_absences)
    if len(home_players) < 9 or len(away_players) < 9:
        return False
    lineup_id = stable_hash({
        "source": source,
        "match_id": match_id,
        "home_players": home_players,
        "away_players": away_players,
    })
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
            source,
            _now_utc().isoformat().replace("+00:00", "Z"),
            confidence,
            json.dumps(home_players, ensure_ascii=True),
            json.dumps(away_players, ensure_ascii=True),
            json.dumps(home_absences, ensure_ascii=True),
            json.dumps(away_absences, ensure_ascii=True),
            notes,
            raw_ref,
        ),
    )
    return True


def _fetch_environment(league_url: str) -> Dict[str, object]:
    resp = requests.get(league_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    m = re.search(r"window\\.environment\\s*=\\s*(\\{.*?\\});", resp.text, re.DOTALL)
    if m:
        return json.loads(m.group(1))

    idx = resp.text.find("window.environment")
    if idx == -1:
        raise RuntimeError("window.environment not found on diretta page.")

    start = resp.text.find("{", idx)
    if start == -1:
        raise RuntimeError("window.environment JSON start not found.")

    depth = 0
    end = None
    in_str = False
    escape = False
    for i, ch in enumerate(resp.text[start:], start=start):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == "\"":
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        raise RuntimeError("window.environment JSON end not found.")

    return json.loads(resp.text[start:end])


def _parse_events(html_text: str) -> List[Dict[str, str]]:
    pattern = f"{FIELD_SEP}~AA{KV_SEP}"
    if pattern not in html_text:
        return []
    parts = html_text.split(pattern)
    events = []
    for part in parts[1:]:
        event_id = part[:8]
        chunk = part[:2000]
        fields = {}
        for seg in chunk.split(FIELD_SEP):
            if KV_SEP in seg:
                k, v = seg.split(KV_SEP, 1)
                fields[k] = v
        home = fields.get("CX")
        away = fields.get("AF")
        ts = fields.get("AD")
        if home and away:
            events.append({
                "event_id": event_id,
                "home": home,
                "away": away,
                "kickoff_ts": ts,
            })
    return events


def _parse_lineups(feed_text: str) -> Tuple[List[str], List[str]]:
    home_players: List[str] = []
    away_players: List[str] = []
    current_team = None

    for seg in feed_text.split(FIELD_SEP):
        if KV_SEP not in seg:
            continue
        key, value = seg.split(KV_SEP, 1)
        key = key.lstrip("~")
        if key == "LC":
            if value in ("1", "2"):
                current_team = value
        elif key == "LI" and current_team:
            if current_team == "1":
                name = clean_person_name(value)
                if name and name not in home_players:
                    home_players.append(name)
            elif current_team == "2":
                name = clean_person_name(value)
                if name and name not in away_players:
                    away_players.append(name)

    return home_players, away_players


def _latest_lineup_ts(match_id: str) -> Optional[datetime]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT fetched_at_utc
            FROM probable_lineups
            WHERE match_id = ?
            ORDER BY fetched_at_utc DESC
            LIMIT 1
            """,
            (match_id,),
        ).fetchone()
    if not row:
        return None
    try:
        return datetime.fromisoformat(str(row["fetched_at_utc"]).replace("Z", "+00:00"))
    except ValueError:
        return None


def _ingest_sky(
    conn,
    alias_map: Dict[str, str],
    by_key,
    by_pair,
    day_filter: Optional[date],
) -> Tuple[int, int]:
    resp = requests.get(SKY_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    m = re.search(r"model='({.*?})'", resp.text, re.DOTALL)
    if not m:
        raise RuntimeError("Sky model JSON not found.")
    blob = html.unescape(m.group(1))
    data = json.loads(blob)
    matches = data.get("matchList", []) or []

    inserted = 0
    skipped = 0
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

        match_id = _find_match_id(by_key, by_pair, alias_map, home_name, away_name, match_date or day_filter)
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

        ok = _insert_lineup(
            conn,
            match_id,
            "Sky Sport",
            0.82,
            home_players,
            away_players,
            _dedupe(home_absences),
            _dedupe(away_absences),
            "sky_predicted_lineups",
            SKY_URL,
        )
        if ok:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


def _ingest_gazzetta(
    conn,
    alias_map: Dict[str, str],
    by_key,
    by_pair,
    day_filter: Optional[date],
) -> Tuple[int, int]:
    resp = requests.get(GAZZETTA_LIST_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    links = re.findall(r"/Calcio/prob_form/[^\"']+/\\d+", resp.text)
    unique_links = []
    seen = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        unique_links.append(f"https://www.gazzetta.it{link}")

    inserted = 0
    skipped = 0
    for link in unique_links:
        match_id_gaz = link.split("/")[-1]
        if not match_id_gaz:
            skipped += 1
            continue
        api_url = f"{GAZZETTA_API_BASE}{match_id_gaz}?patchV=true"
        try:
            data = requests.get(api_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20).json()
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

        match_id = _find_match_id(by_key, by_pair, alias_map, home_name, away_name, match_day or day_filter)
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

        ok = _insert_lineup(
            conn,
            match_id,
            "Gazzetta.it",
            0.85,
            home_players,
            away_players,
            home_absences,
            away_absences,
            "gazzetta_lineups_api",
            api_url,
        )
        if ok:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


def _ingest_football_data(
    conn,
    alias_map: Dict[str, str],
    by_key,
    by_pair,
    day_filter: Optional[date],
    competition: str,
) -> Tuple[int, int]:
    comp_code = FOOTBALL_DATA_COMP_CODES.get(competition)
    if not comp_code or not day_filter:
        return 0, 0

    base_url = settings.football_data_base_url.rstrip("/")
    url = f"{base_url}/competitions/{comp_code}/matches"
    headers = _football_data_headers()
    params = {"dateFrom": day_filter.isoformat(), "dateTo": day_filter.isoformat()}
    resp = requests.get(url, headers=headers, params=params, timeout=25)
    resp.raise_for_status()
    payload = resp.json()
    matches = payload.get("matches") or []

    inserted = 0
    skipped = 0
    for m in matches:
        home_team = m.get("homeTeam") or {}
        away_team = m.get("awayTeam") or {}
        home_name = home_team.get("name")
        away_name = away_team.get("name")
        if not home_name or not away_name:
            skipped += 1
            continue

        match_day = _parse_kickoff_date(m.get("utcDate")) or day_filter
        match_id = _find_match_id(by_key, by_pair, alias_map, home_name, away_name, match_day)
        if not match_id:
            skipped += 1
            continue

        fd_match_id = m.get("id")
        if not fd_match_id:
            skipped += 1
            continue

        detail_url = f"{base_url}/matches/{fd_match_id}"
        detail_resp = requests.get(detail_url, headers=headers, timeout=25)
        if detail_resp.status_code != 200:
            skipped += 1
            continue

        detail = detail_resp.json()
        home_players, away_players, status = _parse_fd_lineups(detail, home_name, away_name, alias_map)
        if len(home_players) < 9 or len(away_players) < 9:
            skipped += 1
            continue

        conf = 0.90
        if status and str(status).upper() in ("SCHEDULED", "TIMED"):
            conf = 0.82

        ok = _insert_lineup(
            conn,
            match_id,
            "Football-Data.org",
            conf,
            home_players,
            away_players,
            [],
            [],
            "football_data_lineups",
            detail_url,
        )
        if ok:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


def _ingest_api_football(
    conn,
    alias_map: Dict[str, str],
    by_key,
    by_pair,
    day_filter: Optional[date],
    competition: str,
) -> Tuple[int, int]:
    league_id = API_FOOTBALL_LEAGUE_IDS.get(competition)
    if not league_id or not day_filter:
        return 0, 0
    headers = _api_football_headers()
    if not headers:
        return 0, 0

    season = _season_for_day(conn, competition, day_filter)
    if not season:
        return 0, 0

    base_url = settings.api_football_base_url.rstrip("/")
    fixtures_url = f"{base_url}/fixtures"
    params = {"league": league_id, "season": season, "date": day_filter.isoformat()}
    resp = requests.get(fixtures_url, headers=headers, params=params, timeout=25)
    resp.raise_for_status()
    payload = resp.json()
    errors = payload.get("errors") or {}
    if errors:
        raise RuntimeError(f"api_football_error:{errors}")
    fixtures = payload.get("response") or []

    inserted = 0
    skipped = 0
    for fixture in fixtures:
        teams = fixture.get("teams") or {}
        home_team = (teams.get("home") or {}).get("name")
        away_team = (teams.get("away") or {}).get("name")
        fix = fixture.get("fixture") or {}
        fixture_id = fix.get("id")
        status = (fix.get("status") or {}).get("short")
        if not home_team or not away_team or not fixture_id:
            skipped += 1
            continue

        match_day = _parse_kickoff_date(fix.get("date")) or day_filter
        match_id = _find_match_id(by_key, by_pair, alias_map, home_team, away_team, match_day)
        if not match_id:
            skipped += 1
            continue

        lineups_url = f"{base_url}/fixtures/lineups"
        lineups_resp = requests.get(lineups_url, headers=headers, params={"fixture": fixture_id}, timeout=25)
        if lineups_resp.status_code != 200:
            skipped += 1
            continue
        lineups = lineups_resp.json().get("response") or []
        if not lineups:
            skipped += 1
            continue

        home_players: List[str] = []
        away_players: List[str] = []
        for entry in lineups:
            team_info = entry.get("team") or {}
            team_name = team_info.get("name") if isinstance(team_info, dict) else entry.get("team")
            players = _extract_players(
                entry.get("startXI")
                or entry.get("startingXI")
                or entry.get("lineup")
                or entry.get("startingLineup")
                or entry.get("players")
            )
            side = _match_team_side(team_name, home_team, away_team, alias_map)
            if side == "home" and players:
                home_players = players
            elif side == "away" and players:
                away_players = players

        if len(home_players) < 9 or len(away_players) < 9:
            skipped += 1
            continue

        conf = 0.9
        if status and str(status).upper() in ("NS", "TBD", "SCHEDULED", "TIMED"):
            conf = 0.82

        ok = _insert_lineup(
            conn,
            match_id,
            "API-Football",
            conf,
            home_players,
            away_players,
            [],
            [],
            "api_football_lineups",
            f"{lineups_url}?fixture={fixture_id}",
        )
        if ok:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


def _ingest_diretta(
    conn,
    alias_map: Dict[str, str],
    matches: List[dict],
    competition: str,
) -> Tuple[int, int]:
    league_url = _diretta_league_urls().get(competition)
    if not league_url:
        return 0, len(matches)

    env = _fetch_environment(league_url)
    feed_sign = env.get("config", {}).get("app", {}).get("feed_sign")
    if not feed_sign:
        return 0, len(matches)

    resp = requests.get(league_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    events = _parse_events(resp.text)
    if not events:
        return 0, len(matches)

    def _norm(team: str) -> str:
        return _team_key(team, alias_map)

    event_map = {(_norm(e["home"]), _norm(e["away"])): e for e in events}
    event_keys = list(event_map.keys())

    def _best_event(home_norm: str, away_norm: str) -> Optional[dict]:
        best_key = None
        best_score = 0.0
        for key in event_keys:
            score = _similarity(home_norm, key[0]) + _similarity(away_norm, key[1])
            if score > best_score:
                best_score = score
                best_key = key
        if best_key and best_score >= 1.7:
            return event_map.get(best_key)
        return None

    inserted = 0
    skipped = 0
    for m in matches:
        key = (_norm(m["home"]), _norm(m["away"]))
        ev = event_map.get(key)
        if not ev:
            ev = _best_event(key[0], key[1])
        if not ev:
            skipped += 1
            continue

        event_id = ev["event_id"]
        feed_url = f"https://www.diretta.it/x/feed/df_li_1_{event_id}"
        headers = {"User-Agent": "Mozilla/5.0", "x-fsign": feed_sign}
        feed_resp = requests.get(feed_url, headers=headers, timeout=20)
        if feed_resp.status_code != 200:
            skipped += 1
            continue

        home_players, away_players = _parse_lineups(feed_resp.text)
        if len(home_players) < 9 or len(away_players) < 9:
            skipped += 1
            continue

        ok = _insert_lineup(
            conn,
            m["match_id"],
            "Diretta.it",
            0.82,
            home_players,
            away_players,
            [],
            [],
            "parsed_from_feed",
            feed_url,
        )
        if ok:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


def refresh_lineups_for_day(
    day_utc: date,
    competition: Optional[str],
    aliases_path: str = ALIASES_DEFAULT,
    min_interval_minutes: int = 10,
    diretta_only: bool = False,
) -> List[str]:
    if not competition:
        return ["LINEUPS_REFRESH_SKIPPED_NO_COMPETITION"]
    key = f"{competition}:{day_utc.isoformat()}"
    now = _now_utc()
    last = _LAST_REFRESH.get(key)
    if last and (now - last).total_seconds() < (min_interval_minutes * 60):
        return ["LINEUPS_REFRESH_SKIPPED_RECENT"]

    _LAST_REFRESH[key] = now

    notes: List[str] = []
    with get_conn() as conn:
        alias_map = _team_alias_map(conn, _load_aliases(aliases_path))
        match_rows = _load_matches(conn, competition, day_utc)
        if not match_rows:
            return ["LINEUPS_REFRESH_NO_MATCHES"]
        by_key, by_pair = _build_match_index(match_rows, alias_map)

        if diretta_only:
            try:
                ins, sk = _ingest_diretta(conn, alias_map, match_rows, competition)
                notes.append(f"lineups_diretta_inserted={ins}")
                notes.append(f"lineups_diretta_skipped={sk}")
            except Exception:
                notes.append("lineups_diretta_error")
            return notes

        if competition in API_FOOTBALL_LEAGUE_IDS:
            if settings.api_football_key:
                try:
                    ins, sk = _ingest_api_football(conn, alias_map, by_key, by_pair, day_utc, competition)
                    notes.append(f"lineups_api_football_inserted={ins}")
                    notes.append(f"lineups_api_football_skipped={sk}")
                except Exception as exc:
                    msg = str(exc).replace("\n", " ")
                    if len(msg) > 120:
                        msg = msg[:120] + "..."
                    notes.append(f"lineups_api_football_error={msg}")
            else:
                notes.append("lineups_api_football_skipped_no_key")

        if settings.sportmonks_api_key:
            try:
                ins, sk = _ingest_sportmonks(conn, alias_map, by_key, by_pair, day_utc, competition)
                notes.append(f"lineups_sportmonks_inserted={ins}")
                notes.append(f"lineups_sportmonks_skipped={sk}")
            except Exception as exc:
                msg = str(exc).replace("\n", " ")
                if len(msg) > 120:
                    msg = msg[:120] + "..."
                notes.append(f"lineups_sportmonks_error={msg}")
        else:
            notes.append("lineups_sportmonks_skipped_no_key")

        if competition in FOOTBALL_DATA_COMP_CODES:
            if settings.football_data_api_key:
                try:
                    ins, sk = _ingest_football_data(conn, alias_map, by_key, by_pair, day_utc, competition)
                    notes.append(f"lineups_football_data_inserted={ins}")
                    notes.append(f"lineups_football_data_skipped={sk}")
                except Exception:
                    notes.append("lineups_football_data_error")
            else:
                notes.append("lineups_football_data_skipped_no_key")

        if competition == "Serie_A":
            try:
                ins, sk = _ingest_sky(conn, alias_map, by_key, by_pair, day_utc)
                notes.append(f"lineups_sky_inserted={ins}")
                notes.append(f"lineups_sky_skipped={sk}")
            except Exception:
                notes.append("lineups_sky_error")

            try:
                ins, sk = _ingest_gazzetta(conn, alias_map, by_key, by_pair, day_utc)
                notes.append(f"lineups_gazzetta_inserted={ins}")
                notes.append(f"lineups_gazzetta_skipped={sk}")
            except Exception:
                notes.append("lineups_gazzetta_error")
        else:
            notes.append("lineups_sky_skipped_non_serie_a")
            notes.append("lineups_gazzetta_skipped_non_serie_a")

        try:
            ins, sk = _ingest_diretta(conn, alias_map, match_rows, competition)
            notes.append(f"lineups_diretta_inserted={ins}")
            notes.append(f"lineups_diretta_skipped={sk}")
        except Exception:
            notes.append("lineups_diretta_error")

    return notes


def ensure_lineups_for_match(
    match_id: str,
    kickoff_utc: Optional[datetime],
    max_age_hours: int = 6,
) -> List[str]:
    match_day = kickoff_utc.date() if kickoff_utc else None
    competition = None

    with get_conn() as conn:
        row = conn.execute(
            "SELECT competition, kickoff_utc FROM matches WHERE match_id = ?",
            (match_id,),
        ).fetchone()
        if row:
            competition = row["competition"]
            if not match_day:
                match_day = _parse_kickoff_date(row["kickoff_utc"])

    last_ts = _latest_lineup_ts(match_id)
    if last_ts:
        age_hours = (_now_utc() - last_ts).total_seconds() / 3600.0
        if age_hours <= max_age_hours:
            return ["LINEUPS_REFRESH_SKIPPED_RECENT"]

    if not match_day or not competition:
        return ["LINEUPS_REFRESH_SKIPPED_NO_MATCH_META"]

    return refresh_lineups_for_day(match_day, competition)

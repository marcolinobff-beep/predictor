from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import unicodedata
from difflib import SequenceMatcher
from datetime import datetime, timezone, date, timedelta
from typing import Optional, Dict, Any, List, Tuple
from uuid import uuid4

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def norm_team(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("&", "and")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", "", s)
    aliases = {
        "internazionale": "inter",
        "intermilan": "inter",
        "acmilan": "milan",
        "asroma": "roma",
        "sscnapoli": "napoli",
        "sslazio": "lazio",
        "hellasverona": "verona",
        "parmacalcio1913": "parma",
        "como1907": "como",
        "uscremonese": "cremonese",
        "ussassuolo": "sassuolo",
        "uslecce": "lecce",
        "cagliaricalcio": "cagliari",
        "genoacfc": "genoa",
        "torinofc": "torino",
        "atalantabc": "atalanta",
        "acffiorentina": "fiorentina",
        "bolognafc": "bologna",
        "spal": "spal",
        "udinese": "udinese",
        "empoli": "empoli",
        "frosinone": "frosinone",
        "salernitana": "salernitana",
        "sampdoria": "sampdoria",
        "spezia": "spezia",
        "monza": "monza",
        "manunited": "manchesterunited",
        "manutd": "manchesterunited",
        "manchesterutd": "manchesterunited",
        "manchestercity": "manchestercity",
        "mancity": "manchestercity",
        "newcastleunited": "newcastle",
        "newcastleutd": "newcastle",
        "westhamunited": "westham",
        "nottinghamforest": "nottinghamforest",
        "nottmforest": "nottinghamforest",
        "sheffieldunited": "sheffieldunited",
        "sheffieldutd": "sheffieldunited",
        "brightonandhovealbion": "brighton",
        "brightonhovealbion": "brighton",
        "afcbournemouth": "bournemouth",
        "fulham": "fulham",
        "crystalpalace": "crystalpalace",
        "astonvilla": "astonvilla",
        "leicestercity": "leicester",
        "leedsunited": "leeds",
        "everton": "everton",
        "burnley": "burnley",
        "brentford": "brentford",
        "liverpool": "liverpool",
        "arsenal": "arsenal",
        "chelsea": "chelsea",
        "wolves": "wolves",
        "psg": "parissaintgermain",
        "parissg": "parissaintgermain",
        "parissaintgermain": "parissaintgermain",
        "saintetienne": "saintetienne",
        "stetienne": "saintetienne",
        "olympiquelyon": "lyon",
        "olympiquemarseille": "marseille",
        "marseille": "marseille",
        "monaco": "monaco",
        "rennes": "rennes",
        "lille": "lille",
        "nice": "nice",
        "nantes": "nantes",
        "montpellier": "montpellier",
        "reims": "reims",
        "lens": "lens",
        "strasbourg": "strasbourg",
        "bayernmunich": "bayernmunchen",
        "bayernmunchen": "bayernmunchen",
        "bayerleverkusen": "leverkusen",
        "borussiadortmund": "dortmund",
        "mgladbach": "borussiamonchengladbach",
        "borussiamgladbach": "borussiamonchengladbach",
        "borussiamonchengladbach": "borussiamonchengladbach",
        "eintrachtfrankfurt": "frankfurt",
        "rbl": "leipzig",
        "rbleipzig": "leipzig",
        "fcaugsburg": "augsburg",
        "vfb": "stuttgart",
        "fcstpauli": "stpauli",
        "fckoln": "koln",
        "1fckoln": "koln",
        "koln": "koln",
        "hoffenheim": "hoffenheim",
        "werderbremen": "bremen",
        "bremen": "bremen",
        "mainz05": "mainz",
        "mainz": "mainz",
        "wolverhamptonwanderers": "wolves",
        "tottenham": "tottenhamhotspur",
        "spurs": "tottenhamhotspur",
        "tottenhamhotspur": "tottenhamhotspur",
        "athleticbilbao": "athleticclub",
        "realbetisbalompie": "realbetis",
        "atleticomadrid": "atletico",
        "atleticodemadrid": "atletico",
        "athmadrid": "atletico",
        "realmadrid": "realmadrid",
        "barcelona": "barcelona",
        "fcbarcelona": "barcelona",
        "realsociedad": "realsociedad",
        "athleticclub": "athleticclub",
        "sevilla": "sevilla",
        "valencia": "valencia",
        "villareal": "villarreal",
        "villarreal": "villarreal",
        "realvalladolid": "valladolid",
        "realzaragoza": "zaragoza",
        "osasuna": "osasuna",
        "celta": "celta",
        "celtavigo": "celta",
        "mallorca": "mallorca",
        "laspalmas": "laspalmas",
        "alaves": "alaves",
        "alaveses": "alaves",
    }
    return aliases.get(s, s)


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _find_row(rows: List[Dict[str, str]], nh: str, na: str) -> Optional[Dict[str, str]]:
    for row in rows:
        if row.get("_home_norm") == nh and row.get("_away_norm") == na:
            return row
    best = None
    best_score = 0.0
    for row in rows:
        score = _similarity(nh, row.get("_home_norm", "")) + _similarity(na, row.get("_away_norm", ""))
        if score > best_score:
            best_score = score
            best = row
    if best and best_score >= 1.66:
        return best
    return None


def parse_fd_date(date_str: str) -> Optional[datetime]:
    s = (date_str or "").strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def parse_iso_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def detect_odds_table_cols(conn) -> Dict[str, bool]:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(odds_quotes)").fetchall()]
    return {
        "has_quote_id": "quote_id" in cols,
        "has_batch_id": "batch_id" in cols,
        "has_retrieved": "retrieved_at_utc" in cols,
        "has_source_id": "source_id" in cols,
        "has_reliability_score": "reliability_score" in cols,
        "has_ttl_seconds": "ttl_seconds" in cols,
        "has_cache_hit": "cache_hit" in cols,
        "has_raw_ref": "raw_ref" in cols,
    }


def pick_odds(row: Dict[str, str], pre: str, closing: bool) -> Optional[float]:
    key = pre
    if closing:
        key = pre.replace("B365", "B365C", 1)

    v = (row.get(key) or "").strip()
    if not v and closing:
        v = (row.get(pre) or "").strip()

    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _insert_odds_row(conn, meta: Dict[str, bool], payload: Dict[str, Any]) -> None:
    allowed_cols = [
        "quote_id", "match_id", "bookmaker", "market", "selection", "odds_decimal",
        "retrieved_at_utc", "batch_id", "source_id",
        "reliability_score", "ttl_seconds", "cache_hit", "raw_ref",
    ]

    cols = []
    vals = []
    for c in allowed_cols:
        if c in payload:
            if c == "quote_id" and not meta["has_quote_id"]:
                continue
            if c == "batch_id" and not meta["has_batch_id"]:
                continue
            if c == "source_id" and not meta["has_source_id"]:
                continue
            if c == "reliability_score" and not meta["has_reliability_score"]:
                continue
            if c == "ttl_seconds" and not meta["has_ttl_seconds"]:
                continue
            if c == "cache_hit" and not meta["has_cache_hit"]:
                continue
            if c == "raw_ref" and not meta["has_raw_ref"]:
                continue
            cols.append(c)
            vals.append(payload[c])

    if not cols:
        raise RuntimeError("Nessuna colonna valida da inserire in odds_quotes (schema inatteso).")

    placeholders = ", ".join(["?"] * len(cols))
    col_sql = ", ".join(cols)

    conn.execute(
        f"INSERT INTO odds_quotes ({col_sql}) VALUES ({placeholders})",
        tuple(vals),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--division", required=True)  # es: I1
    ap.add_argument("--season", required=True)    # es: 2526
    ap.add_argument("--closing", action="store_true")
    ap.add_argument("--competition", default=None)
    ap.add_argument("--url", default=None)
    args = ap.parse_args()

    day = date.fromisoformat(args.date)
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    start_iso = start.isoformat().replace("+00:00", "Z")
    end_iso = end.isoformat().replace("+00:00", "Z")

    url = args.url or f"https://www.football-data.co.uk/mmz4281/{args.season}/{args.division}.csv"

    with get_conn() as conn:
        sql = """
            SELECT match_id, home, away, kickoff_utc
            FROM matches
            WHERE kickoff_utc >= ? AND kickoff_utc < ?
        """
        params: List[Any] = [start_iso, end_iso]
        if args.competition:
            sql += " AND competition = ?"
            params.append(args.competition)
        sql += " ORDER BY kickoff_utc ASC"
        matches = conn.execute(sql, params).fetchall()

        if not matches:
            print("No matches found for that date.")
            return

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    text = resp.text
    rows = list(csv.DictReader(io.StringIO(text)))
    rows_by_date: Dict[date, List[Dict[str, str]]] = {}
    for row in rows:
        d = parse_fd_date(row.get("Date", ""))
        if not d:
            continue
        row["_home_norm"] = norm_team(row.get("HomeTeam", ""))
        row["_away_norm"] = norm_team(row.get("AwayTeam", ""))
        rows_by_date.setdefault(d.date(), []).append(row)

    retrieved_at = now_iso_z()
    inserted = 0
    missing = []

    with get_conn() as conn:
        meta = detect_odds_table_cols(conn)
        for m in matches:
            match_id = m["match_id"]
            home_name = m["home"]
            away_name = m["away"]
            kickoff_dt = parse_iso_dt(m["kickoff_utc"])
            if not kickoff_dt:
                missing.append(match_id)
                continue
            target_date = kickoff_dt.astimezone(timezone.utc).date()
            nh, na = norm_team(str(home_name)), norm_team(str(away_name))

            candidates = rows_by_date.get(target_date, [])
            found = _find_row(candidates, nh, na)
            if not found:
                # fallback: some providers use local date (could differ by 1 day vs UTC)
                for offset in (-1, 1):
                    alt_date = target_date + timedelta(days=offset)
                    alt_candidates = rows_by_date.get(alt_date, [])
                    found = _find_row(alt_candidates, nh, na)
                    if found:
                        break

            if not found:
                missing.append(match_id)
                continue

            odds_rows = []
            o_h = pick_odds(found, "B365H", args.closing)
            o_d = pick_odds(found, "B365D", args.closing)
            o_a = pick_odds(found, "B365A", args.closing)
            if o_h and o_d and o_a:
                odds_rows += [
                    ("Bet365", "1X2", "HOME", o_h),
                    ("Bet365", "1X2", "DRAW", o_d),
                    ("Bet365", "1X2", "AWAY", o_a),
                ]

            o_over = pick_odds(found, "B365>2.5", args.closing)
            o_under = pick_odds(found, "B365<2.5", args.closing)
            if o_over and o_under:
                odds_rows += [
                    ("Bet365", "OU_2.5", "OVER", o_over),
                    ("Bet365", "OU_2.5", "UNDER", o_under),
                ]

            if not odds_rows:
                missing.append(match_id)
                continue

            source_id = f"football_data:{args.division}:{args.season}:{'closing' if args.closing else 'pre'}"
            batch_id = f"fd_{args.division}_{args.season}_{retrieved_at}_{uuid4()}"
            reliability_score = 0.90
            ttl_seconds = 24 * 3600
            cache_hit = True
            raw_ref = f"football-data.co.uk {args.division} {args.season} (closing={args.closing})"

            conn.execute(
                "DELETE FROM odds_quotes WHERE match_id=? AND bookmaker='Bet365' AND source_id=?",
                (match_id, source_id),
            )

            for bookmaker, market, selection, odds_dec in odds_rows:
                payload = {
                    "quote_id": f"{match_id}:{bookmaker}:{market}:{selection}:{batch_id}",
                    "match_id": match_id,
                    "bookmaker": bookmaker,
                    "market": market,
                    "selection": selection,
                    "odds_decimal": float(odds_dec),
                    "retrieved_at_utc": retrieved_at,
                    "batch_id": batch_id,
                    "source_id": source_id,
                    "reliability_score": reliability_score,
                    "ttl_seconds": ttl_seconds,
                    "cache_hit": int(cache_hit) if isinstance(cache_hit, bool) else cache_hit,
                    "raw_ref": raw_ref,
                }
                _insert_odds_row(conn, meta, payload)
                inserted += 1

        conn.commit()

    print(f"OK: inserted={inserted} missing_matches={len(missing)}")
    if missing:
        print("Missing match_ids:", ", ".join(missing))


if __name__ == "__main__":
    main()

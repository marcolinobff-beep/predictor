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
        "manunited": "manchesterunited",
        "manutd": "manchesterunited",
        "manchesterutd": "manchesterunited",
        "psg": "parissaintgermain",
        "parissg": "parissaintgermain",
        "bayernmunich": "bayernmunchen",
        "mgladbach": "borussiamonchengladbach",
        "borussiamgladbach": "borussiamonchengladbach",
        "rbleipzig": "leipzig",
        "wolverhamptonwanderers": "wolves",
        "tottenham": "tottenhamhotspur",
        "spurs": "tottenhamhotspur",
        "athleticbilbao": "athleticclub",
        "realbetisbalompie": "realbetis",
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


def _competition_values(name: Optional[str]) -> List[str]:
    if not name:
        return []
    if name == "Serie_A":
        return ["Serie_A", "Serie A"]
    return [name]


def _season_to_code(season: str) -> Optional[str]:
    s = (season or "").strip()
    if not s:
        return None
    if re.fullmatch(r"\d{4}", s):
        return s
    m = re.match(r"(\d{4}).*?(\d{2,4})$", s)
    if not m:
        return None
    y1 = m.group(1)
    y2 = m.group(2)
    if len(y2) == 4:
        y2 = y2[2:]
    return f"{y1[2:]}{y2}"


def _load_matches(
    conn,
    start_dt: datetime,
    end_dt: datetime,
    competition_values: List[str],
    season: Optional[str] = None,
) -> List[Any]:
    sql = """
        SELECT match_id, home, away, kickoff_utc
        FROM matches
        WHERE kickoff_utc >= ? AND kickoff_utc < ?
    """
    params: List[Any] = [
        start_dt.isoformat().replace("+00:00", "Z"),
        end_dt.isoformat().replace("+00:00", "Z"),
    ]
    if season:
        sql += " AND season = ?"
        params.append(season)
    if competition_values:
        placeholders = ", ".join(["?"] * len(competition_values))
        sql += f" AND competition IN ({placeholders})"
        params.extend(competition_values)
    sql += " ORDER BY kickoff_utc ASC"
    return conn.execute(sql, params).fetchall()


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
    ap.add_argument("--start-date", help="YYYY-MM-DD (UTC)")
    ap.add_argument("--end-date", help="YYYY-MM-DD (UTC, inclusive)")
    ap.add_argument("--division", default="I1")  # es: I1
    ap.add_argument("--season", default=None)    # es: 2425
    ap.add_argument("--all-seasons", action="store_true")
    ap.add_argument("--competition", default="Serie_A")
    ap.add_argument("--closing", action="store_true")
    ap.add_argument("--url", default=None)
    ap.add_argument("--max-matches", type=int, default=0)
    ap.add_argument("--hours-before", type=int, default=6)
    ap.add_argument("--closing-offset-minutes", type=int, default=10)
    args = ap.parse_args()

    competition_values = _competition_values(args.competition)

    def run_range(
        season_label: str,
        season_code: str,
        start_dt: datetime,
        end_dt: datetime,
        url_override: Optional[str] = None,
    ) -> Tuple[int, int]:
        url = url_override or f"https://www.football-data.co.uk/mmz4281/{season_code}/{args.division}.csv"

        with get_conn() as conn:
            matches = _load_matches(conn, start_dt, end_dt, competition_values, season_label)

        if not matches:
            print(f"No matches found for season {season_label}.")
            return 0, 0

        if args.max_matches and args.max_matches > 0:
            matches = matches[: args.max_matches]

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(resp.text)))

        rows_by_date: Dict[date, List[Dict[str, str]]] = {}
        for row in rows:
            d = parse_fd_date(row.get("Date", ""))
            if not d:
                continue
            row["_home_norm"] = norm_team(row.get("HomeTeam", ""))
            row["_away_norm"] = norm_team(row.get("AwayTeam", ""))
            rows_by_date.setdefault(d.date(), []).append(row)

        inserted = 0
        missing = 0

        with get_conn() as conn:
            meta = detect_odds_table_cols(conn)
            for m in matches:
                match_id = m["match_id"]
                home_name = m["home"]
                away_name = m["away"]
                kickoff_dt = parse_iso_dt(m["kickoff_utc"])
                if not kickoff_dt:
                    missing += 1
                    continue
                if args.closing:
                    retrieved_at = (kickoff_dt - timedelta(minutes=args.closing_offset_minutes)).astimezone(timezone.utc)
                else:
                    retrieved_at = (kickoff_dt - timedelta(hours=args.hours_before)).astimezone(timezone.utc)
                retrieved_at_iso = retrieved_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                target_date = kickoff_dt.astimezone(timezone.utc).date()
                nh = norm_team(str(home_name))
                na = norm_team(str(away_name))
                candidates = rows_by_date.get(target_date, [])
                row = _find_row(candidates, nh, na)
                if not row:
                    missing += 1
                    continue

                odds_rows: List[Tuple[str, str, str, float]] = []
                o_h = pick_odds(row, "B365H", args.closing)
                o_d = pick_odds(row, "B365D", args.closing)
                o_a = pick_odds(row, "B365A", args.closing)
                if o_h and o_d and o_a:
                    odds_rows += [
                        ("Bet365", "1X2", "HOME", o_h),
                        ("Bet365", "1X2", "DRAW", o_d),
                        ("Bet365", "1X2", "AWAY", o_a),
                    ]

                o_over = pick_odds(row, "B365>2.5", args.closing)
                o_under = pick_odds(row, "B365<2.5", args.closing)
                if o_over and o_under:
                    odds_rows += [
                        ("Bet365", "OU_2.5", "OVER", o_over),
                        ("Bet365", "OU_2.5", "UNDER", o_under),
                    ]

                if not odds_rows:
                    missing += 1
                    continue

                source_id = f"football_data:{args.division}:{season_code}:{'closing' if args.closing else 'pre'}"
                batch_id = f"fd_{args.division}_{season_code}_{retrieved_at}_{uuid4()}"
                reliability_score = 0.90
                ttl_seconds = 24 * 3600
                cache_hit = True
                raw_ref = f"football-data.co.uk {args.division} {season_code} (closing={args.closing})"

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
                        "retrieved_at_utc": retrieved_at_iso,
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

        print(f"OK: season={season_label} inserted={inserted} missing_matches={missing} url={url}")
        return inserted, missing

    if args.all_seasons:
        with get_conn() as conn:
            sql = """
                SELECT season, MIN(kickoff_utc) AS min_k, MAX(kickoff_utc) AS max_k, COUNT(*) AS cnt
                FROM matches
                WHERE season IS NOT NULL
            """
            params: List[Any] = []
            if competition_values:
                placeholders = ", ".join(["?"] * len(competition_values))
                sql += f" AND competition IN ({placeholders})"
                params.extend(competition_values)
            sql += " GROUP BY season ORDER BY season"
            rows = conn.execute(sql, params).fetchall()

        if not rows:
            print("No seasons found in DB for that competition.")
            return

        total_ins = 0
        total_miss = 0
        for row in rows:
            season_label = row["season"]
            season_code = _season_to_code(season_label)
            if not season_code:
                print(f"Skip season {season_label}: cannot derive football-data season code.")
                continue
            min_k = parse_iso_dt(row["min_k"])
            max_k = parse_iso_dt(row["max_k"])
            if not (min_k and max_k):
                print(f"Skip season {season_label}: missing kickoff range.")
                continue
            start_dt = datetime(min_k.year, min_k.month, min_k.day, tzinfo=timezone.utc)
            end_dt = datetime(max_k.year, max_k.month, max_k.day, tzinfo=timezone.utc) + timedelta(days=1)
            ins, miss = run_range(season_label, season_code, start_dt, end_dt, args.url)
            total_ins += ins
            total_miss += miss

        print(f"OK: total_inserted={total_ins} total_missing={total_miss}")
        return

    if not args.start_date or not args.end_date or not args.season:
        raise SystemExit("Missing --start-date/--end-date/--season (or use --all-seasons).")

    start_day = date.fromisoformat(args.start_date)
    end_day = date.fromisoformat(args.end_date)
    if end_day < start_day:
        raise SystemExit("end-date must be >= start-date.")

    start_dt = datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc)
    end_dt = datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc) + timedelta(days=1)
    run_range(args.season, args.season, start_dt, end_dt, args.url)


if __name__ == "__main__":
    main()

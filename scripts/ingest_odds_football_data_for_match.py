from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def norm_team(s: str) -> str:
    s = (s or "").strip().lower()
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
    }
    return aliases.get(s, s)


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


def detect_match_table(conn) -> Tuple[str, Dict[str, str]]:
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]

    def cols(t: str) -> List[str]:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({t})").fetchall()]

    home_candidates = ["home_team", "home", "home_name", "home_team_name"]
    away_candidates = ["away_team", "away", "away_name", "away_team_name"]
    kickoff_candidates = ["kickoff_utc", "kickoff", "date_utc", "kickoff_datetime", "kickoff_time_utc"]

    for t in tables:
        c = cols(t)
        if "match_id" not in c:
            continue
        home_col = next((x for x in home_candidates if x in c), None)
        away_col = next((x for x in away_candidates if x in c), None)
        ko_col = next((x for x in kickoff_candidates if x in c), None)
        if home_col and away_col and ko_col:
            return t, {"home": home_col, "away": away_col, "kickoff": ko_col}

    if "matches" in tables:
        c = cols("matches")
        if "match_id" in c:
            home_col = next((x for x in home_candidates if x in c), None)
            away_col = next((x for x in away_candidates if x in c), None)
            ko_col = next((x for x in kickoff_candidates if x in c), None)
            if home_col and away_col and ko_col:
                return "matches", {"home": home_col, "away": away_col, "kickoff": ko_col}

    raise RuntimeError("Non trovo una tabella match con match_id + home/away + kickoff_*.")


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
    """
    Insert dinamico: usa solo le colonne presenti nella tabella.
    """
    allowed_cols = [
        "quote_id", "match_id", "bookmaker", "market", "selection", "odds_decimal",
        "retrieved_at_utc", "batch_id", "source_id",
        "reliability_score", "ttl_seconds", "cache_hit", "raw_ref",
    ]

    # filtra solo colonne che esistono davvero (in base a meta)
    cols = []
    vals = []
    for c in allowed_cols:
        if c in payload:
            # colonne "opzionali" che potrebbero non esistere nello schema
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--match_id", required=True)
    ap.add_argument("--division", required=True)  # es: I1
    ap.add_argument("--season", required=True)    # es: 2526
    ap.add_argument("--closing", action="store_true")
    ap.add_argument("--url", default=None)
    args = ap.parse_args()

    url = args.url or f"https://www.football-data.co.uk/mmz4281/{args.season}/{args.division}.csv"

    # 1) leggi match dal DB
    with get_conn() as conn:
        match_table, mcols = detect_match_table(conn)
        r = conn.execute(
            f"SELECT match_id, {mcols['home']}, {mcols['away']}, {mcols['kickoff']} "
            f"FROM {match_table} WHERE match_id=?",
            (args.match_id,)
        ).fetchone()
        if not r:
            raise RuntimeError(f"match_id non trovato in {match_table}: {args.match_id}")

        match_id, home_name, away_name, kickoff_raw = r[0], r[1], r[2], r[3]
        kickoff_dt = parse_iso_dt(kickoff_raw)
        if not kickoff_dt:
            raise RuntimeError(f"kickoff_utc non parsabile: {kickoff_raw}")

        target_date = kickoff_dt.astimezone(timezone.utc).date()
        nh, na = norm_team(str(home_name)), norm_team(str(away_name))

    # 2) scarica CSV
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    text = resp.text

    # 3) trova riga del match
    found = None
    for row in csv.DictReader(io.StringIO(text)):
        d = parse_fd_date(row.get("Date", ""))
        if not d or d.date() != target_date:
            continue
        if norm_team(row.get("HomeTeam", "")) == nh and norm_team(row.get("AwayTeam", "")) == na:
            found = row
            break

    if not found:
        # fallback: stessa coppia ignorando la data
        cand = []
        for row in csv.DictReader(io.StringIO(text)):
            if norm_team(row.get("HomeTeam", "")) == nh and norm_team(row.get("AwayTeam", "")) == na:
                cand.append(row)

        if len(cand) == 1:
            found = cand[0]
        else:
            cand2 = []
            for row in csv.DictReader(io.StringIO(text)):
                if norm_team(row.get("HomeTeam", "")) == na and norm_team(row.get("AwayTeam", "")) == nh:
                    cand2.append(row)

            msg = {
                "target_date": str(target_date),
                "home": home_name,
                "away": away_name,
                "same_pair_hits": [(c.get("Date"), c.get("Time"), c.get("HomeTeam"), c.get("AwayTeam")) for c in cand[:10]],
                "swapped_pair_hits": [(c.get("Date"), c.get("Time"), c.get("HomeTeam"), c.get("AwayTeam")) for c in cand2[:10]],
            }
            raise RuntimeError("Match non trovato per data. Debug candidates: " + str(msg))

    # 4) estrai odds
    odds_rows = []
    retrieved_at = now_iso_z()

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
        raise RuntimeError("Nel CSV non ho trovato colonne odds utili (B365*).")

    # 5) insert in odds_quotes
    with get_conn() as conn:
        meta = detect_odds_table_cols(conn)

        source_id = f"football_data:{args.division}:{args.season}:{'closing' if args.closing else 'pre'}"
        batch_id = f"fd_{args.division}_{args.season}_{retrieved_at}"

        # pulizia: sovrascrivo le quote Bet365 per quel match
            conn.execute(
                "DELETE FROM odds_quotes WHERE match_id=? AND bookmaker='Bet365' AND source_id=?",
                (match_id, source_id),
            )

        # default “sani”
        reliability_score = 0.90
        ttl_seconds = 24 * 3600  # dataset statico: trattalo come valido 24h
        cache_hit = True
        raw_ref = f"football-data.co.uk {args.division} {args.season} (closing={args.closing})"

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

        conn.commit()

    print(f"OK: imported {len(odds_rows)} odds for {match_id} retrieved_at_utc={retrieved_at}")


if __name__ == "__main__":
    main()

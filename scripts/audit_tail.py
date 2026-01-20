from __future__ import annotations

import argparse
import json
from app.db.sqlite import get_conn

def summarize(payload: dict) -> dict:
    match = payload.get("match") or {}
    ctx = payload.get("match_context") or {}
    sim = (payload.get("simulation_outputs") or {}).get("meta") or {}
    recs = payload.get("recommendations") or []
    no_bet = payload.get("no_bet") or None

    # Conteggio reasons/flags dal market_evaluation
    reasons_count = {}
    me = payload.get("market_evaluation") or []
    for it in me:
        for r in (it.get("reasons") or []):
            reasons_count[r] = reasons_count.get(r, 0) + 1

    return {
        "status": payload.get("status"),
        "match_id": match.get("match_id"),
        "competition": match.get("competition"),
        "season": match.get("season"),
        "kickoff_utc": match.get("kickoff_utc"),
        "features_version": ctx.get("features_version"),
        "data_snapshot_id": ctx.get("data_snapshot_id"),
        "n_sims": sim.get("n_sims"),
        "simulation_id": sim.get("simulation_id"),
        "recs": [
            {
                "market": r.get("market"),
                "selection": r.get("selection"),
                "odds": r.get("odds_decimal"),
                "stake_frac": r.get("stake_fraction"),
                "edge": r.get("expected_edge"),
            }
            for r in recs
        ],
        "no_bet": (no_bet.get("reason_codes") if no_bet else None),
        "reasons_count": reasons_count,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5, help="how many latest audit rows")
    args = ap.parse_args()

    with get_conn() as c:
        rows = c.execute(
            "SELECT rowid, request_id, created_at_utc, payload_json "
            "FROM audit_log ORDER BY rowid DESC LIMIT ?",
            (args.n,),
        ).fetchall()

    if not rows:
        print("No audit rows found.")
        return

    for row in rows:
        rowid, request_id, created_at_utc, payload_json = tuple(row)
        payload = json.loads(payload_json)
        s = summarize(payload)

        print("=" * 80)
        print(f"rowid={rowid} request_id={request_id} created_at_utc={created_at_utc}")
        print(
            f"{s['status']} | {s['competition']} {s['season']} | {s['match_id']} | "
            f"features={s['features_version']} | n_sims={s['n_sims']}"
        )

        if s["recs"]:
            print("recs:")
            for r in s["recs"]:
                print(f"  - {r['market']} {r['selection']} odds={r['odds']} stake={r['stake_frac']} edge={r['edge']}")
        else:
            print(f"no_bet={s['no_bet']}")

        if s["reasons_count"]:
            top = sorted(s["reasons_count"].items(), key=lambda x: (-x[1], x[0]))
            print("market_eval reasons_count (top):")
            for k, v in top[:10]:
                print(f"  - {k}: {v}")

if __name__ == "__main__":
    main()

import argparse
import json
from datetime import datetime, timezone
from app.db.sqlite import get_conn
from app.core.ids import stable_hash

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--window", type=int, default=5)
    ap.add_argument("--features_version", default="understat_v1")
    args = ap.parse_args()

    W = args.window

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT understat_match_id, datetime_utc, home_team, away_team, home_xg, away_xg
            FROM understat_matches
            WHERE league = ? AND season = ? AND home_xg IS NOT NULL AND away_xg IS NOT NULL
            ORDER BY datetime_utc
            """,
            (args.league, args.season)
        ).fetchall()

        # rolling store per team
        hist_for = {}      # team -> list of xG for
        hist_against = {}  # team -> list of xG against

        made = 0
        for r in rows:
            dt = r["datetime_utc"]
            h = r["home_team"]
            a = r["away_team"]
            hxg = float(r["home_xg"])
            axg = float(r["away_xg"])

            hf = hist_for.get(h, [])[-W:]
            ha = hist_against.get(h, [])[-W:]
            af = hist_for.get(a, [])[-W:]
            aa = hist_against.get(a, [])[-W:]

            # calcola solo se abbiamo storico sufficiente
            if len(hf) >= 2 and len(aa) >= 2 and len(af) >= 2 and len(ha) >= 2:
                home_xg_for = sum(hf) / len(hf)
                home_xg_against = sum(ha) / len(ha)
                away_xg_for = sum(af) / len(af)
                away_xg_against = sum(aa) / len(aa)

                # lambda trasparente MVP
                lambda_home = 0.55 * home_xg_for + 0.45 * away_xg_against
                lambda_away = 0.55 * away_xg_for + 0.45 * home_xg_against

                features = {
                    "home_xg_for_w": home_xg_for,
                    "home_xg_against_w": home_xg_against,
                    "away_xg_for_w": away_xg_for,
                    "away_xg_against_w": away_xg_against,
                    "lambda_home": lambda_home,
                    "lambda_away": lambda_away,
                }

                # match_id interno: per MVP usiamo una chiave stabile derivata da understat_match_id
                match_id = f"understat:{r['understat_match_id']}"

                conn.execute(
                    """
                    INSERT OR REPLACE INTO matches(match_id, competition, season, kickoff_utc, home, away, venue)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        match_id,
                        args.league,                      # per ora competition = league_code
                        f"{args.season}/{str(args.season+1)[-2:]}",
                        dt,
                        h, a,
                        None
                    )
                )

                conn.execute(
                    """
                    INSERT OR REPLACE INTO match_features(match_id, features_version, features_json, created_at_utc)
                    VALUES (?, ?, ?, ?)
                    """,
                    (match_id, args.features_version, json.dumps(features), utc_now_iso())
                )

                conn.execute(
                    """
                    INSERT OR REPLACE INTO match_external_ids(match_id, source_id, external_id)
                    VALUES (?, 'understat', ?)
                    """,
                    (match_id, r["understat_match_id"])
                )

                made += 1

            # aggiorna storico (dopo aver calcolato)
            hist_for.setdefault(h, []).append(hxg)
            hist_against.setdefault(h, []).append(axg)
            hist_for.setdefault(a, []).append(axg)
            hist_against.setdefault(a, []).append(hxg)

    print(f"OK: wrote features for {made} matches (features_version={args.features_version})")

if __name__ == "__main__":
    main()

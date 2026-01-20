import json
from argparse import ArgumentParser
from datetime import datetime, timezone

from app.db.sqlite import get_conn


def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _weighted_mean(values, decay: float) -> float:
    if not values:
        return 0.0
    weights = []
    w = 1.0
    for _ in values:
        weights.append(w)
        w *= decay
    total_w = sum(weights)
    if total_w <= 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_w


def _shrink_to_league(value: float, league_avg: float, n: int, k: int = 5) -> float:
    if n <= 0:
        return league_avg
    alpha = n / (n + k)
    return alpha * value + (1.0 - alpha) * league_avg


def _clamp(value: float, lo: float = 0.2, hi: float = 3.5) -> float:
    return max(lo, min(hi, value))


def _season_label(season_start: int) -> str:
    return f"{season_start}/{str(season_start + 1)[-2:]}"


def main():
    ap = ArgumentParser()
    ap.add_argument("--league", required=True)      # es. Serie_A
    ap.add_argument("--season", type=int, required=True)  # es. 2025
    ap.add_argument("--window", type=int, default=5)
    ap.add_argument("--decay-form", type=float, default=0.85)
    ap.add_argument("--decay-season", type=float, default=0.98)
    ap.add_argument("--features_version", default="understat_v3")
    args = ap.parse_args()

    W = args.window
    league = args.league
    season = args.season
    fv = args.features_version
    season_str = _season_label(season)

    with get_conn() as conn:
        league_rows = conn.execute(
            """
            SELECT home_xg, away_xg
            FROM understat_matches
            WHERE league = ? AND season = ?
            """,
            (league, season)
        ).fetchall()
        if not league_rows:
            raise RuntimeError("Nessun match understat trovato per league/season.")

        avg_total_xg = sum((float(r["home_xg"]) + float(r["away_xg"])) for r in league_rows) / len(league_rows)
        league_avg_team_xg = avg_total_xg / 2.0

        matches = conn.execute(
            """
            SELECT match_id, kickoff_utc, home, away
            FROM matches
            WHERE competition = ? AND season = ?
              AND match_id LIKE 'understat:%'
            ORDER BY kickoff_utc ASC
            """,
            (league, season_str)
        ).fetchall()

        wrote = 0
        for m in matches:
            match_id = m["match_id"]
            kickoff = m["kickoff_utc"]
            home = m["home"]
            away = m["away"]

            home_home = conn.execute(
                """
                SELECT home_xg AS xg_for, away_xg AS xg_against
                FROM understat_matches
                WHERE league=? AND season=? AND home_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                LIMIT ?
                """,
                (league, season, home, kickoff, W)
            ).fetchall()

            away_away = conn.execute(
                """
                SELECT away_xg AS xg_for, home_xg AS xg_against
                FROM understat_matches
                WHERE league=? AND season=? AND away_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                LIMIT ?
                """,
                (league, season, away, kickoff, W)
            ).fetchall()

            home_home_season = conn.execute(
                """
                SELECT home_xg AS xg_for, away_xg AS xg_against
                FROM understat_matches
                WHERE league=? AND season=? AND home_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                """,
                (league, season, home, kickoff)
            ).fetchall()

            away_away_season = conn.execute(
                """
                SELECT away_xg AS xg_for, home_xg AS xg_against
                FROM understat_matches
                WHERE league=? AND season=? AND away_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                """,
                (league, season, away, kickoff)
            ).fetchall()

            min_samples = max(3, W // 2)
            if len(home_home) < min_samples or len(away_away) < min_samples:
                continue

            home_xg_for_form = _weighted_mean([float(r["xg_for"]) for r in home_home], args.decay_form)
            home_xg_against_form = _weighted_mean([float(r["xg_against"]) for r in home_home], args.decay_form)
            away_xg_for_form = _weighted_mean([float(r["xg_for"]) for r in away_away], args.decay_form)
            away_xg_against_form = _weighted_mean([float(r["xg_against"]) for r in away_away], args.decay_form)

            home_xg_for_season = _weighted_mean([float(r["xg_for"]) for r in home_home_season], args.decay_season)
            home_xg_against_season = _weighted_mean([float(r["xg_against"]) for r in home_home_season], args.decay_season)
            away_xg_for_season = _weighted_mean([float(r["xg_for"]) for r in away_away_season], args.decay_season)
            away_xg_against_season = _weighted_mean([float(r["xg_against"]) for r in away_away_season], args.decay_season)

            w_form_home = min(0.8, len(home_home) / float(W))
            w_form_away = min(0.8, len(away_away) / float(W))
            w_season_home = 1.0 - w_form_home
            w_season_away = 1.0 - w_form_away

            home_xg_for = (w_form_home * home_xg_for_form) + (w_season_home * home_xg_for_season)
            home_xg_against = (w_form_home * home_xg_against_form) + (w_season_home * home_xg_against_season)
            away_xg_for = (w_form_away * away_xg_for_form) + (w_season_away * away_xg_for_season)
            away_xg_against = (w_form_away * away_xg_against_form) + (w_season_away * away_xg_against_season)

            home_xg_for = _shrink_to_league(home_xg_for, league_avg_team_xg, len(home_home))
            home_xg_against = _shrink_to_league(home_xg_against, league_avg_team_xg, len(home_home))
            away_xg_for = _shrink_to_league(away_xg_for, league_avg_team_xg, len(away_away))
            away_xg_against = _shrink_to_league(away_xg_against, league_avg_team_xg, len(away_away))

            lambda_home = (home_xg_for * away_xg_against) / max(1e-6, league_avg_team_xg)
            lambda_away = (away_xg_for * home_xg_against) / max(1e-6, league_avg_team_xg)
            lambda_home = _clamp(lambda_home)
            lambda_away = _clamp(lambda_away)

            features = {
                "home_xg_for_form": float(home_xg_for_form),
                "home_xg_against_form": float(home_xg_against_form),
                "away_xg_for_form": float(away_xg_for_form),
                "away_xg_against_form": float(away_xg_against_form),
                "home_xg_for_season": float(home_xg_for_season),
                "home_xg_against_season": float(home_xg_against_season),
                "away_xg_for_season": float(away_xg_for_season),
                "away_xg_against_season": float(away_xg_against_season),
                "lambda_home": float(lambda_home),
                "lambda_away": float(lambda_away),
                "league_avg_team_xg": float(league_avg_team_xg),
                "home_samples": float(len(home_home)),
                "away_samples": float(len(away_away)),
                "form_weight_home": float(w_form_home),
                "form_weight_away": float(w_form_away),
            }

            conn.execute(
                """
                INSERT INTO match_features (match_id, features_version, features_json, created_at_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(match_id, features_version)
                DO UPDATE SET
                features_json = excluded.features_json,
                created_at_utc = excluded.created_at_utc
                """,
                (match_id, fv, json.dumps(features), iso_now())
            )
            wrote += 1

        print(f"OK: wrote features for {wrote} matches (features_version={fv})")


if __name__ == "__main__":
    main()

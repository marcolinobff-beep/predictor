import json
import math
import os
import sys
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime, timezone, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.sqlite import get_conn


def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _weight(days: float, half_life_days: float) -> float:
    if half_life_days <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * (days / half_life_days))


def _time_weighted_mean(values, days_ago, half_life_days: float) -> float:
    if not values:
        return 0.0
    weights = [_weight(d, half_life_days) for d in days_ago]
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


def _elo_expected(elo_home: float, elo_away: float, home_adv: float) -> float:
    return 1.0 / (1.0 + 10 ** (-(elo_home + home_adv - elo_away) / 400.0))


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _recent_team_dates(conn, league: str, season: int, team: str, kickoff: datetime):
    rows = conn.execute(
        """
        SELECT datetime_utc
        FROM understat_matches
        WHERE league = ? AND season = ?
          AND (home_team = ? OR away_team = ?)
          AND datetime_utc < ?
        ORDER BY datetime_utc DESC
        """,
        (league, season, team, team, kickoff.isoformat().replace("+00:00", "Z")),
    ).fetchall()
    return [_parse_dt(r["datetime_utc"]) for r in rows]


def _team_overall_rows(conn, league: str, season: int, team: str, kickoff: datetime):
    cutoff = kickoff.isoformat().replace("+00:00", "Z")
    rows = conn.execute(
        """
        SELECT home_xg AS xg_for, away_xg AS xg_against,
               home_goals AS goals_for, away_goals AS goals_against, datetime_utc
        FROM understat_matches
        WHERE league = ? AND season = ?
          AND home_team = ? AND datetime_utc < ?
        UNION ALL
        SELECT away_xg AS xg_for, home_xg AS xg_against,
               away_goals AS goals_for, home_goals AS goals_against, datetime_utc
        FROM understat_matches
        WHERE league = ? AND season = ?
          AND away_team = ? AND datetime_utc < ?
        """,
        (league, season, team, cutoff, league, season, team, cutoff),
    ).fetchall()
    return sorted(rows, key=lambda r: _parse_dt(r["datetime_utc"]), reverse=True)


def _weighted_metric(rows, key: str, kickoff: datetime, half_life_days: float, limit: int | None = None) -> float:
    if not rows:
        return 0.0
    subset = rows[:limit] if limit else rows
    values = []
    days_ago = []
    for r in subset:
        val = r[key]
        if val is None:
            continue
        dt = _parse_dt(r["datetime_utc"])
        values.append(float(val))
        days_ago.append(max(0.0, (kickoff - dt).total_seconds() / 86400.0))
    return _time_weighted_mean(values, days_ago, half_life_days)


def _ratio_clamp(num: float, den: float, lo: float = 0.94, hi: float = 1.06) -> float:
    if den <= 0:
        return 1.0
    return max(lo, min(hi, num / den))


def _std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return var ** 0.5


def _rest_days(dates: list[datetime], kickoff: datetime) -> float | None:
    if not dates:
        return None
    delta = kickoff - dates[0]
    return max(0.0, delta.total_seconds() / 86400.0)


def _count_recent(dates: list[datetime], kickoff: datetime, days: int) -> int:
    if not dates:
        return 0
    cutoff = kickoff - timedelta(days=days)
    return sum(1 for d in dates if d >= cutoff)


def _schedule_factor(rest_days: float | None, matches_7d: int, matches_14d: int) -> float:
    factor = 1.0
    if rest_days is not None:
        factor += max(-0.08, min(0.08, (rest_days - 5.0) * 0.015))
    if matches_7d >= 2:
        factor -= 0.03
    if matches_7d >= 3:
        factor -= 0.02
    if matches_14d >= 4:
        factor -= 0.02
    return max(0.85, min(1.08, factor))


def _compute_elo_index(conn, league: str, season: int, k_factor: float, home_adv: float, half_life_days: float):
    rows = conn.execute(
        """
        SELECT understat_match_id, datetime_utc, home_team, away_team, home_goals, away_goals
        FROM understat_matches
        WHERE league = ? AND season = ?
        ORDER BY datetime_utc ASC
        """,
        (league, season),
    ).fetchall()

    ratings = defaultdict(lambda: 1500.0)
    last_played = {}
    elo_map = {}

    for r in rows:
        home = r["home_team"]
        away = r["away_team"]
        dt = _parse_dt(r["datetime_utc"])

        for team in (home, away):
            last = last_played.get(team)
            if last:
                days = max(0.0, (dt - last).total_seconds() / 86400.0)
                ratings[team] = 1500.0 + (ratings[team] - 1500.0) * _weight(days, half_life_days)

        elo_h = ratings[home]
        elo_a = ratings[away]
        elo_map[r["understat_match_id"]] = (elo_h, elo_a)

        hg = r["home_goals"]
        ag = r["away_goals"]
        if hg is None or ag is None:
            last_played[home] = dt
            last_played[away] = dt
            continue

        if hg > ag:
            score_home = 1.0
        elif hg == ag:
            score_home = 0.5
        else:
            score_home = 0.0

        exp_home = _elo_expected(elo_h, elo_a, home_adv)
        ratings[home] = elo_h + k_factor * (score_home - exp_home)
        ratings[away] = elo_a + k_factor * ((1.0 - score_home) - (1.0 - exp_home))

        last_played[home] = dt
        last_played[away] = dt

    return elo_map


def main():
    ap = ArgumentParser()
    ap.add_argument("--league", required=True)      # es. Serie_A
    ap.add_argument("--season", type=int, required=True)  # es. 2025
    ap.add_argument("--window", type=int, default=5)
    ap.add_argument("--half-life-form", type=float, default=30.0)
    ap.add_argument("--half-life-season", type=float, default=120.0)
    ap.add_argument("--elo-k", type=float, default=20.0)
    ap.add_argument("--elo-home-adv", type=float, default=60.0)
    ap.add_argument("--elo-half-life", type=float, default=120.0)
    ap.add_argument("--features_version", default="understat_v5")
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

        league_rows = [r for r in league_rows if r["home_xg"] is not None and r["away_xg"] is not None]
        if not league_rows:
            raise RuntimeError("Nessun match understat con xG disponibile per league/season.")

        avg_total_xg = sum((float(r["home_xg"]) + float(r["away_xg"])) for r in league_rows) / len(league_rows)
        league_avg_team_xg = avg_total_xg / 2.0

        elo_map = _compute_elo_index(conn, league, season, args.elo_k, args.elo_home_adv, args.elo_half_life)

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
            kickoff = _parse_dt(m["kickoff_utc"])
            home = m["home"]
            away = m["away"]
            understat_id = match_id.split(":", 1)[1]

            home_home = conn.execute(
                """
                SELECT home_xg AS xg_for, away_xg AS xg_against, datetime_utc
                FROM understat_matches
                WHERE league=? AND season=? AND home_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                LIMIT ?
                """,
                (league, season, home, kickoff.isoformat().replace("+00:00", "Z"), W)
            ).fetchall()
            home_home = [r for r in home_home if r["xg_for"] is not None and r["xg_against"] is not None]

            away_away = conn.execute(
                """
                SELECT away_xg AS xg_for, home_xg AS xg_against, datetime_utc
                FROM understat_matches
                WHERE league=? AND season=? AND away_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                LIMIT ?
                """,
                (league, season, away, kickoff.isoformat().replace("+00:00", "Z"), W)
            ).fetchall()
            away_away = [r for r in away_away if r["xg_for"] is not None and r["xg_against"] is not None]

            home_home_season = conn.execute(
                """
                SELECT home_xg AS xg_for, away_xg AS xg_against, datetime_utc
                FROM understat_matches
                WHERE league=? AND season=? AND home_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                """,
                (league, season, home, kickoff.isoformat().replace("+00:00", "Z"))
            ).fetchall()
            home_home_season = [
                r for r in home_home_season if r["xg_for"] is not None and r["xg_against"] is not None
            ]

            away_away_season = conn.execute(
                """
                SELECT away_xg AS xg_for, home_xg AS xg_against, datetime_utc
                FROM understat_matches
                WHERE league=? AND season=? AND away_team=?
                  AND datetime_utc < ?
                ORDER BY datetime_utc DESC
                """,
                (league, season, away, kickoff.isoformat().replace("+00:00", "Z"))
            ).fetchall()
            away_away_season = [
                r for r in away_away_season if r["xg_for"] is not None and r["xg_against"] is not None
            ]

            home_dates = _recent_team_dates(conn, league, season, home, kickoff)
            away_dates = _recent_team_dates(conn, league, season, away, kickoff)
            rest_home = _rest_days(home_dates, kickoff)
            rest_away = _rest_days(away_dates, kickoff)
            matches_7d_home = _count_recent(home_dates, kickoff, 7)
            matches_7d_away = _count_recent(away_dates, kickoff, 7)
            matches_14d_home = _count_recent(home_dates, kickoff, 14)
            matches_14d_away = _count_recent(away_dates, kickoff, 14)

            overall_home = _team_overall_rows(conn, league, season, home, kickoff)
            overall_away = _team_overall_rows(conn, league, season, away, kickoff)

            min_samples = max(3, W // 2)
            samples_low = len(home_home) < min_samples or len(away_away) < min_samples

            def _days_ago(rows):
                return [
                    max(0.0, (kickoff - _parse_dt(r["datetime_utc"])).total_seconds() / 86400.0)
                    for r in rows
                ]

            home_xg_for_form = _time_weighted_mean([float(r["xg_for"]) for r in home_home], _days_ago(home_home), args.half_life_form)
            home_xg_against_form = _time_weighted_mean([float(r["xg_against"]) for r in home_home], _days_ago(home_home), args.half_life_form)
            away_xg_for_form = _time_weighted_mean([float(r["xg_for"]) for r in away_away], _days_ago(away_away), args.half_life_form)
            away_xg_against_form = _time_weighted_mean([float(r["xg_against"]) for r in away_away], _days_ago(away_away), args.half_life_form)

            home_xg_for_season = _time_weighted_mean([float(r["xg_for"]) for r in home_home_season], _days_ago(home_home_season), args.half_life_season)
            home_xg_against_season = _time_weighted_mean([float(r["xg_against"]) for r in home_home_season], _days_ago(home_home_season), args.half_life_season)
            away_xg_for_season = _time_weighted_mean([float(r["xg_for"]) for r in away_away_season], _days_ago(away_away_season), args.half_life_season)
            away_xg_against_season = _time_weighted_mean([float(r["xg_against"]) for r in away_away_season], _days_ago(away_away_season), args.half_life_season)

            home_xg_for_form_all = _weighted_metric(overall_home, "xg_for", kickoff, args.half_life_form, limit=W)
            home_xg_against_form_all = _weighted_metric(overall_home, "xg_against", kickoff, args.half_life_form, limit=W)
            away_xg_for_form_all = _weighted_metric(overall_away, "xg_for", kickoff, args.half_life_form, limit=W)
            away_xg_against_form_all = _weighted_metric(overall_away, "xg_against", kickoff, args.half_life_form, limit=W)

            home_xg_for_season_all = _weighted_metric(overall_home, "xg_for", kickoff, args.half_life_season)
            home_xg_against_season_all = _weighted_metric(overall_home, "xg_against", kickoff, args.half_life_season)
            away_xg_for_season_all = _weighted_metric(overall_away, "xg_for", kickoff, args.half_life_season)
            away_xg_against_season_all = _weighted_metric(overall_away, "xg_against", kickoff, args.half_life_season)

            home_goals_for_form_all = _weighted_metric(overall_home, "goals_for", kickoff, args.half_life_form, limit=W)
            home_goals_against_form_all = _weighted_metric(overall_home, "goals_against", kickoff, args.half_life_form, limit=W)
            away_goals_for_form_all = _weighted_metric(overall_away, "goals_for", kickoff, args.half_life_form, limit=W)
            away_goals_against_form_all = _weighted_metric(overall_away, "goals_against", kickoff, args.half_life_form, limit=W)

            home_goals_for_season_all = _weighted_metric(overall_home, "goals_for", kickoff, args.half_life_season)
            home_goals_against_season_all = _weighted_metric(overall_home, "goals_against", kickoff, args.half_life_season)
            away_goals_for_season_all = _weighted_metric(overall_away, "goals_for", kickoff, args.half_life_season)
            away_goals_against_season_all = _weighted_metric(overall_away, "goals_against", kickoff, args.half_life_season)

            home_xg_for_std = _std([float(r["xg_for"]) for r in overall_home[:W] if r["xg_for"] is not None])
            home_xg_against_std = _std([float(r["xg_against"]) for r in overall_home[:W] if r["xg_against"] is not None])
            away_xg_for_std = _std([float(r["xg_for"]) for r in overall_away[:W] if r["xg_for"] is not None])
            away_xg_against_std = _std([float(r["xg_against"]) for r in overall_away[:W] if r["xg_against"] is not None])

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

            home_xg_for_form_all = _shrink_to_league(home_xg_for_form_all, league_avg_team_xg, len(overall_home[:W]))
            home_xg_against_form_all = _shrink_to_league(home_xg_against_form_all, league_avg_team_xg, len(overall_home[:W]))
            away_xg_for_form_all = _shrink_to_league(away_xg_for_form_all, league_avg_team_xg, len(overall_away[:W]))
            away_xg_against_form_all = _shrink_to_league(away_xg_against_form_all, league_avg_team_xg, len(overall_away[:W]))

            home_xg_for_season_all = _shrink_to_league(home_xg_for_season_all, league_avg_team_xg, len(overall_home))
            home_xg_against_season_all = _shrink_to_league(home_xg_against_season_all, league_avg_team_xg, len(overall_home))
            away_xg_for_season_all = _shrink_to_league(away_xg_for_season_all, league_avg_team_xg, len(overall_away))
            away_xg_against_season_all = _shrink_to_league(away_xg_against_season_all, league_avg_team_xg, len(overall_away))

            lambda_home = (home_xg_for * away_xg_against) / max(1e-6, league_avg_team_xg)
            lambda_away = (away_xg_for * home_xg_against) / max(1e-6, league_avg_team_xg)
            form_attack_factor_home = _ratio_clamp(home_xg_for_form_all, home_xg_for_season_all)
            form_attack_factor_away = _ratio_clamp(away_xg_for_form_all, away_xg_for_season_all)
            form_defense_factor_home = _ratio_clamp(home_xg_against_form_all, home_xg_against_season_all)
            form_defense_factor_away = _ratio_clamp(away_xg_against_form_all, away_xg_against_season_all)

            lambda_home = lambda_home * form_attack_factor_home * form_defense_factor_away
            lambda_away = lambda_away * form_attack_factor_away * form_defense_factor_home

            sched_factor_home = _schedule_factor(rest_home, matches_7d_home, matches_14d_home)
            sched_factor_away = _schedule_factor(rest_away, matches_7d_away, matches_14d_away)
            lambda_home = _clamp(lambda_home * sched_factor_home)
            lambda_away = _clamp(lambda_away * sched_factor_away)

            elo_home, elo_away = elo_map.get(understat_id, (1500.0, 1500.0))

            features = {
                "home_xg_for_form": float(home_xg_for_form),
                "home_xg_against_form": float(home_xg_against_form),
                "away_xg_for_form": float(away_xg_for_form),
                "away_xg_against_form": float(away_xg_against_form),
                "home_xg_for_season": float(home_xg_for_season),
                "home_xg_against_season": float(home_xg_against_season),
                "away_xg_for_season": float(away_xg_for_season),
                "away_xg_against_season": float(away_xg_against_season),
                "overall_xg_for_form_home": float(home_xg_for_form_all),
                "overall_xg_against_form_home": float(home_xg_against_form_all),
                "overall_xg_for_form_away": float(away_xg_for_form_all),
                "overall_xg_against_form_away": float(away_xg_against_form_all),
                "overall_xg_for_season_home": float(home_xg_for_season_all),
                "overall_xg_against_season_home": float(home_xg_against_season_all),
                "overall_xg_for_season_away": float(away_xg_for_season_all),
                "overall_xg_against_season_away": float(away_xg_against_season_all),
                "finishing_delta_form_home": float(home_goals_for_form_all - home_xg_for_form_all),
                "finishing_delta_form_away": float(away_goals_for_form_all - away_xg_for_form_all),
                "finishing_delta_season_home": float(home_goals_for_season_all - home_xg_for_season_all),
                "finishing_delta_season_away": float(away_goals_for_season_all - away_xg_for_season_all),
                "defense_delta_form_home": float(home_goals_against_form_all - home_xg_against_form_all),
                "defense_delta_form_away": float(away_goals_against_form_all - away_xg_against_form_all),
                "defense_delta_season_home": float(home_goals_against_season_all - home_xg_against_season_all),
                "defense_delta_season_away": float(away_goals_against_season_all - away_xg_against_season_all),
                "form_attack_factor_home": float(form_attack_factor_home),
                "form_attack_factor_away": float(form_attack_factor_away),
                "form_defense_factor_home": float(form_defense_factor_home),
                "form_defense_factor_away": float(form_defense_factor_away),
                "xg_for_form_std_home": float(home_xg_for_std),
                "xg_against_form_std_home": float(home_xg_against_std),
                "xg_for_form_std_away": float(away_xg_for_std),
                "xg_against_form_std_away": float(away_xg_against_std),
                "lambda_home": float(lambda_home),
                "lambda_away": float(lambda_away),
                "league_avg_team_xg": float(league_avg_team_xg),
                "home_samples": float(len(home_home)),
                "away_samples": float(len(away_away)),
                "samples_low": bool(samples_low),
                "form_weight_home": float(w_form_home),
                "form_weight_away": float(w_form_away),
                "elo_home": float(elo_home),
                "elo_away": float(elo_away),
                "elo_diff": float(elo_home - elo_away),
                "elo_k": float(args.elo_k),
                "elo_home_adv": float(args.elo_home_adv),
                "decay_half_life_form_days": float(args.half_life_form),
                "decay_half_life_season_days": float(args.half_life_season),
                "elo_half_life_days": float(args.elo_half_life),
                "rest_days_home": float(rest_home) if rest_home is not None else None,
                "rest_days_away": float(rest_away) if rest_away is not None else None,
                "matches_7d_home": float(matches_7d_home),
                "matches_7d_away": float(matches_7d_away),
                "matches_14d_home": float(matches_14d_home),
                "matches_14d_away": float(matches_14d_away),
                "schedule_factor_home": float(sched_factor_home),
                "schedule_factor_away": float(sched_factor_away),
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

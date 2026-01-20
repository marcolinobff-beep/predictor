import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import HTTPException
from app.db.sqlite import get_conn
from app.models.schemas import MatchRef, TeamRef, MatchContext, ModelOutputs
from app.core.ids import stable_hash
from app.core.config import settings
from app.services.lineup_service import compute_lineup_adjustment
from app.services.tactical_service import get_tactical_profile

def _row_to_matchref(row):
    return MatchRef(
        match_id=row["match_id"],
        competition=row["competition"],
        season=row["season"],
        kickoff_utc=datetime.fromisoformat(row["kickoff_utc"].replace("Z", "+00:00")),
        home=TeamRef(name=row["home"]),
        away=TeamRef(name=row["away"]),
        venue=row["venue"],
    )

def _get_latest_features(conn, match_id: str):
    feat_row = conn.execute(
        """
        SELECT features_version, features_json, created_at_utc
        FROM match_features
        WHERE match_id = ?
        ORDER BY created_at_utc DESC
        LIMIT 1
        """,
        (match_id,)
    ).fetchone()

    if not feat_row:
        return "none", {}, [
            "Nessuna feature trovata in locale (match_features vuoto per questo match)."
        ]

    return feat_row["features_version"], json.loads(feat_row["features_json"]), []


def _extract_schedule_factors(features: dict) -> dict:
    if not features:
        return {}
    keys = [
        "rest_days_home",
        "rest_days_away",
        "matches_7d_home",
        "matches_7d_away",
        "matches_14d_home",
        "matches_14d_away",
        "schedule_factor_home",
        "schedule_factor_away",
    ]
    out = {}
    for k in keys:
        if k in features and features[k] is not None:
            out[k] = features[k]
    return out


def _pct_delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None or baseline == 0:
        return None
    return (current / baseline) - 1.0


def _extract_form_info(features: dict) -> dict:
    if not features:
        return {}
    keys = [
        "overall_xg_for_form_home",
        "overall_xg_against_form_home",
        "overall_xg_for_form_away",
        "overall_xg_against_form_away",
        "overall_xg_for_season_home",
        "overall_xg_against_season_home",
        "overall_xg_for_season_away",
        "overall_xg_against_season_away",
        "finishing_delta_form_home",
        "finishing_delta_form_away",
        "finishing_delta_season_home",
        "finishing_delta_season_away",
        "defense_delta_form_home",
        "defense_delta_form_away",
        "defense_delta_season_home",
        "defense_delta_season_away",
        "form_attack_factor_home",
        "form_attack_factor_away",
        "form_defense_factor_home",
        "form_defense_factor_away",
    ]
    out = {}
    for k in keys:
        if k in features and features[k] is not None:
            out[k] = features[k]

    out["xg_for_delta_home"] = _pct_delta(
        features.get("overall_xg_for_form_home"),
        features.get("overall_xg_for_season_home"),
    )
    out["xg_against_delta_home"] = _pct_delta(
        features.get("overall_xg_against_form_home"),
        features.get("overall_xg_against_season_home"),
    )
    out["xg_for_delta_away"] = _pct_delta(
        features.get("overall_xg_for_form_away"),
        features.get("overall_xg_for_season_away"),
    )
    out["xg_against_delta_away"] = _pct_delta(
        features.get("overall_xg_against_form_away"),
        features.get("overall_xg_against_season_away"),
    )
    return out


_DATA_QUALITY_CACHE = {"path": None, "mtime": None, "data": None}


def _load_data_quality(path: str) -> dict | None:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return None
    if _DATA_QUALITY_CACHE["path"] == str(p) and _DATA_QUALITY_CACHE["mtime"] == mtime:
        return _DATA_QUALITY_CACHE["data"]
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    _DATA_QUALITY_CACHE.update({"path": str(p), "mtime": mtime, "data": data})
    return data


def _get_data_quality_for_league(league: str | None) -> dict | None:
    if not league:
        return None
    data = _load_data_quality(settings.data_quality_report_path)
    if not data:
        return None
    by_league = data.get("by_league") or {}
    entry = by_league.get(league)
    if not entry:
        return None
    features = entry.get("features") or {}
    tactical = entry.get("tactical") or {}
    lineups = entry.get("lineups") or {}
    upcoming = entry.get("upcoming") or {}
    return {
        "features_pct": float(features.get("pct", 0.0)) / 100.0,
        "tactical_pct": float(tactical.get("pct", 0.0)) / 100.0,
        "lineups_pct": float(lineups.get("pct", 0.0)) / 100.0,
        "missing_results_past": entry.get("missing_results_past"),
        "stale_or_missing_lineups": upcoming.get("stale_or_missing_lineups"),
    }


def _compute_model_confidence(
    features: dict,
    lineup_info: dict | None,
    form_info: dict | None,
    data_quality: dict | None,
) -> dict:
    lineup_cov = 0.0
    has_lineup = False
    if lineup_info:
        cov_h = lineup_info.get("coverage_home")
        cov_a = lineup_info.get("coverage_away")
        has_lineup = bool(lineup_info.get("lineup_source"))
        if cov_h is not None and cov_a is not None:
            lineup_cov = max(0.0, min(1.0, (float(cov_h) + float(cov_a)) / 2.0))

    league_avg = float(features.get("league_avg_team_xg", 1.35) or 1.35)
    stds = []
    for k in (
        "xg_for_form_std_home",
        "xg_against_form_std_home",
        "xg_for_form_std_away",
        "xg_against_form_std_away",
    ):
        v = features.get(k)
        if v is not None:
            stds.append(float(v))
    avg_std = sum(stds) / len(stds) if stds else 0.6
    stability = 1.0 - min(0.4, avg_std / (league_avg * 1.2))
    stability = max(0.55, min(0.95, stability))

    fin_penalty = 0.0
    if form_info:
        fin_h = abs(float(form_info.get("finishing_delta_form_home") or 0.0))
        fin_a = abs(float(form_info.get("finishing_delta_form_away") or 0.0))
        if max(fin_h, fin_a) >= 0.6:
            fin_penalty = 0.08

    lineup_penalty = 0.0
    if not has_lineup:
        lineup_penalty = 0.12
    elif lineup_cov < 0.2:
        lineup_penalty = 0.08
    elif lineup_cov < 0.35:
        lineup_penalty = 0.05

    stability_penalty = 0.0
    if stability < 0.55:
        stability_penalty = 0.08
    elif stability < 0.6:
        stability_penalty = 0.05

    source_bonus = 0.05 if has_lineup and lineup_cov >= 0.4 else 0.0

    quality_score = None
    quality_penalty = 0.0
    if data_quality:
        feats = data_quality.get("features_pct")
        tacts = data_quality.get("tactical_pct")
        lineups = data_quality.get("lineups_pct")
        if feats is not None and tacts is not None and lineups is not None:
            quality_score = (0.5 * feats) + (0.2 * tacts) + (0.3 * lineups)
            if quality_score < 0.5:
                quality_penalty = 0.08
            elif quality_score < 0.7:
                quality_penalty = 0.04

    score = 0.20 + 0.45 * lineup_cov + 0.30 * stability + source_bonus
    score -= (fin_penalty + lineup_penalty + stability_penalty + quality_penalty)
    score = max(0.15, min(0.85, score))

    return {
        "score": round(score, 3),
        "lineup_coverage": round(lineup_cov, 3),
        "form_stability": round(stability, 3),
        "finishing_penalty": round(fin_penalty, 3),
        "lineup_penalty": round(lineup_penalty, 3),
        "stability_penalty": round(stability_penalty, 3),
        "data_quality_score": round(quality_score, 3) if quality_score is not None else None,
        "data_quality_penalty": round(quality_penalty, 3),
        "data_quality": data_quality or {},
        "lineup_source": bool(has_lineup),
    }

def _availability_adjustment(conn, home: str, away: str, lookback_days: int = 10):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat().replace("+00:00", "Z")
    rows = conn.execute(
        """
        SELECT related_team, event_type, reliability_score
        FROM news_articles
        WHERE related_team IN (?, ?)
          AND published_at_utc >= ?
        """,
        (home, away, cutoff),
    ).fetchall()

    def impact(team_name: str) -> float:
        impact_sum = 0.0
        for r in rows:
            if r["related_team"] != team_name:
                continue
            et = (r["event_type"] or "").lower()
            if et not in ("injury", "suspension"):
                continue
            rel = float(r["reliability_score"] or 0.5)
            impact_sum += 0.03 * rel
        return min(0.18, impact_sum)

    return {
        "home_attack_penalty": impact(home),
        "away_attack_penalty": impact(away),
        "home_defense_penalty": impact(home),
        "away_defense_penalty": impact(away),
        "events_count": len(rows),
    }


def _apply_adjustment(features: dict, adj: dict):
    lam_h = float(features.get("lambda_home", 0.0))
    lam_a = float(features.get("lambda_away", 0.0))
    if lam_h <= 0 or lam_a <= 0:
        return features, None

    lam_h_adj = lam_h * (1.0 - adj["home_attack_penalty"]) * (1.0 + adj["away_defense_penalty"])
    lam_a_adj = lam_a * (1.0 - adj["away_attack_penalty"]) * (1.0 + adj["home_defense_penalty"])

    lam_h_adj = max(0.2, min(3.5, lam_h_adj))
    lam_a_adj = max(0.2, min(3.5, lam_a_adj))

    new_features = dict(features)
    new_features["lambda_home"] = lam_h_adj
    new_features["lambda_away"] = lam_a_adj

    return new_features, {
        "lambda_home_raw": lam_h,
        "lambda_away_raw": lam_a,
        "lambda_home_adj": lam_h_adj,
        "lambda_away_adj": lam_a_adj,
        "home_attack_penalty": adj["home_attack_penalty"],
        "away_attack_penalty": adj["away_attack_penalty"],
    }


def _apply_elo_adjustment(features: dict):
    elo_home = features.get("elo_home")
    elo_away = features.get("elo_away")
    if elo_home is None or elo_away is None:
        return features, None

    lam_h = float(features.get("lambda_home", 0.0))
    lam_a = float(features.get("lambda_away", 0.0))
    if lam_h <= 0 or lam_a <= 0:
        return features, None

    diff = float(elo_home) - float(elo_away)
    scale = max(-0.10, min(0.10, diff / 800.0))
    lam_h_adj = max(0.2, min(3.5, lam_h * (1.0 + scale)))
    lam_a_adj = max(0.2, min(3.5, lam_a * (1.0 - scale)))

    new_features = dict(features)
    new_features["lambda_home"] = lam_h_adj
    new_features["lambda_away"] = lam_a_adj

    return new_features, {
        "elo_home": float(elo_home),
        "elo_away": float(elo_away),
        "elo_diff": diff,
        "elo_scale": scale,
        "lambda_home_raw": lam_h,
        "lambda_away_raw": lam_a,
        "lambda_home_elo": lam_h_adj,
        "lambda_away_elo": lam_a_adj,
    }


def _season_start_from_label(label: str | None) -> int | None:
    if not label:
        return None
    s = str(label).strip()
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _apply_lineup_adjustment(features: dict, match_id: str, league: str, season_label: str, home: str, away: str):
    season_start = _season_start_from_label(season_label)
    if not season_start:
        return features, None

    lineup, adj = compute_lineup_adjustment(
        match_id=match_id,
        league=league,
        season_start=season_start,
        home_team=home,
        away_team=away,
    )
    if not adj:
        return features, None

    lam_h = float(features.get("lambda_home", 0.0))
    lam_a = float(features.get("lambda_away", 0.0))
    if lam_h <= 0 or lam_a <= 0:
        return features, None

    lam_h_adj = max(0.2, min(3.5, lam_h * (1.0 - adj.penalty_home)))
    lam_a_adj = max(0.2, min(3.5, lam_a * (1.0 - adj.penalty_away)))

    new_features = dict(features)
    new_features["lambda_home"] = lam_h_adj
    new_features["lambda_away"] = lam_a_adj

    return new_features, {
        "lineup_source": lineup.source if lineup else None,
        "lineup_confidence": lineup.confidence if lineup else None,
        "coverage_home": adj.coverage_home,
        "coverage_away": adj.coverage_away,
        "absence_share_home": adj.absence_share_home,
        "absence_share_away": adj.absence_share_away,
        "penalty_home": adj.penalty_home,
        "penalty_away": adj.penalty_away,
        "lambda_home_raw": lam_h,
        "lambda_away_raw": lam_a,
        "lambda_home_lineup": lam_h_adj,
        "lambda_away_lineup": lam_a_adj,
    }


def _delta_pct(delta: float | None, base: float | None) -> float | None:
    if delta is None or base in (None, 0):
        return None
    return delta / base


def _build_driver_insights(
    features: dict,
    schedule_factors: dict,
    form_info: dict | None,
    tactical: dict | None,
    availability_info: dict | None,
    elo_info: dict | None,
    lineup_info: dict | None,
) -> list[dict]:
    drivers: list[dict] = []
    lam_h = float(features.get("lambda_home", 0.0) or 0.0)
    lam_a = float(features.get("lambda_away", 0.0) or 0.0)

    def add_driver(key: str, label: str, delta_h: float | None, delta_a: float | None, note: str | None):
        if delta_h is None and delta_a is None:
            return
        delta_h = float(delta_h) if delta_h is not None else 0.0
        delta_a = float(delta_a) if delta_a is not None else 0.0
        if abs(delta_h) < 0.02 and abs(delta_a) < 0.02:
            return
        drivers.append({
            "key": key,
            "label": label,
            "home_delta": round(delta_h, 3),
            "away_delta": round(delta_a, 3),
            "home_delta_pct": round(_delta_pct(delta_h, lam_h) or 0.0, 3),
            "away_delta_pct": round(_delta_pct(delta_a, lam_a) or 0.0, 3),
            "note": note,
        })

    if availability_info:
        dh = availability_info.get("lambda_home_adj") - availability_info.get("lambda_home_raw")
        da = availability_info.get("lambda_away_adj") - availability_info.get("lambda_away_raw")
        add_driver("availability", "assenze/notizie", dh, da, "penalita news infortuni/squalifiche")

    if elo_info:
        dh = elo_info.get("lambda_home_elo") - elo_info.get("lambda_home_raw")
        da = elo_info.get("lambda_away_elo") - elo_info.get("lambda_away_raw")
        add_driver("elo", "rating elo", dh, da, "gap rating casa/trasferta")

    if lineup_info:
        dh = lineup_info.get("lambda_home_lineup") - lineup_info.get("lambda_home_raw")
        da = lineup_info.get("lambda_away_lineup") - lineup_info.get("lambda_away_raw")
        add_driver("lineup", "formazioni/assenze", dh, da, "penalita assenze da lineup")

    if form_info:
        def form_signal(xg_for_delta: float | None, xg_against_delta: float | None) -> float | None:
            vals = []
            if xg_for_delta is not None:
                vals.append(float(xg_for_delta))
            if xg_against_delta is not None:
                vals.append(-float(xg_against_delta))
            if not vals:
                return None
            signal = sum(vals) / len(vals)
            return max(-0.35, min(0.35, signal))

        sig_h = form_signal(form_info.get("xg_for_delta_home"), form_info.get("xg_against_delta_home"))
        sig_a = form_signal(form_info.get("xg_for_delta_away"), form_info.get("xg_against_delta_away"))
        dh = lam_h * sig_h * 0.25 if sig_h is not None else None
        da = lam_a * sig_a * 0.25 if sig_a is not None else None
        add_driver("form", "trend xG", dh, da, "trend forma vs stagione")

    if schedule_factors:
        rest_h = schedule_factors.get("rest_days_home")
        rest_a = schedule_factors.get("rest_days_away")
        m7_h = schedule_factors.get("matches_7d_home")
        m7_a = schedule_factors.get("matches_7d_away")
        fatigue_signal = 0.0
        if rest_h is not None and rest_a is not None:
            diff = float(rest_h) - float(rest_a)
            if diff >= 3:
                fatigue_signal += 0.04
            elif diff <= -3:
                fatigue_signal -= 0.04
        if m7_h is not None and m7_a is not None:
            diff = float(m7_h) - float(m7_a)
            if diff >= 2:
                fatigue_signal -= 0.03
            elif diff <= -2:
                fatigue_signal += 0.03
        if abs(fatigue_signal) >= 0.02:
            add_driver(
                "fatigue",
                "fatica/turnover",
                lam_h * fatigue_signal,
                lam_a * (-fatigue_signal),
                "rest days e carico gare recenti",
            )

    if tactical:
        tags = tactical.get("tags") or []
        tempo = tactical.get("tempo")
        tempo_signal = 0.0
        if tempo == "high":
            tempo_signal += 0.05
        elif tempo == "low":
            tempo_signal -= 0.05
        if "press_vs_possession" in tags:
            tempo_signal += 0.02
        tempo_signal = max(-0.08, min(0.08, tempo_signal))
        if abs(tempo_signal) >= 0.02:
            add_driver("tempo", "ritmo/stile", lam_h * tempo_signal, lam_a * tempo_signal, "ppda/possesso")

    drivers.sort(key=lambda d: max(abs(d["home_delta"]), abs(d["away_delta"])), reverse=True)
    return drivers


def get_match_context(home: str, away: str, competition: str, kickoff_utc: datetime):
    kickoff_iso = kickoff_utc.isoformat().replace("+00:00", "Z")
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM matches
            WHERE competition = ?
              AND home = ?
              AND away = ?
              AND kickoff_utc = ?
            """,
            (competition, home, away, kickoff_iso)
        ).fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="Match non trovato in SQLite. Inseriscilo o usa /v1/analyze_by_id."
            )

        match = _row_to_matchref(row)
        data_quality = _get_data_quality_for_league(match.competition)
        features_version, features, notes = _get_latest_features(conn, match.match_id)
        schedule_factors = _extract_schedule_factors(features)
        form_info = _extract_form_info(features)
        adj = _availability_adjustment(conn, match.home.name, match.away.name)
        adj_features, adj_info = _apply_adjustment(features, adj)
        elo_features, elo_info = _apply_elo_adjustment(adj_features)
        lineup_features, lineup_info = _apply_lineup_adjustment(
            elo_features,
            match_id=match.match_id,
            league=match.competition,
            season_label=match.season,
            home=match.home.name,
            away=match.away.name,
        )
        if adj_info and adj.get("events_count"):
            notes.append("AVAILABILITY_ADJUSTMENT_APPLIED")
        if elo_info:
            notes.append("ELO_ADJUSTMENT_APPLIED")
        if lineup_info:
            notes.append("LINEUP_ADJUSTMENT_APPLIED")
        if form_info:
            notes.append("FORM_FEATURES_AVAILABLE")
        tactical = get_tactical_profile(match.match_id, features)
        if tactical.get("tags"):
            notes.append("TACTICAL_TAGS_AVAILABLE")
        model_conf = _compute_model_confidence(features, lineup_info, form_info, data_quality)
        drivers = _build_driver_insights(
            features=lineup_features,
            schedule_factors=schedule_factors,
            form_info=form_info,
            tactical=tactical,
            availability_info=adj_info,
            elo_info=elo_info,
            lineup_info=lineup_info,
        )

        data_snapshot_id = stable_hash({
            "match": dict(row),
            "features_version": features_version,
            "features": features,
        })

        context = MatchContext(
            data_snapshot_id=data_snapshot_id,
            features_version=features_version,
            features=features,
            schedule_factors=schedule_factors,
            notes=notes,
        )

        model_outputs = ModelOutputs(
            model_version="context_only_v0",
            params={},
            derived={
                "availability": adj_info,
                "elo": elo_info,
                "lineup": lineup_info,
                "form": form_info,
                "tactical": tactical,
                "model_confidence": model_conf,
                "drivers": drivers,
            },
            warnings=(["FEATURES_MISSING"] if not features else []),
        )

        return {
            "match": match,
            "context": context,
            "model_outputs": model_outputs,
            "model_inputs": {"features": lineup_features, "features_version": features_version},
        }

def get_match_context_by_id(match_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="match_id non trovato in SQLite.")

        match = _row_to_matchref(row)
        data_quality = _get_data_quality_for_league(match.competition)
        features_version, features, notes = _get_latest_features(conn, match.match_id)
        schedule_factors = _extract_schedule_factors(features)
        form_info = _extract_form_info(features)
        adj = _availability_adjustment(conn, match.home.name, match.away.name)
        adj_features, adj_info = _apply_adjustment(features, adj)
        elo_features, elo_info = _apply_elo_adjustment(adj_features)
        lineup_features, lineup_info = _apply_lineup_adjustment(
            elo_features,
            match_id=match.match_id,
            league=match.competition,
            season_label=match.season,
            home=match.home.name,
            away=match.away.name,
        )
        if adj_info and adj.get("events_count"):
            notes.append("AVAILABILITY_ADJUSTMENT_APPLIED")
        if elo_info:
            notes.append("ELO_ADJUSTMENT_APPLIED")
        if lineup_info:
            notes.append("LINEUP_ADJUSTMENT_APPLIED")
        if form_info:
            notes.append("FORM_FEATURES_AVAILABLE")
        tactical = get_tactical_profile(match.match_id, features)
        if tactical.get("tags"):
            notes.append("TACTICAL_TAGS_AVAILABLE")
        model_conf = _compute_model_confidence(features, lineup_info, form_info, data_quality)
        drivers = _build_driver_insights(
            features=lineup_features,
            schedule_factors=schedule_factors,
            form_info=form_info,
            tactical=tactical,
            availability_info=adj_info,
            elo_info=elo_info,
            lineup_info=lineup_info,
        )

        data_snapshot_id = stable_hash({
            "match": dict(row),
            "features_version": features_version,
            "features": features,
        })

        context = MatchContext(
            data_snapshot_id=data_snapshot_id,
            features_version=features_version,
            features=features,
            schedule_factors=schedule_factors,
            notes=notes,
        )

        model_outputs = ModelOutputs(
            model_version="context_only_v0",
            params={},
            derived={
                "availability": adj_info,
                "elo": elo_info,
                "lineup": lineup_info,
                "form": form_info,
                "tactical": tactical,
                "model_confidence": model_conf,
                "drivers": drivers,
            },
            warnings=(["FEATURES_MISSING"] if not features else []),
        )

        return {
            "match": match,
            "context": context,
            "model_outputs": model_outputs,
            "model_inputs": {"features": lineup_features, "features_version": features_version},
        }

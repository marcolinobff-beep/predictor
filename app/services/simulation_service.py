from __future__ import annotations
from datetime import datetime, timezone
from uuid import uuid4
from typing import Dict, Any, List
import numpy as np
from fastapi import HTTPException

from app.models.schemas import SimulationOutputs, SimulationMeta
from app.core.config import settings
from app.core.calibration import load_calibration, apply_calibration, select_calibration, select_league_calibration
from app.core.calibration_policy import should_calibrate_1x2
from app.core.dc_params import get_rho
from app.core.ensemble import get_ensemble_weight
from app.core.gbm_light import load_model, predict_probs
from app.core.market_models import load_multinomial, implied_probs_from_odds_1x2
from app.services.web_intel_service import get_web_intel
from app.core.probabilities import match_probs
from app.core.temp_scale import apply_temp_scale_1x2, get_temp_scale
from app.db.sqlite import get_conn

def _ci95(p: float, n: int) -> Dict[str, float]:
    # Intervallo ~normale, sufficiente per MVP (derivato SOLO dalla simulazione)
    se = (p * (1.0 - p) / n) ** 0.5 if n > 0 else 0.0
    lo = max(0.0, p - 1.96 * se)
    hi = min(1.0, p + 1.96 * se)
    return {"lo": lo, "hi": hi, "se": se}


def _market_probs_from_webintel(match_id: str, kickoff_utc: datetime | None) -> Dict[str, float] | None:
    try:
        intel = get_web_intel(match_id, kickoff_utc)
        odds = intel.odds or []
        # estrai 1X2
        oh = od = oa = None
        for q in odds:
            if (q.market or "").upper() == "1X2":
                sel = (q.selection or "").upper()
                if sel == "HOME":
                    oh = q.odds_decimal
                elif sel == "DRAW":
                    od = q.odds_decimal
                elif sel == "AWAY":
                    oa = q.odds_decimal
        if oh and od and oa and oh > 1 and od > 1 and oa > 1:
            return implied_probs_from_odds_1x2(float(oh), float(od), float(oa))
    except Exception:
        return None
    return None


def _vector_from_feature_names(feature_names: List[str], features: Dict[str, Any], market_probs: Dict[str, float], dc_probs: Dict[str, float], residual_probs: Dict[str, float] | None) -> np.ndarray:
    """Costruisce X (1,D) a partire da un vocabolario di feature.
    Convenzioni:
      - market_* : probabilità implicite
      - dc_*     : probabilità DC
      - res_*    : output residual model
      - altre    : pescate da features dict (match_features JSON)
    """
    x = []
    for name in feature_names:
        if name.startswith("market_"):
            key = name.replace("market_", "")
            x.append(float(market_probs.get(key, 0.0)))
        elif name.startswith("dc_"):
            key = name.replace("dc_", "")
            x.append(float(dc_probs.get(key, 0.0)))
        elif name.startswith("res_"):
            key = name.replace("res_", "")
            x.append(float((residual_probs or {}).get(key, 0.0)))
        else:
            x.append(float(features.get(name, 0.0)))
    return np.array([x], dtype=float)

def _match_meta(match_id: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT season, kickoff_utc, competition FROM matches WHERE match_id = ?",
            (match_id,),
        ).fetchone()
    if not row:
        return None, None, None
    season = row["season"]
    kickoff = datetime.fromisoformat(str(row["kickoff_utc"]).replace("Z", "+00:00"))
    competition = row["competition"]
    return season, kickoff, competition


def _blend_probs(base: Dict[str, float], extra: Dict[str, float], weight: float) -> Dict[str, float]:
    w = max(0.0, min(1.0, weight))
    out = dict(base)
    for k, p in (extra or {}).items():
        if k in out:
            out[k] = (1.0 - w) * out[k] + w * float(p)
        else:
            out[k] = float(p)

    # normalize 1X2
    keys_1x2 = ["home_win", "draw", "away_win"]
    if all(k in out for k in keys_1x2):
        s = sum(out[k] for k in keys_1x2)
        if s > 0:
            for k in keys_1x2:
                out[k] = out[k] / s

    # normalize OU 2.5
    if "over_2_5" in out and "under_2_5" in out:
        s = out["over_2_5"] + out["under_2_5"]
        if s > 0:
            out["over_2_5"] = out["over_2_5"] / s
            out["under_2_5"] = out["under_2_5"] / s

    # normalize BTTS
    if "btts_yes" in out and "btts_no" in out:
        s = out["btts_yes"] + out["btts_no"]
        if s > 0:
            out["btts_yes"] = out["btts_yes"] / s
            out["btts_no"] = out["btts_no"] / s

    return out

def run_match_simulation(
    match_id: str,
    data_snapshot_id: str,
    n_sims: int,
    seed: int,
    model_version: str,
    model_inputs: Dict[str, Any],
) -> SimulationOutputs:
    if n_sims < 1000:
        raise HTTPException(status_code=400, detail="n_sims deve essere >= 1000.")

    features = (model_inputs or {}).get("features") or {}
    if "lambda_home" not in features or "lambda_away" not in features:
        raise HTTPException(
            status_code=400,
            detail="Feature mancanti per la simulazione: servono lambda_home e lambda_away (in locale).",
        )

    lam_h = float(features["lambda_home"])
    lam_a = float(features["lambda_away"])
    if lam_h <= 0 or lam_a <= 0:
        raise HTTPException(status_code=400, detail="lambda_home/lambda_away devono essere > 0.")

    rng = np.random.default_rng(seed)
    home_goals = rng.poisson(lam=lam_h, size=n_sims)
    away_goals = rng.poisson(lam=lam_a, size=n_sims)

    # Prob mercati base (derivati SOLO dagli esiti MC)
    p_home = float(np.mean(home_goals > away_goals))
    p_draw = float(np.mean(home_goals == away_goals))
    p_away = float(np.mean(home_goals < away_goals))

    total_goals = home_goals + away_goals
    p_over_2_5 = float(np.mean(total_goals >= 3))
    p_under_2_5 = float(np.mean(total_goals <= 2))
    p_btts_yes = float(np.mean((home_goals > 0) & (away_goals > 0)))
    p_btts_no = 1.0 - p_btts_yes

    probs_mc = {
        "home_win": p_home,
        "draw": p_draw,
        "away_win": p_away,
        "over_2_5": p_over_2_5,
        "under_2_5": p_under_2_5,
        "btts_yes": p_btts_yes,
        "btts_no": p_btts_no,
    }

    season_label, kickoff_utc, competition = _match_meta(match_id)
    rho = get_rho(settings.dc_params_path, competition)
    probs_dc = match_probs(lam_h, lam_a, cap=8, rho=rho)

    # --- STEP2/3: MARKET + RESIDUAL + STACK (se disponibili) ---
    kickoff_dt = None
    try:
        kickoff_dt = model_inputs.get("kickoff_utc")  # può non esserci
    except Exception:
        kickoff_dt = None

    market_probs = _market_probs_from_webintel(match_id, kickoff_utc)
    residual_model = load_multinomial(settings.residual_model_path)
    residual_probs = None
    if market_probs and residual_model:
        Xr = _vector_from_feature_names(residual_model.feature_names, features, market_probs, probs_dc, None)
        pr = residual_model.predict_proba(Xr)[0]
        # mapping: classes expected HOME/DRAW/AWAY
        cmap = {c: pr[i] for i, c in enumerate(residual_model.classes)}
        residual_probs = {
            "home_win": float(cmap.get("HOME", 0.0)),
            "draw": float(cmap.get("DRAW", 0.0)),
            "away_win": float(cmap.get("AWAY", 0.0)),
        }

    stack_model = load_multinomial(settings.stack_model_path)
    if market_probs and stack_model:
        # input stacking: market + dc + residual (se manca, 0)
        Xs = _vector_from_feature_names(stack_model.feature_names, features, market_probs, probs_dc, residual_probs)
        ps = stack_model.predict_proba(Xs)[0]
        cmap = {c: ps[i] for i, c in enumerate(stack_model.classes)}
        probs = {
            "home_win": float(cmap.get("HOME", probs_dc.get("home_win", 0.0))),
            "draw": float(cmap.get("DRAW", probs_dc.get("draw", 0.0))),
            "away_win": float(cmap.get("AWAY", probs_dc.get("away_win", 0.0))),
            # mantieni gli altri mercati da DC (o dal blend successivo)
            **{k: v for k, v in probs_dc.items() if k not in ("home_win", "draw", "away_win")},
        }
    else:
        probs = dict(probs_dc)
    probs = probs_dc

    gbm_model = load_model(settings.gbm_model_path, competition)
    gbm_probs = predict_probs(gbm_model, features) if gbm_model else {}
    ensemble_weight = None
    if gbm_probs:
        ensemble_weight = get_ensemble_weight(
            settings.ensemble_weights_path,
            competition,
            settings.ensemble_weight,
        )
        probs = _blend_probs(probs_dc, gbm_probs, ensemble_weight)

    temp_scale = get_temp_scale(settings.temp_scale_path, competition)
    if temp_scale:
        probs = apply_temp_scale_1x2(probs, temp_scale)

    cal = load_calibration(settings.calibration_by_season_path) or load_calibration(settings.calibration_path)
    cal = select_league_calibration(cal, competition)
    cal_sel = select_calibration(cal, season_label, kickoff_utc) if cal else None
    if cal_sel:
        calibrated = apply_calibration(probs, cal_sel)
        if not should_calibrate_1x2(settings.calibration_policy_path, competition, default=True):
            for key in ("home_win", "draw", "away_win"):
                calibrated[key] = probs.get(key, calibrated.get(key))
        probs = calibrated

    # Diagnostica convergenza super semplice: differenza tra metà 1 e metà 2
    half = n_sims // 2
    if half >= 1000:
        p_home_1 = float(np.mean(home_goals[:half] > away_goals[:half]))
        p_home_2 = float(np.mean(home_goals[half:] > away_goals[half:]))
        conv_delta = abs(p_home_1 - p_home_2)
    else:
        conv_delta = None

    # Scoreline topK (solo display; non “inventa” nulla)
    # Per evitare payload enormi: cap goal a 8 nel report display
    cap = 8
    h_cap = np.minimum(home_goals, cap)
    a_cap = np.minimum(away_goals, cap)
    key = h_cap * (cap + 1) + a_cap
    counts = np.bincount(key, minlength=(cap + 1) * (cap + 1))
    total = counts.sum()

    topk = 12
    idx = np.argsort(counts)[::-1][:topk]
    scoreline_topk: List[Dict[str, Any]] = []
    for k in idx:
        c = int(counts[k])
        if c == 0:
            continue
        hg = int(k // (cap + 1))
        ag = int(k % (cap + 1))
        scoreline_topk.append({"home_goals": hg, "away_goals": ag, "p": float(c / total)})

    intervals = {name: _ci95(p, n_sims) for name, p in probs.items()}

    meta = SimulationMeta(
        simulation_id=str(uuid4()),
        n_sims=n_sims,
        seed=seed,
        model_version=model_version,
        timestamp_utc=datetime.now(timezone.utc),
        data_snapshot_id=data_snapshot_id,
    )

    return SimulationOutputs(
        meta=meta,
        scoreline_topk=scoreline_topk,
        probs=probs,
        intervals=intervals,
        diagnostics={
            "lambda_home": lam_h,
            "lambda_away": lam_a,
            "convergence_delta_home_win": conv_delta,
            "calibration_applied": bool(cal_sel),
            "calibration_version": cal_sel.get("version") if cal_sel else (cal.get("version") if cal else None),
            "calibration_1x2_enabled": should_calibrate_1x2(
                settings.calibration_policy_path, competition, default=True
            ),
            "temp_scale_1x2": temp_scale,
            "prob_source": ("ensemble_gbm" if gbm_probs else "dc_poisson"),
            "dc_rho": rho,
            "ensemble_weight": ensemble_weight if gbm_probs else None,
            "probs_gbm_home_win": gbm_probs.get("home_win") if gbm_probs else None,
            "probs_mc_home_win": probs_mc["home_win"],
        },
    )

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

from app.core.config import settings


@dataclass
class KpiStatus:
    status: str
    season: Optional[str]
    phase: Optional[str]
    logloss_1x2: Optional[float]
    brier_1x2: Optional[float]
    roi_1x2: Optional[float]
    picks_1x2: Optional[int]
    brier_by_market: Optional[Dict[str, float]]
    logloss_by_market: Optional[Dict[str, float]]
    roi_by_market: Optional[Dict[str, object]]
    reasons: list[str]


_CACHE = {"loaded_at": None, "data": None}


def _load_report(path: Path, max_age_minutes: int = 10) -> Optional[dict]:
    now = datetime.now(timezone.utc)
    loaded_at = _CACHE["loaded_at"]
    if loaded_at and (now - loaded_at) < timedelta(minutes=max_age_minutes):
        return _CACHE["data"]
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    _CACHE["loaded_at"] = now
    _CACHE["data"] = data
    return data


def _season_label(season_value: Optional[str]) -> Optional[str]:
    if not season_value:
        return None
    s = str(season_value).strip()
    if "/" in s:
        return s
    if len(s) >= 4 and s[:4].isdigit():
        start = int(s[:4])
        return f"{start}/{str(start + 1)[-2:]}"
    return None


def _brier_1x2_avg(brier: Dict[str, float]) -> Optional[float]:
    if not brier:
        return None
    parts = [brier.get("home_win"), brier.get("draw"), brier.get("away_win")]
    if any(v is None for v in parts):
        return None
    return sum(parts) / 3.0


def _phase_for_date(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    m = dt.month
    if m in (8, 9, 10):
        return "early"
    if m in (11, 12, 1, 2):
        return "mid"
    return "late"


def get_kpi_status(
    competition: str,
    season: Optional[str],
    kickoff_utc: Optional[datetime] = None,
    thresholds: Optional[Dict[str, float]] = None,
) -> Optional[KpiStatus]:
    path = Path(settings.kpi_report_path)
    report = _load_report(path)
    if not report:
        return None

    league_report = report
    if isinstance(report, dict) and "by_league" in report:
        league_report = (report.get("by_league") or {}).get(competition)
        if league_report is None:
            return None
    elif isinstance(report, dict) and report.get("league") and report.get("league") != competition:
        return None

    season_label = _season_label(season)
    by_season = (league_report or {}).get("by_season", {}) if isinstance(league_report, dict) else {}
    season_report = by_season.get(season_label) if season_label else None
    if not season_report:
        return None

    phase = _phase_for_date(kickoff_utc)
    phase_report = None
    if phase:
        phase_report = (season_report.get("by_phase") or {}).get(phase)

    target_report = phase_report or season_report
    brier = target_report.get("brier", {})
    brier_by_market = target_report.get("brier_by_market", {})
    logloss = target_report.get("logloss_1x2")
    logloss_by_market = target_report.get("logloss_by_market", {})
    brier_1x2 = _brier_1x2_avg(brier)
    roi_1x2 = None
    picks_1x2 = None
    roi_by_market = target_report.get("roi_by_market") or {}
    roi_info = roi_by_market.get("1X2")
    if isinstance(roi_info, dict):
        roi_1x2 = roi_info.get("roi")
        picks_1x2 = roi_info.get("picks")

    t = thresholds or {
        "max_logloss_1x2": 1.12,
        "max_brier_1x2": 0.26,
        "min_roi_1x2": -0.03,
        "min_roi_picks": 40,
    }

    reasons: list[str] = []
    status = "OK"

    if logloss is not None and logloss > t["max_logloss_1x2"]:
        reasons.append("KPI_LOGLOSS_HIGH")
        status = "BLOCK"
    if brier_1x2 is not None and brier_1x2 > t["max_brier_1x2"]:
        reasons.append("KPI_BRIER_HIGH")
        status = "BLOCK"

    if (
        roi_1x2 is not None
        and picks_1x2 is not None
        and picks_1x2 >= t["min_roi_picks"]
        and roi_1x2 < t["min_roi_1x2"]
    ):
        reasons.append("KPI_ROI_NEGATIVE")
        if status != "BLOCK":
            status = "WARN"

    return KpiStatus(
        status=status,
        season=season_label,
        phase=phase,
        logloss_1x2=logloss,
        brier_1x2=brier_1x2,
        roi_1x2=roi_1x2,
        picks_1x2=picks_1x2,
        brier_by_market=brier_by_market,
        logloss_by_market=logloss_by_market,
        roi_by_market=roi_by_market,
        reasons=reasons,
    )

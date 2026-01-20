from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from app.core.config import settings
from app.db.sqlite import get_conn


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _load_kpi_report() -> Dict[str, Any]:
    try:
        with open(settings.kpi_report_path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def get_dashboard_kpis() -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    week_start = now - timedelta(days=7)
    month_start = now - timedelta(days=30)

    rows: list[Any] = []
    competitions: list[Any] = []
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT created_at_utc, payload_json
                FROM audit_log
                WHERE created_at_utc >= ?
                ORDER BY created_at_utc DESC
                """,
                (month_start.isoformat().replace("+00:00", "Z"),),
            ).fetchall()

            competitions = conn.execute(
                "SELECT DISTINCT competition FROM matches ORDER BY competition",
            ).fetchall()
    except sqlite3.Error:
        rows = []
        competitions = []

    total_today = 0
    total_week = 0
    bets_suggested = 0
    edges = []
    evs = []

    for r in rows:
        created = _parse_dt(r["created_at_utc"])
        if not created:
            continue
        if created >= day_start:
            total_today += 1
        if created >= week_start:
            total_week += 1
        try:
            payload = json.loads(r["payload_json"])
        except Exception:
            continue
        recs = payload.get("recommendations") or []
        if created >= week_start:
            bets_suggested += len(recs)
        for rec in recs:
            edge = rec.get("expected_edge")
            ev = rec.get("expected_ev_per_unit")
            if edge is not None:
                edges.append(float(edge))
            if ev is not None:
                evs.append(float(ev))

    kpi = _load_kpi_report()
    accuracy = 0.0
    roi_last_30 = 0.0
    by_league = (kpi.get("by_league") or {}) if isinstance(kpi, dict) else {}
    brier_vals = []
    roi_vals = []
    for league_data in by_league.values():
        seasons = (league_data.get("by_season") or {})
        if not seasons:
            continue
        latest = sorted(seasons.keys())[-1]
        season = seasons.get(latest) or {}
        brier = season.get("brier") or {}
        brier_vals.extend([
            brier.get("home_win", 0.0),
            brier.get("draw", 0.0),
            brier.get("away_win", 0.0),
        ])
        roi_by_market = season.get("roi_by_market") or {}
        for entry in roi_by_market.values():
            roi_vals.append(float(entry.get("roi", 0.0)))

    if brier_vals:
        accuracy = max(0.0, 1.0 - (_avg(brier_vals)))
    if roi_vals:
        roi_last_30 = _avg(roi_vals)

    competitions_covered: list[str] = []
    for row in competitions:
        if not row:
            continue
        comp = None
        if isinstance(row, dict):
            comp = row.get("competition")
        else:
            try:
                comp = row["competition"]
            except Exception:
                try:
                    comp = row[0]
                except Exception:
                    comp = None
        if comp:
            competitions_covered.append(str(comp))

    return {
        "total_analyses_today": total_today,
        "total_analyses_week": total_week,
        "bets_suggested": bets_suggested,
        "avg_edge": _avg(edges),
        "avg_ev": _avg(evs),
        "model_accuracy": accuracy,
        "roi_last_30_days": roi_last_30,
        "competitions_covered": competitions_covered,
        "last_updated": now.isoformat().replace("+00:00", "Z"),
    }

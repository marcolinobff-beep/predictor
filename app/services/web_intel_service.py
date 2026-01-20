from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, List

from app.db.sqlite import get_conn
from app.models.schemas import WebIntel, WebSource, WebOddsQuote, NewsItem
from app.core.text_utils import clean_person_name
from app.services.lineup_service import get_latest_lineup
from app.services.lineup_refresh_service import ensure_lineups_for_match


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_web_intel(match_id: str, kickoff_utc: Optional[datetime]) -> WebIntel:
    """Carica quote da SQLite.

    Regole:
    - Preferisce sempre quote PRE-kickoff (<= kickoff_utc).
    - Se non esistono quote pre-kickoff, usa comunque l'ultimo batch disponibile
      ma aggiunge un warning 'ODDS_POST_KICKOFF_ONLY' (per debug) e il report
      dovrebbe evitare raccomandazioni betting.
    """
    with get_conn() as conn:
        odds: List[WebOddsQuote] = []
        sources: List[WebSource] = []
        news: List[NewsItem] = []
        notes: List[str] = []
        predicted_lineups: List[dict] = []

        kickoff_dt = _as_utc(kickoff_utc) if kickoff_utc else None

        try:
            notes.extend(ensure_lineups_for_match(match_id, kickoff_dt))
        except Exception:
            notes.append("LINEUPS_REFRESH_ERROR")

        # 1) prova: ultimo batch con quote pre-kickoff
        row = None
        if kickoff_dt is not None:
            row = conn.execute(
                """
                SELECT batch_id, retrieved_at_utc
                FROM odds_quotes
                WHERE match_id = ?
                  AND retrieved_at_utc <= ?
                ORDER BY retrieved_at_utc DESC
                LIMIT 1
                """,
                (match_id, kickoff_dt.isoformat().replace("+00:00", "Z")),
            ).fetchone()

        post_kickoff_only = False

        # 2) fallback: ultimo batch in assoluto
        if row is None:
            row = conn.execute(
                """
                SELECT batch_id, retrieved_at_utc
                FROM odds_quotes
                WHERE match_id = ?
                ORDER BY retrieved_at_utc DESC
                LIMIT 1
                """,
                (match_id,),
            ).fetchone()
            if row is not None and kickoff_dt is not None:
                try:
                    last_ts = datetime.fromisoformat(str(row["retrieved_at_utc"]).replace("Z", "+00:00"))
                    last_ts = _as_utc(last_ts)
                    if last_ts > kickoff_dt:
                        post_kickoff_only = True
                except Exception:
                    # se parsing fallisce, non blocchiamo qui; verrà gestito dal gate stale/other
                    pass

        if row and row["batch_id"]:
            batch_id = row["batch_id"]

            qrows = conn.execute(
                """
                SELECT *
                FROM odds_quotes
                WHERE match_id = ? AND batch_id = ?
                """,
                (match_id, batch_id),
            ).fetchall()

            source_scores = {}
            for r in qrows:
                odds.append(
                    WebOddsQuote(
                        bookmaker=r["bookmaker"],
                        market=r["market"],
                        selection=r["selection"],
                        odds_decimal=float(r["odds_decimal"]),
                        retrieved_at_utc=datetime.fromisoformat(str(r["retrieved_at_utc"]).replace("Z", "+00:00")),
                    )
                )
                source_scores.setdefault(r["source_id"], []).append(float(r["reliability_score"]))

            for sid, scores in source_scores.items():
                sources.append(
                    WebSource(
                        source_id=f"odds_local:{sid}",
                        fetched_at_utc=datetime.now(timezone.utc),
                        cache_hit=True,
                        ttl_seconds=300,
                        reliability_score=sum(scores) / len(scores),
                        raw_ref=f"sqlite:odds_quotes batch_id={batch_id}",
                    )
                )

            notes.append(f"Loaded {len(odds)} odds from SQLite (batch_id={batch_id}).")
            if kickoff_dt is not None:
                notes.append(f"Kickoff_utc={kickoff_dt.isoformat()}")
            if post_kickoff_only:
                notes.append("ODDS_POST_KICKOFF_ONLY")
                notes.append("WARNING: non risultano quote pre-kickoff in DB per questo match. Uso quote post-kickoff solo per debug.")

            web_snapshot_id = f"web_local_{match_id}_{batch_id}"
        else:
            sources = [
                WebSource(
                    source_id="odds_local:none",
                    fetched_at_utc=datetime.now(timezone.utc),
                    cache_hit=True,
                    ttl_seconds=0,
                    reliability_score=0.0,
                    raw_ref=None,
                )
            ]
            notes.append("Nessuna quota trovata in SQLite (odds_quotes vuota).") 
            web_snapshot_id = f"web_local_{match_id}_empty"

        # probable lineups (se presenti)
        def _clean_list(values: List[str]) -> List[str]:
            out: List[str] = []
            for v in values:
                name = clean_person_name(v)
                if name:
                    out.append(name)
            return out

        lineup = get_latest_lineup(match_id)
        if lineup:
            predicted_lineups.append({
                "source": lineup.source,
                "confidence": lineup.confidence,
                "fetched_at_utc": lineup.fetched_at_utc,
                "home_players": _clean_list(lineup.home_players),
                "away_players": _clean_list(lineup.away_players),
                "home_absences": _clean_list(lineup.home_absences),
                "away_absences": _clean_list(lineup.away_absences),
            })
            notes.append(f"Loaded probable lineup from {lineup.source}.")

        # news locali (se presenti)
        try:
            team_names = []
            row_match = conn.execute(
                "SELECT home, away FROM matches WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            if row_match:
                team_names = [row_match["home"], row_match["away"]]

            if team_names:
                nrows = conn.execute(
                    """
                    SELECT news_id, source, title, url, published_at_utc, reliability_score,
                           related_match_id, related_team, related_player, event_type, summary
                    FROM news_articles
                    WHERE related_match_id = ?
                       OR related_team IN (?, ?)
                    ORDER BY published_at_utc DESC
                    LIMIT 20
                    """,
                    (match_id, team_names[0], team_names[1]),
                ).fetchall()
            else:
                nrows = conn.execute(
                    """
                    SELECT news_id, source, title, url, published_at_utc, reliability_score,
                           related_match_id, related_team, related_player, event_type, summary
                    FROM news_articles
                    WHERE related_match_id = ?
                    ORDER BY published_at_utc DESC
                    LIMIT 20
                    """,
                    (match_id,),
                ).fetchall()
            for r in nrows:
                news.append(
                    NewsItem(
                        news_id=r["news_id"],
                        source=r["source"],
                        title=r["title"],
                        url=r["url"],
                        published_at_utc=datetime.fromisoformat(str(r["published_at_utc"]).replace("Z", "+00:00")),
                        reliability_score=float(r["reliability_score"]) if r["reliability_score"] is not None else 0.0,
                        related_match_id=r["related_match_id"],
                        related_team=r["related_team"],
                        related_player=r["related_player"],
                        event_type=r["event_type"],
                        summary=r["summary"],
                    )
                )
            if nrows:
                notes.append(f"Loaded {len(nrows)} news from SQLite.")
        except Exception:
            notes.append("NEWS_TABLE_MISSING_OR_ERROR")

    return WebIntel(
        web_snapshot_id=web_snapshot_id,
        sources=sources,
        odds=odds,
        injuries=[],
        predicted_lineups=predicted_lineups,
        news=news,
        weather=None,
        notes=notes,
    )

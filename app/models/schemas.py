from __future__ import annotations
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, conint, confloat
from datetime import datetime, date

ReportStatus = Literal["OK", "MISSING_DATA", "ERROR"]

class TeamRef(BaseModel):
    name: str
    team_id: Optional[str] = None

class MatchRef(BaseModel):
    match_id: str
    competition: str
    season: Optional[str] = None
    kickoff_utc: datetime
    home: TeamRef
    away: TeamRef
    venue: Optional[str] = None

class MatchContext(BaseModel):
    data_snapshot_id: str
    features_version: str
    features: Dict[str, float] = Field(default_factory=dict)
    schedule_factors: Dict[str, Any] = Field(default_factory=dict)
    notes: List[str] = Field(default_factory=list)

class WebSource(BaseModel):
    source_id: str
    fetched_at_utc: datetime
    cache_hit: bool
    ttl_seconds: int
    reliability_score: confloat(ge=0.0, le=1.0)
    raw_ref: Optional[str] = None

class WebOddsQuote(BaseModel):
    bookmaker: str
    market: str
    selection: str
    odds_decimal: confloat(gt=1.0)
    retrieved_at_utc: datetime

class NewsItem(BaseModel):
    news_id: str
    source: str
    title: str
    url: Optional[str] = None
    published_at_utc: datetime
    reliability_score: confloat(ge=0.0, le=1.0) = 0.0
    related_match_id: Optional[str] = None
    related_team: Optional[str] = None
    related_player: Optional[str] = None
    event_type: Optional[str] = None
    summary: Optional[str] = None

class WebIntel(BaseModel):
    web_snapshot_id: str
    sources: List[WebSource]
    odds: List[WebOddsQuote] = Field(default_factory=list)
    injuries: List[Dict[str, Any]] = Field(default_factory=list)
    predicted_lineups: List[Dict[str, Any]] = Field(default_factory=list)
    news: List[NewsItem] = Field(default_factory=list)
    weather: Optional[Dict[str, Any]] = None
    notes: List[str] = Field(default_factory=list)

class ModelOutputs(BaseModel):
    model_version: str
    params: Dict[str, Any] = Field(default_factory=dict)
    derived: Dict[str, Any] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)

class SimulationMeta(BaseModel):
    simulation_id: str
    n_sims: conint(ge=1000)
    seed: conint(ge=0)
    model_version: str
    timestamp_utc: datetime
    data_snapshot_id: str

class SimulationOutputs(BaseModel):
    meta: SimulationMeta
    scoreline_topk: List[Dict[str, Any]] = Field(default_factory=list)
    probs: Dict[str, float] = Field(default_factory=dict)
    intervals: Dict[str, Any] = Field(default_factory=dict)
    diagnostics: Dict[str, Any] = Field(default_factory=dict)

class MarketEvalItem(BaseModel):
    market: str
    selection: str
    bookmaker: str
    odds_decimal: float
    fair_prob: Optional[confloat(ge=0.0, le=1.0)] = None
    fair_odds: Optional[confloat(gt=1.0)] = None
    edge: Optional[float] = None
    ev_per_unit: Optional[float] = None
    implied_prob: Optional[float] = None        # prob da quote normalizzate
    market_overround: Optional[float] = None    # somma(1/odds) per market
    consensus_odds: Optional[float] = None
    line_value_pct: Optional[float] = None
    bookmakers_count: Optional[int] = None
    uncertainty_flag: bool = False
    reasons: List[str] = Field(default_factory=list)
    confidence: Optional[confloat(ge=0.0, le=1.0)] = None

class Recommendation(BaseModel):
    bet_id: str
    market: str
    selection: str
    bookmaker: str
    odds_decimal: float
    stake_fraction: confloat(ge=0.0, le=0.05)
    expected_edge: float
    expected_ev_per_unit: float
    confidence: Optional[confloat(ge=0.0, le=1.0)] = None
    line_value_pct: Optional[float] = None
    consensus_odds: Optional[float] = None
    rationale: List[str] = Field(default_factory=list)

class PlayerProjection(BaseModel):
    player_id: str
    player_name: str
    team: Optional[str] = None
    position: Optional[str] = None
    season: int
    games: Optional[int] = None
    minutes: Optional[int] = None
    xg: Optional[float] = None
    xa: Optional[float] = None
    shots: Optional[int] = None
    key_passes: Optional[int] = None
    xg_per90: Optional[float] = None
    xa_per90: Optional[float] = None
    shots_per90: Optional[float] = None
    key_passes_per90: Optional[float] = None
    gi_per90: Optional[float] = None
    xg_share: Optional[float] = None
    xa_share: Optional[float] = None
    gi_share: Optional[float] = None
    expected_xg: Optional[float] = None
    expected_xa: Optional[float] = None
    expected_gi: Optional[float] = None

class PlayerProjectionReport(BaseModel):
    home: List[PlayerProjection] = Field(default_factory=list)
    away: List[PlayerProjection] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)

class NoBet(BaseModel):
    reason_codes: List[str]
    explanation: List[str]

class AuditToolRun(BaseModel):
    tool_name: str
    status: Literal["OK", "SKIPPED", "ERROR"]
    started_at_utc: datetime
    ended_at_utc: datetime
    input_ref: Optional[str] = None
    output_ref: Optional[str] = None
    error: Optional[str] = None

class Audit(BaseModel):
    request_id: str
    generated_at_utc: datetime
    service_version: str
    tool_runs: List[AuditToolRun]
    data_snapshot_id: Optional[str] = None
    web_snapshot_id: Optional[str] = None
    simulation_id: Optional[str] = None

class MatchAnalysisReport(BaseModel):
    status: ReportStatus
    match: MatchRef
    match_context: Optional[MatchContext] = None
    web_intel: Optional[WebIntel] = None
    model_outputs: Optional[ModelOutputs] = None
    simulation_outputs: Optional[SimulationOutputs] = None
    player_projections: Optional[PlayerProjectionReport] = None
    market_evaluation: List[MarketEvalItem] = Field(default_factory=list)
    recommendations: List[Recommendation] = Field(default_factory=list, max_length=3)
    no_bet: Optional[NoBet] = None
    audit: Audit
    errors: List[str] = Field(default_factory=list)

class SlatePick(BaseModel):
    match_id: str
    competition: str
    kickoff_utc: datetime
    home: str
    away: str
    market: str
    selection: str
    bookmaker: str
    odds_decimal: float
    stake_fraction: confloat(ge=0.0, le=0.05)
    expected_edge: float
    expected_ev_per_unit: float

class SlateMultiple(BaseModel):
    difficulty: Literal["safe", "medium", "risky"]
    legs: List[SlatePick] = Field(default_factory=list)
    total_odds: Optional[float] = None

class SlateReport(BaseModel):
    date_utc: date
    competition: Optional[str] = None
    picks: List[SlatePick] = Field(default_factory=list)
    multiples: List[SlateMultiple] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)

class ChatResponse(BaseModel):
    answer: str
    resolved_intent: str
    warnings: List[str] = Field(default_factory=list)
    report: Optional[MatchAnalysisReport] = None
    slate: Optional[SlateReport] = None
    session_id: Optional[str] = None

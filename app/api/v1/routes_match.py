from fastapi import APIRouter
from pydantic import BaseModel
from datetime import datetime, timezone, date
from app.models.schemas import MatchAnalysisReport, MatchRef, TeamRef, SlateReport
from app.services.report_service import analyze_match

router = APIRouter(tags=["match"])

class AnalyzeRequest(BaseModel):
    home: str
    away: str
    competition: str
    kickoff_utc: datetime
    n_sims: int = 200000
    seed: int = 42
    bankroll: float = 1000.0

    def to_match_ref_fallback(self) -> MatchRef:
        return MatchRef(
            match_id="UNKNOWN",
            competition=self.competition,
            kickoff_utc=self.kickoff_utc,
            home=TeamRef(name=self.home),
            away=TeamRef(name=self.away),
        )

@router.post("/analyze", response_model=MatchAnalysisReport)
def analyze(req: AnalyzeRequest) -> MatchAnalysisReport:
    return analyze_match(req)

@router.get("/health")
def health():
    return {"status": "ok", "time_utc": datetime.now(timezone.utc).isoformat()}

class AnalyzeByIdRequest(BaseModel):
    match_id: str
    n_sims: int = 200000
    seed: int = 42
    bankroll: float = 1000.0

@router.post("/analyze_by_id", response_model=MatchAnalysisReport)
def analyze_by_id(req: AnalyzeByIdRequest) -> MatchAnalysisReport:
    from app.services.report_service import analyze_match_by_id
    return analyze_match_by_id(req)

class SlateRequest(BaseModel):
    date_utc: date
    competition: str | None = None
    n_sims: int = 50000
    seed: int = 42
    bankroll: float = 1000.0
    max_picks_per_match: int = 1
    legs: int = 3

@router.post("/slate", response_model=SlateReport)
def slate(req: SlateRequest) -> SlateReport:
    from app.services.slate_service import build_slate_report
    return build_slate_report(
        day_utc=req.date_utc,
        competition=req.competition,
        n_sims=req.n_sims,
        seed=req.seed,
        bankroll=req.bankroll,
        max_picks_per_match=req.max_picks_per_match,
        legs=req.legs,
    )


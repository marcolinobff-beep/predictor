from typing import List, Optional

from fastapi import APIRouter, Query

from app.models.schemas import PlayerProjection
from app.services.player_projection_service import list_player_projections


router = APIRouter()


@router.get("/players/projections", response_model=List[PlayerProjection])
def get_player_projections(
    league: str = Query(..., description="Understat league, es: Serie_A"),
    season: int = Query(..., description="Season start year, es: 2024 per 2024/25"),
    team: Optional[str] = Query(None, description="Team title (optional)"),
    min_minutes: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
) -> List[PlayerProjection]:
    return list_player_projections(
        league=league,
        season=season,
        team=team,
        min_minutes=min_minutes,
        limit=limit,
    )

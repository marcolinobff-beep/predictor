from fastapi import APIRouter
from pydantic import BaseModel

from app.models.schemas import ChatResponse
from app.services.chat_service import answer_query

router = APIRouter(tags=["chat"])


class ChatRequest(BaseModel):
    query: str
    n_sims: int = 50000
    seed: int = 42
    bankroll: float = 1000.0
    session_id: str | None = None


@router.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    return answer_query(
        query=req.query,
        n_sims=req.n_sims,
        seed=req.seed,
        bankroll=req.bankroll,
        session_id=req.session_id,
    )

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.routes_match import router as match_router
from app.api.v1.routes_chat import router as chat_router
from app.api.v1.routes_players import router as players_router
from app.api.v1.routes_ui_api import router as ui_api_router
from dotenv import load_dotenv
load_dotenv()


app = FastAPI(title="Football Prediction Bot", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(match_router, prefix="/v1")
app.include_router(chat_router, prefix="/v1")
app.include_router(players_router, prefix="/v1")
app.include_router(ui_api_router, prefix="/v1/ui")

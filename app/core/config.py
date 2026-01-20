from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    service_version: str = "0.1.0"
    sqlite_path: str = Field(default="data/sqlite/football.db")
    calibration_path: str = Field(default="data/calibration/calibration_v1.json")
    calibration_by_season_path: str = Field(default="data/calibration/calibration_by_season.json")
    dc_params_path: str = Field(default="data/calibration/dc_params.json")
    kpi_report_path: str = Field(default="data/reports/kpi_report.json")
    data_quality_report_path: str = Field(default="data/reports/data_quality.json")
    gbm_model_path: str = Field(default="data/models/gbm_light.json")
    residual_model_path: str = Field(default="data/models/residual_vs_market_1x2.json")
    stack_model_path: str = Field(default="data/models/stack_1x2.json")
    betting_gate_path: str = Field(default="data/config/betting_gate.json")

    ensemble_weight: float = Field(default=0.35)
    ensemble_weights_path: str = Field(default="data/calibration/ensemble_weights.json")
    temp_scale_path: str = Field(default="data/calibration/temp_scale_1x2.json")
    calibration_policy_path: str = Field(default="data/calibration/calibration_policy.json")
    market_rules_path: str = Field(default="data/config/market_rules.json")
    schedine_rules_path: str = Field(default="data/config/schedine_rules.json")
    football_data_api_key: str | None = Field(default=None)
    football_data_base_url: str = Field(default="https://api.football-data.org/v4")
    api_football_key: str | None = Field(default=None)
    api_football_base_url: str = Field(default="https://v3.football.api-sports.io")
    sportmonks_api_key: str | None = Field(default=None)
    sportmonks_base_url: str = Field(default="https://api.sportmonks.com/v3/football")
    sportmonks_leagues_path: str = Field(default="data/config/sportmonks_leagues.json")
    diretta_leagues_path: str = Field(default="data/config/diretta_leagues.json")
    chat_ui_token: str | None = Field(default=None)
    llm_enabled: bool = Field(default=False)
    llm_provider: str = Field(default="ollama")
    llm_base_url: str = Field(default="http://127.0.0.1:11434")
    llm_model: str = Field(default="llama3.1:8b")
    llm_temperature: float = Field(default=0.2)
    llm_max_tokens: int = Field(default=700)
    llm_timeout: float = Field(default=6.0)

settings = Settings()

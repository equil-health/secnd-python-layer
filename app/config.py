from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # GCP / Vertex AI
    GCP_PROJECT_ID: str = ""
    GCP_LOCATION: str = "europe-west4"
    GCP_SERVICE_ACCOUNT_FILE: str = ""

    # MedGemma dedicated endpoint
    MEDGEMMA_ENDPOINT_ID: str = ""
    MEDGEMMA_DEDICATED_DOMAIN: str = ""

    # External APIs
    SERPER_API_KEY: str = ""

    # Infrastructure
    DATABASE_URL: str = ""
    REDIS_URL: str = ""
    GCS_BUCKET: str = "medsecondopinion-reports"

    # App Settings
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    CORS_ORIGINS: str = "http://localhost:5173,http://localhost:3000"

    # Pipeline Tuning
    MEDGEMMA_MAX_TOKENS: int = 4096
    MEDGEMMA_TEMPERATURE: float = 0.3
    GEMINI_MAX_TOKENS: int = 2048
    GEMINI_TEMPERATURE: float = 0.3
    SERPER_RESULTS_PER_QUERY: int = 5
    STORM_SEARCH_TOP_K: int = 20
    PIPELINE_TIMEOUT_SECONDS: int = 300

    # File uploads
    UPLOAD_DIR: str = "./uploads"
    MAX_UPLOAD_SIZE_MB: int = 50

    @property
    def MEDGEMMA_PREDICT_URL(self) -> str:
        return (
            f"https://{self.MEDGEMMA_DEDICATED_DOMAIN}/v1/projects/{self.GCP_PROJECT_ID}"
            f"/locations/{self.GCP_LOCATION}/endpoints/{self.MEDGEMMA_ENDPOINT_ID}:predict"
        )

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

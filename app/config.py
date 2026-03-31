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

    # Google AI Studio
    GEMINI_API_KEY: str = ""

    # External APIs
    SERPER_API_KEY: str = ""
    GOOGLE_SEARCH_API_KEY: str = ""
    GOOGLE_CSE_ID: str = ""
    HF_TOKEN: str = ""
    OPENALEX_EMAIL: str = ""
    OPENALEX_API_KEY: str = ""

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
    STORM_TIMEOUT_SECONDS: int = 180
    COSTORM_TIMEOUT_SECONDS: int = 240
    COSTORM_OUTPUT_DIR: str = "./costorm_output"
    PIPELINE_TIMEOUT_SECONDS: int = 300

    # MedASR
    MEDASR_MODEL: str = "google/medasr"
    MEDASR_CHUNK_SECONDS: int = 90

    # File uploads
    UPLOAD_DIR: str = "./uploads"
    MAX_UPLOAD_SIZE_MB: int = 50

    # Pulse — Medical Literature Digest
    NCBI_API_KEY: str = ""
    NCBI_EMAIL: str = ""
    PULSE_ENABLED: bool = True
    PULSE_MAX_ARTICLES_PER_DIGEST: int = 10
    PULSE_DEFAULT_FREQUENCY: str = "weekly"
    PULSE_SCAN_DAYS_BACK: int = 7

    # Breaking — Daily Headline Pipeline
    BREAKING_ENABLED: bool = True
    BREAKING_SPECIALTIES_COUNT: int = 10
    BREAKING_HEADLINES_PER_SPECIALTY: int = 7
    BREAKING_RAW_FETCH_COUNT: int = 20
    BREAKING_DEDUP_THRESHOLD: float = 0.87
    BREAKING_REDIS_TTL_HOURS: int = 12

    # SDSS — GPU Pod (Second Opinion pipeline)
    SDSS_BASE_URL: str = ""  # e.g. https://xyz.ngrok-free.dev

    # Firebase — Push Notifications
    FIREBASE_SERVICE_ACCOUNT_PATH: str = ""

    # Auth / JWT
    JWT_SECRET_KEY: str = "change-me-in-production"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRY_HOURS: int = 24

    # Admin seed
    ADMIN_EMAIL: str = ""
    ADMIN_PASSWORD: str = ""
    ADMIN_NAME: str = "Admin"

    @property
    def MEDGEMMA_PREDICT_URL(self) -> str:
        return (
            f"https://{self.MEDGEMMA_DEDICATED_DOMAIN}/v1/projects/{self.GCP_PROJECT_ID}"
            f"/locations/{self.GCP_LOCATION}/endpoints/{self.MEDGEMMA_ENDPOINT_ID}:predict"
        )

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

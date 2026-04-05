from pydantic_settings import BaseSettings
from functools import lru_cache


# Platform secrets for JWT auth — stored in config for now, move to vault later
PLATFORM_SECRETS: dict[str, str] = {
    "marketpulse": "marketpulse-secret-change-in-prod",
    "mfpulse": "mfpulse-secret-change-in-prod",
    "champion_trader": "champion-trader-secret-change-in-prod",
    "admin": "admin-secret-change-in-prod",
}

# Platforms with admin privileges
ADMIN_PLATFORMS: set[str] = {"admin"}


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://fie_admin:password@localhost:5432/data_engine"
    database_url_sync: str = "postgresql+psycopg2://fie_admin:password@localhost:5432/data_engine"

    # Redis
    redis_url: str = "redis://127.0.0.1:6379/0"

    # JWT
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 24
    jwt_refresh_expiry_days: int = 30

    # Rate limiting
    rate_limit_requests_per_minute: int = 1000

    # AWS
    aws_region: str = "ap-south-1"
    pii_kms_key_arn: str = ""
    pii_hmac_key_arn: str = ""

    # Morningstar
    morningstar_access_code: str = ""
    morningstar_base_url: str = "https://api.morningstar.com/v2/service/mf"

    # External APIs
    fred_api_key: str = ""
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    # Goldilocks
    goldilocks_email: str = ""
    goldilocks_password: str = ""

    # Slack
    slack_webhook_url: str = ""

    # S3
    s3_archive_bucket: str = "jsl-data-engine-archive"

    # App
    app_env: str = "production"
    app_port: int = 8010
    log_level: str = "INFO"

    # Migration source databases
    fie_v3_database_url: str = ""
    mf_pulse_database_url: str = ""
    client_portal_database_url: str = ""

    # Dashboard CORS origin
    dashboard_origin: str = "http://localhost:8099"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()

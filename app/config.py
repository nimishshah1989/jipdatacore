import os

from functools import lru_cache

from pydantic_settings import BaseSettings


# Platform secrets for JWT auth — loaded from environment variables
PLATFORM_SECRETS: dict[str, str] = {
    "marketpulse": os.environ.get("PLATFORM_SECRET_MARKETPULSE", ""),
    "mfpulse": os.environ.get("PLATFORM_SECRET_MFPULSE", ""),
    "champion_trader": os.environ.get("PLATFORM_SECRET_CHAMPION", ""),
    "admin": os.environ.get("PLATFORM_SECRET_ADMIN", ""),
}

# Platforms with admin privileges
ADMIN_PLATFORMS: set[str] = {"admin"}


class Settings(BaseSettings):
    # Database
    database_url: str = ""
    database_url_sync: str = ""

    # Redis
    redis_url: str = ""

    # JWT
    jwt_secret: str = ""
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
    groq_api_key: str = ""

    # Goldilocks
    goldilocks_email: str = ""
    goldilocks_password: str = ""

    # Screener.in
    screener_session_cookie: str = ""

    # Slack
    slack_webhook_url: str = ""

    # Pipeline trigger API key (for Claude scheduled agents)
    pipeline_api_key: str = ""

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

    # extra="ignore" tolerates unknown env vars (e.g. platform-specific
    # fields someone adds to .env without updating this class). Prevents
    # Pydantic from crashing the service on startup if .env drifts.
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


@lru_cache
def get_settings() -> Settings:
    return Settings()

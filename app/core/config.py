from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    # API Config
    API_TITLE: str = "Parquet S3 API"
    API_VERSION: str = "1.0.0"
    PORT: int = 8000
    API_ACCESS_TOKEN: str = "default_insecure_token_change_me"

    # AWS / Supabase S3 Config
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_DEFAULT_REGION: str = "us-east-1"
    SUPABASE_S3_ENDPOINT: str
    SUPABASE_BUCKET: str

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8",
        extra="ignore"
    )

@lru_cache
def get_settings():
    return Settings()

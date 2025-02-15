from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
import sys


class Settings(BaseSettings):
    """환경설정"""

    model_config = SettingsConfigDict(
        env_file=os.path.join(
            (
                os.path.dirname(sys.executable)
                if getattr(sys, "frozen", False)
                else os.path.dirname(os.path.dirname(__file__))
            ),
            ".env",
        ),
        env_file_encoding="utf-8",
    )

    # target folder path
    ERD_FOLDER_PATH: str
    # DB Host
    DB_SERVER: str
    # DB Port
    DB_PORT: int = 3306
    # DB Username
    DB_USERNAME: str
    # DB Password
    DB_PASSWORD: str
    # DB Database name
    DB_DATABASE: str


@lru_cache
def get_settings():
    s = Settings()
    return s


settings = get_settings()

from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv
from typing import Optional # Ensure Optional is imported

load_dotenv() # Load environment variables from .env file

class Settings(BaseSettings):
    # IMPORTANT: All sensitive credentials (DB URLs, API keys, etc.)
    # should be loaded from environment variables and not hardcoded in source files.
    # Use a .env file for local development, and environment variables in production.
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/observability_db")
    AIRFLOW_DATABASE_URL: str = os.getenv("AIRFLOW_DATABASE_URL", "postgresql://airflow_user:airflow_password@localhost/airflow_db")
    APPLICATION_DATABASE_URL: Optional[str] = os.getenv("APPLICATION_DATABASE_URL", "postgresql://app_user:app_password@localhost/application_data_db")
    # Add other configurations as needed, e.g., AIRFLOW_API_URL from previous setup
    AIRFLOW_API_URL: str = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")


    class Config:
        env_file = ".env"
        extra = "ignore" # Ignore extra fields from .env that are not defined in Settings

settings = Settings()

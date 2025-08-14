from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # MongoDB Configuration
    mongodb_url: str = Field(..., env="MONGODB_URL", description="MongoDB connection string")
    database_name: str = Field(default="guppy_funds", env="DATABASE_NAME", description="Database name")
    amex_collection: str = Field(default="amex_raw", env="AMEX_COLLECTION", description="Amex raw transactions collection")
    wells_collection: str = Field(default="wells_raw", env="WELLS_COLLECTION", description="Wells Fargo raw transactions collection")
    
    # Application Configuration
    environment: str = Field(default="development", env="ENVIRONMENT", description="Environment (development/production)")
    log_level: str = Field(default="INFO", env="LOG_LEVEL", description="Logging level")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

# Global settings instance
settings = Settings()
"""
Configuration for analytics service.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Analytics service settings."""
    
    # Service configuration
    SERVICE_NAME: str = "social-media-analytics"
    SERVICE_VERSION: str = "1.0.0"
    HOST: str = "0.0.0.0"
    PORT: int = 8001
    DEBUG: bool = False
    
    # MySQL configuration
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_DATABASE: str = "social_media_analytics"
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "password"
    DB_POOL_SIZE: int = 10
    
    # API configuration
    API_V1_PREFIX: str = "/api/v1"
    API_TITLE: str = "Social Media Analytics API"
    API_DESCRIPTION: str = "Real-time analytics and insights"
    
    # Logging
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
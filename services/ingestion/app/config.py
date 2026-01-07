"""
Configuration management for the ingestion service.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Service configuration
    SERVICE_NAME: str = "social-media-ingestion"
    SERVICE_VERSION: str = "1.0.0"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False
    
    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "social-events"
    KAFKA_PRODUCER_ACKS: str = "1"  # 0, 1, or "all"
    KAFKA_PRODUCER_RETRIES: int = 3
    KAFKA_PRODUCER_COMPRESSION_TYPE: str = "gzip"  # none, gzip, snappy, lz4, zstd
    KAFKA_PRODUCER_BATCH_SIZE: int = 16384  # bytes
    KAFKA_PRODUCER_LINGER_MS: int = 10  # milliseconds
    KAFKA_PRODUCER_MAX_IN_FLIGHT: int = 5
    
    # Kafka connection
    KAFKA_REQUEST_TIMEOUT_MS: int = 30000
    KAFKA_METADATA_MAX_AGE_MS: int = 300000
    
    # Security (optional)
    KAFKA_SECURITY_PROTOCOL: str = "PLAINTEXT"  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    KAFKA_SASL_MECHANISM: Optional[str] = None  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    KAFKA_SASL_USERNAME: Optional[str] = None
    KAFKA_SASL_PASSWORD: Optional[str] = None
    
    # API configuration
    API_V1_PREFIX: str = "/api/v1"
    API_TITLE: str = "Social Media Event Ingestion API"
    API_DESCRIPTION: str = "High-throughput event ingestion service"
    API_VERSION: str = "1.0.0"
    
    # Rate limiting
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_PER_MINUTE: int = 10000
    
    # Monitoring
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9090
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json or text
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()
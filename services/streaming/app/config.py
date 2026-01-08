"""
Configuration for Spark streaming service.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Streaming service settings."""
    
    # Service configuration
    SERVICE_NAME: str = "social-media-streaming"
    SERVICE_VERSION: str = "1.0.0"
    
    # Spark configuration
    SPARK_APP_NAME: str = "SocialMediaEventProcessor"
    SPARK_MASTER: str = "local[*]"  # local[*] for development, spark://host:port for cluster
    SPARK_LOG_LEVEL: str = "WARN"
    
    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_TOPIC: str = "social-events"
    KAFKA_GROUP_ID: str = "spark-streaming-consumer"
    KAFKA_STARTING_OFFSETS: str = "latest"  # earliest or latest
    KAFKA_MAX_OFFSETS_PER_TRIGGER: int = 10000
    
    # Streaming configuration
    CHECKPOINT_LOCATION: str = "/tmp/spark-checkpoints"
    TRIGGER_INTERVAL: str = "30 seconds"  # Processing trigger interval
    WATERMARK_DELAY: str = "10 minutes"  # Late data handling
    
    # Window configurations for aggregations
    WINDOW_DURATION_5MIN: str = "5 minutes"
    WINDOW_DURATION_1HR: str = "1 hour"
    WINDOW_DURATION_1DAY: str = "1 day"
    SLIDE_DURATION: str = "1 minute"
    
    # S3 configuration
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: str = "us-east-1"
    S3_BUCKET: str = "social-media-events"
    S3_RAW_EVENTS_PREFIX: str = "raw-events"
    S3_ENABLE_PARTITIONING: bool = True
    
    # MySQL configuration
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_DATABASE: str = "social_media_analytics"
    MYSQL_USER: str = "root"
    MYSQL_PASSWORD: str = "password"
    MYSQL_JDBC_URL: str = ""  # Auto-constructed if empty
    
    # Processing configuration
    ENABLE_S3_WRITE: bool = True
    ENABLE_MYSQL_WRITE: bool = True
    BATCH_SIZE: int = 1000
    
    # Monitoring
    ENABLE_METRICS: bool = True
    LOG_LEVEL: str = "INFO"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Auto-construct JDBC URL if not provided
        if not self.MYSQL_JDBC_URL:
            self.MYSQL_JDBC_URL = (
                f"jdbc:mysql://{self.MYSQL_HOST}:{self.MYSQL_PORT}/"
                f"{self.MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true"
            )
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
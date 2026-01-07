"""
S3 writer for raw event data (Data Lake).
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from .config import settings

logger = logging.getLogger(__name__)


class S3Writer:
    """Handles writing data to S3 data lake."""
    
    def __init__(self, spark: SparkSession):
        """Initialize S3 writer with Spark session."""
        self.spark = spark
        self.bucket = settings.S3_BUCKET
        self.prefix = settings.S3_RAW_EVENTS_PREFIX
    
    def write_raw_events(self, df: DataFrame, checkpoint_location: str):
        """
        Write raw events to S3 in Parquet format with partitioning.
        
        Args:
            df: DataFrame containing events
            checkpoint_location: Checkpoint directory for fault tolerance
        
        Returns:
            StreamingQuery object
        """
        s3_path = f"s3a://{self.bucket}/{self.prefix}"
        
        logger.info(f"Writing raw events to S3: {s3_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .option("path", s3_path) \
            .option("checkpointLocation", checkpoint_location) \
            .partitionBy("year", "month", "day", "hour") \
            .trigger(processingTime=settings.TRIGGER_INTERVAL) \
            .start()
        
        logger.info(f"Raw events writer started with checkpoint: {checkpoint_location}")
        
        return query
    
    def write_batch_to_s3(self, df: DataFrame, path_suffix: str):
        """
        Write batch data to S3 (for historical backfills).
        
        Args:
            df: DataFrame to write
            path_suffix: Additional path suffix after prefix
        """
        s3_path = f"s3a://{self.bucket}/{self.prefix}/{path_suffix}"
        
        logger.info(f"Writing batch data to S3: {s3_path}")
        
        df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .save(s3_path)
        
        logger.info("Batch write to S3 completed")
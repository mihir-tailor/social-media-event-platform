"""
MySQL writer for aggregated metrics.
"""
import logging
from pyspark.sql import DataFrame
from .config import settings

logger = logging.getLogger(__name__)


class MySQLWriter:
    """Handles writing aggregated data to MySQL."""
    
    def __init__(self):
        """Initialize MySQL writer configuration."""
        self.jdbc_url = settings.MYSQL_JDBC_URL
        self.properties = {
            "user": settings.MYSQL_USER,
            "password": settings.MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    
    def _write_to_mysql(self, df: DataFrame, table: str, checkpoint: str):
        """
        Generic method to write DataFrame to MySQL.
        
        Args:
            df: DataFrame to write
            table: Target table name
            checkpoint: Checkpoint location
        
        Returns:
            StreamingQuery object
        """
        logger.info(f"Writing to MySQL table: {table}")
        
        def write_batch(batch_df, batch_id):
            """Write each micro-batch to MySQL."""
            try:
                batch_df.write \
                    .jdbc(
                        url=self.jdbc_url,
                        table=table,
                        mode="append",
                        properties=self.properties
                    )
                logger.debug(f"Batch {batch_id} written to {table}")
            except Exception as e:
                logger.error(f"Error writing batch {batch_id} to {table}: {e}")
                # Don't raise to allow stream to continue
        
        query = df.writeStream \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", checkpoint) \
            .trigger(processingTime=settings.TRIGGER_INTERVAL) \
            .start()
        
        logger.info(f"MySQL writer started for {table}")
        return query
    
    def write_event_metrics(self, df: DataFrame, checkpoint_location: str, 
                           table_name: str = "event_metrics_5min"):
        """
        Write event metrics to MySQL.
        
        Schema:
        - window_start: timestamp
        - window_end: timestamp
        - event_type: string
        - platform: string
        - event_count: long
        - unique_users: long
        - computed_at: timestamp
        """
        return self._write_to_mysql(df, table_name, checkpoint_location)
    
    def write_user_metrics(self, df: DataFrame, checkpoint_location: str,
                          table_name: str = "user_metrics_1hr"):
        """
        Write user activity metrics to MySQL.
        
        Schema:
        - window_start: timestamp
        - window_end: timestamp
        - user_id: string
        - platform: string
        - total_events: long
        - likes: long
        - comments: long
        - shares: long
        - views: long
        - clicks: long
        - computed_at: timestamp
        """
        return self._write_to_mysql(df, table_name, checkpoint_location)
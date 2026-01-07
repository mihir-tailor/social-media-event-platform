"""
PySpark Structured Streaming processor for real-time event processing.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, current_timestamp,
    to_timestamp, date_format, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, MapType
)

from .config import settings
from .s3_writer import S3Writer
from .mysql_writer import MySQLWriter

logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)


class EventStreamProcessor:
    """Main processor for streaming events."""
    
    def __init__(self):
        """Initialize Spark session and components."""
        self.spark = self._create_spark_session()
        self.s3_writer = S3Writer(self.spark) if settings.ENABLE_S3_WRITE else None
        self.mysql_writer = MySQLWriter() if settings.ENABLE_MYSQL_WRITE else None
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        logger.info("Creating Spark session...")
        
        builder = SparkSession.builder \
            .appName(settings.SPARK_APP_NAME) \
            .master(settings.SPARK_MASTER)
        
        # Kafka packages
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "mysql:mysql-connector-java:8.0.33,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        
        # AWS credentials if provided
        if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
            builder = builder \
                .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID) \
                .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
        
        # Additional S3 configuration
        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.AWS_REGION}.amazonaws.com")
        
        # Performance tuning
        builder = builder \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.stateStore.providerClass", 
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(settings.SPARK_LOG_LEVEL)
        
        logger.info("Spark session created successfully")
        return spark
    
    def _get_event_schema(self) -> StructType:
        """Define schema for incoming events."""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("content_id", StringType(), True),
            StructField("platform", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("session_id", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("location", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka."""
        logger.info(f"Reading from Kafka topic: {settings.KAFKA_TOPIC}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", settings.KAFKA_TOPIC) \
            .option("startingOffsets", settings.KAFKA_STARTING_OFFSETS) \
            .option("maxOffsetsPerTrigger", settings.KAFKA_MAX_OFFSETS_PER_TRIGGER) \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON from Kafka value
        schema = self._get_event_schema()
        events_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp string to timestamp type
        events_df = events_df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )
        
        # Add processing timestamp
        events_df = events_df.withColumn(
            "processing_time",
            current_timestamp()
        )
        
        return events_df
    
    def process_raw_events(self, events_df):
        """Process and write raw events to S3."""
        if not self.s3_writer:
            logger.warning("S3 writing disabled")
            return
        
        logger.info("Starting raw events processing to S3...")
        
        # Add partitioning columns
        partitioned_df = events_df \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp")))
        
        # Write to S3 with partitioning
        query = self.s3_writer.write_raw_events(
            partitioned_df,
            checkpoint_location=f"{settings.CHECKPOINT_LOCATION}/raw-events"
        )
        
        return query
    
    def process_aggregated_metrics(self, events_df):
        """Process and aggregate metrics, write to MySQL."""
        if not self.mysql_writer:
            logger.warning("MySQL writing disabled")
            return
        
        logger.info("Starting aggregated metrics processing...")
        
        # Set watermark for handling late data
        events_with_watermark = events_df.withWatermark("timestamp", settings.WATERMARK_DELAY)
        
        # Aggregate: Events per 5-minute window by type
        event_metrics_5min = events_with_watermark \
            .groupBy(
                window(col("timestamp"), settings.WINDOW_DURATION_5MIN, settings.SLIDE_DURATION),
                col("event_type"),
                col("platform")
            ) \
            .agg(
                count("*").alias("event_count"),
                count(col("user_id").isNotNull()).alias("unique_users")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("platform"),
                col("event_count"),
                col("unique_users"),
                current_timestamp().alias("computed_at")
            )
        
        # Write 5-minute metrics to MySQL
        query1 = self.mysql_writer.write_event_metrics(
            event_metrics_5min,
            checkpoint_location=f"{settings.CHECKPOINT_LOCATION}/event-metrics-5min",
            table_name="event_metrics_5min"
        )
        
        # Aggregate: User activity metrics
        user_metrics = events_with_watermark \
            .groupBy(
                window(col("timestamp"), settings.WINDOW_DURATION_1HR),
                col("user_id"),
                col("platform")
            ) \
            .agg(
                count("*").alias("total_events"),
                count(col("event_type") == "like").alias("likes"),
                count(col("event_type") == "comment").alias("comments"),
                count(col("event_type") == "share").alias("shares"),
                count(col("event_type") == "view").alias("views"),
                count(col("event_type") == "click").alias("clicks")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("platform"),
                col("total_events"),
                col("likes"),
                col("comments"),
                col("shares"),
                col("views"),
                col("clicks"),
                current_timestamp().alias("computed_at")
            )
        
        # Write user metrics to MySQL
        query2 = self.mysql_writer.write_user_metrics(
            user_metrics,
            checkpoint_location=f"{settings.CHECKPOINT_LOCATION}/user-metrics-1hr",
            table_name="user_metrics_1hr"
        )
        
        return query1, query2
    
    def start_streaming(self):
        """Start all streaming queries."""
        logger.info("Starting event stream processing...")
        
        # Read events from Kafka
        events_df = self.read_kafka_stream()
        
        queries = []
        
        # Start raw events processing
        if settings.ENABLE_S3_WRITE:
            raw_query = self.process_raw_events(events_df)
            if raw_query:
                queries.append(raw_query)
        
        # Start aggregated metrics processing
        if settings.ENABLE_MYSQL_WRITE:
            metric_queries = self.process_aggregated_metrics(events_df)
            if metric_queries:
                queries.extend([q for q in metric_queries if q])
        
        # Wait for all queries to terminate
        logger.info(f"Started {len(queries)} streaming queries")
        
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping streaming queries...")
            for query in queries:
                query.stop()
            self.spark.stop()
            logger.info("All queries stopped")


def main():
    """Main entry point."""
    processor = EventStreamProcessor()
    processor.start_streaming()


if __name__ == "__main__":
    main()
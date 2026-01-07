"""
Unit tests for Spark stream processor.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("test_processor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark_session
    
    spark_session.stop()


class TestSparkProcessor:
    """Tests for Spark stream processor."""
    
    def test_spark_session_creation(self, spark):
        """Test Spark session is created successfully."""
        assert spark is not None
        assert spark.version >= "3.5.0"
    
    def test_event_schema(self, spark):
        """Test event schema definition."""
        from app.spark_processor import EventStreamProcessor
        
        processor = EventStreamProcessor()
        schema = processor._get_event_schema()
        
        assert isinstance(schema, StructType)
        assert "event_id" in [field.name for field in schema.fields]
        assert "event_type" in [field.name for field in schema.fields]
        assert "user_id" in [field.name for field in schema.fields]
    
    def test_dataframe_creation(self, spark):
        """Test creating DataFrame with event schema."""
        schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("platform", StringType(), False),
            StructField("timestamp", StringType(), False)
        ])
        
        data = [
            ("evt_1", "like", "user_1", "web", "2024-01-15T10:00:00"),
            ("evt_2", "view", "user_2", "ios", "2024-01-15T10:01:00")
        ]
        
        df = spark.createDataFrame(data, schema)
        
        assert df.count() == 2
        assert len(df.columns) == 5
    
    def test_window_aggregation(self, spark):
        """Test windowing aggregation logic."""
        from pyspark.sql.functions import window, count
        
        # Create test data with timestamps
        data = [
            ("evt_1", "like", "user_1", "2024-01-15T10:00:00"),
            ("evt_2", "like", "user_2", "2024-01-15T10:01:00"),
            ("evt_3", "view", "user_3", "2024-01-15T10:05:00")
        ]
        
        df = spark.createDataFrame(
            data,
            ["event_id", "event_type", "user_id", "timestamp"]
        )
        
        # Convert string to timestamp
        df = df.withColumn(
            "timestamp",
            df["timestamp"].cast(TimestampType())
        )
        
        # Aggregate by 5-minute windows
        result = df.groupBy(
            window("timestamp", "5 minutes"),
            "event_type"
        ).agg(count("*").alias("event_count"))
        
        assert result.count() > 0
    
    def test_filtering_events(self, spark):
        """Test filtering events by type."""
        data = [
            ("evt_1", "like", "user_1"),
            ("evt_2", "view", "user_2"),
            ("evt_3", "like", "user_3")
        ]
        
        df = spark.createDataFrame(
            data,
            ["event_id", "event_type", "user_id"]
        )
        
        # Filter only likes
        likes = df.filter(df.event_type == "like")
        
        assert likes.count() == 2
    
    def test_user_aggregation(self, spark):
        """Test aggregating events by user."""
        from pyspark.sql.functions import count
        
        data = [
            ("evt_1", "like", "user_1"),
            ("evt_2", "view", "user_1"),
            ("evt_3", "like", "user_2")
        ]
        
        df = spark.createDataFrame(
            data,
            ["event_id", "event_type", "user_id"]
        )
        
        # Count events per user
        user_counts = df.groupBy("user_id") \
            .agg(count("*").alias("event_count"))
        
        results = user_counts.collect()
        
        # user_1 should have 2 events
        user_1_count = [r for r in results if r.user_id == "user_1"][0].event_count
        assert user_1_count == 2
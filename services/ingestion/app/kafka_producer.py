"""
Kafka producer implementation for event publishing.
"""
import json
import logging
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from .config import settings

logger = logging.getLogger(__name__)


class EventProducer:
    """
    Kafka producer wrapper for publishing events.
    Implements singleton pattern for connection pooling.
    """
    
    _instance: Optional['EventProducer'] = None
    _producer: Optional[KafkaProducer] = None
    
    def __new__(cls):
        """Ensure only one producer instance exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize Kafka producer if not already initialized."""
        if self._producer is None:
            self._initialize_producer()
    
    def _initialize_producer(self):
        """Create and configure Kafka producer."""
        try:
            config = {
                'bootstrap_servers': settings.KAFKA_BOOTSTRAP_SERVERS.split(','),
                'acks': settings.KAFKA_PRODUCER_ACKS,
                'retries': settings.KAFKA_PRODUCER_RETRIES,
                'compression_type': settings.KAFKA_PRODUCER_COMPRESSION_TYPE,
                'batch_size': settings.KAFKA_PRODUCER_BATCH_SIZE,
                'linger_ms': settings.KAFKA_PRODUCER_LINGER_MS,
                'max_in_flight_requests_per_connection': settings.KAFKA_PRODUCER_MAX_IN_FLIGHT,
                'request_timeout_ms': settings.KAFKA_REQUEST_TIMEOUT_MS,
                'metadata_max_age_ms': settings.KAFKA_METADATA_MAX_AGE_MS,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
            }
            
            # Add security configuration if needed
            if settings.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
                config['security_protocol'] = settings.KAFKA_SECURITY_PROTOCOL
                if settings.KAFKA_SASL_MECHANISM:
                    config['sasl_mechanism'] = settings.KAFKA_SASL_MECHANISM
                    config['sasl_plain_username'] = settings.KAFKA_SASL_USERNAME
                    config['sasl_plain_password'] = settings.KAFKA_SASL_PASSWORD
            
            self._producer = KafkaProducer(**config)
            logger.info(
                f"Kafka producer initialized successfully. "
                f"Brokers: {settings.KAFKA_BOOTSTRAP_SERVERS}"
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    async def send_event(self, event_data: dict, key: Optional[str] = None) -> bool:
        """
        Send a single event to Kafka.
        
        Args:
            event_data: Event data dictionary
            key: Optional partition key (e.g., user_id for ordering)
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self._producer:
            logger.error("Producer not initialized")
            return False
        
        try:
            # Use user_id as key for consistent partitioning
            partition_key = key or event_data.get('user_id')
            
            # Send asynchronously with callback
            future = self._producer.send(
                settings.KAFKA_TOPIC,
                value=event_data,
                key=partition_key
            )
            
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Event sent to Kafka: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True
            
        except KafkaTimeoutError as e:
            logger.error(f"Kafka timeout while sending event: {e}")
            return False
        except KafkaError as e:
            logger.error(f"Kafka error while sending event: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while sending event: {e}")
            return False
    
    async def send_batch(self, events: list[dict]) -> tuple[int, int]:
        """
        Send multiple events to Kafka.
        
        Args:
            events: List of event data dictionaries
        
        Returns:
            tuple: (successful_count, failed_count)
        """
        successful = 0
        failed = 0
        
        for event in events:
            if await self.send_event(event):
                successful += 1
            else:
                failed += 1
        
        # Flush to ensure all messages are sent
        self.flush()
        
        return successful, failed
    
    def flush(self, timeout: Optional[float] = None):
        """
        Flush pending messages.
        
        Args:
            timeout: Maximum time to wait in seconds
        """
        if self._producer:
            try:
                self._producer.flush(timeout=timeout)
            except Exception as e:
                logger.error(f"Error flushing producer: {e}")
    
    def close(self):
        """Close the producer connection."""
        if self._producer:
            try:
                self._producer.close(timeout=10)
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
            finally:
                self._producer = None
    
    def is_connected(self) -> bool:
        """
        Check if producer is connected to Kafka.
        
        Returns:
            bool: True if connected, False otherwise
        """
        if not self._producer:
            return False
        
        try:
            # Bootstrap check - tries to get metadata
            self._producer.bootstrap_connected()
            return True
        except Exception:
            return False


# Global producer instance
producer = EventProducer()
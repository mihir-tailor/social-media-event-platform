"""
Integration tests for ingestion API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock, MagicMock


# Mock the producer before importing the app
@pytest.fixture(autouse=True)
def mock_producer():
    """Mock Kafka producer for all tests."""
    with patch('app.main.producer') as mock:
        mock.send_event = AsyncMock(return_value=True)
        mock.send_batch = AsyncMock(return_value=(2, 0))
        mock.is_connected = MagicMock(return_value=True)
        mock.flush = MagicMock()
        mock.close = MagicMock()
        yield mock


@pytest.fixture
def client():
    """Create test client."""
    from app.main import app
    return TestClient(app)


class TestRootEndpoints:
    """Tests for root and health endpoints."""
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns service info."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert "version" in data
        assert "status" in data
    
    def test_health_check_healthy(self, client):
        """Test health check when Kafka is connected."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["kafka_connected"] is True
        assert data["service"] == "ingestion"


class TestSingleEventIngestion:
    """Tests for single event ingestion endpoint."""
    
    def test_ingest_valid_event(self, client):
        """Test ingesting a valid event."""
        event = {
            "event_type": "like",
            "user_id": "user_12345",
            "platform": "web",
            "content_id": "post_98765"
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "success"
        assert "event_id" in data
        assert data["message"] == "Event ingested successfully"
    
    def test_ingest_event_minimal_fields(self, client):
        """Test ingesting event with only required fields."""
        event = {
            "event_type": "view",
            "user_id": "user_999",
            "platform": "ios"
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "success"
    
    def test_ingest_event_with_metadata(self, client):
        """Test ingesting event with metadata."""
        event = {
            "event_type": "click",
            "user_id": "user_555",
            "platform": "android",
            "metadata": {"duration_ms": 150, "scroll_depth": 75}
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 201
    
    def test_invalid_event_type(self, client):
        """Test validation fails for invalid event type."""
        event = {
            "event_type": "invalid_type",
            "user_id": "user_123",
            "platform": "web"
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 422  # Validation error
    
    def test_empty_user_id(self, client):
        """Test validation fails for empty user_id."""
        event = {
            "event_type": "like",
            "user_id": "   ",
            "platform": "web"
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 422
    
    def test_missing_required_field(self, client):
        """Test validation fails when required field is missing."""
        event = {
            "event_type": "like",
            # Missing user_id
            "platform": "web"
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 422
    
    def test_invalid_platform(self, client):
        """Test validation fails for invalid platform."""
        event = {
            "event_type": "like",
            "user_id": "user_123",
            "platform": "invalid_platform"
        }
        
        response = client.post("/api/v1/events", json=event)
        
        assert response.status_code == 422


class TestBatchEventIngestion:
    """Tests for batch event ingestion endpoint."""
    
    def test_ingest_valid_batch(self, client):
        """Test ingesting a valid batch of events."""
        batch = {
            "events": [
                {
                    "event_type": "like",
                    "user_id": "user_1",
                    "platform": "web"
                },
                {
                    "event_type": "comment",
                    "user_id": "user_2",
                    "platform": "ios"
                }
            ]
        }
        
        response = client.post("/api/v1/events/batch", json=batch)
        
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "success"
        assert data["total_events"] == 2
        assert data["successful"] == 2
        assert data["failed"] == 0
    
    def test_ingest_large_batch(self, client):
        """Test ingesting batch with many events."""
        batch = {
            "events": [
                {
                    "event_type": "view",
                    "user_id": f"user_{i}",
                    "platform": "web"
                }
                for i in range(100)
            ]
        }
        
        response = client.post("/api/v1/events/batch", json=batch)
        
        assert response.status_code == 201
        data = response.json()
        assert data["total_events"] == 100
    
    def test_empty_batch(self, client):
        """Test validation fails for empty batch."""
        batch = {"events": []}
        
        response = client.post("/api/v1/events/batch", json=batch)
        
        assert response.status_code == 422
    
    def test_batch_with_invalid_event(self, client):
        """Test batch with one invalid event."""
        batch = {
            "events": [
                {
                    "event_type": "like",
                    "user_id": "user_valid",
                    "platform": "web"
                },
                {
                    "event_type": "invalid",  # Invalid type
                    "user_id": "user_invalid",
                    "platform": "web"
                }
            ]
        }
        
        response = client.post("/api/v1/events/batch", json=batch)
        
        assert response.status_code == 422
    
    def test_batch_size_limit(self, client):
        """Test batch size limit (max 1000 events)."""
        batch = {
            "events": [
                {
                    "event_type": "view",
                    "user_id": f"user_{i}",
                    "platform": "web"
                }
                for i in range(1001)
            ]
        }
        
        response = client.post("/api/v1/events/batch", json=batch)
        
        assert response.status_code == 422


class TestKafkaFailure:
    """Tests for Kafka connection failures."""
    
    def test_event_ingestion_kafka_down(self, client):
        """Test event ingestion when Kafka is unavailable."""
        with patch('app.main.producer') as mock_producer:
            mock_producer.send_event = AsyncMock(return_value=False)
            
            event = {
                "event_type": "like",
                "user_id": "user_123",
                "platform": "web"
            }
            
            response = client.post("/api/v1/events", json=event)
            
            assert response.status_code == 503  # Service Unavailable
    
    def test_health_check_kafka_down(self, client):
        """Test health check when Kafka is disconnected."""
        with patch('app.main.producer') as mock_producer:
            mock_producer.is_connected = MagicMock(return_value=False)
            
            response = client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "unhealthy"
            assert data["kafka_connected"] is False
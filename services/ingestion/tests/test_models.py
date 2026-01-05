"""
Unit tests for ingestion models.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError
from app.models import (
    EventPayload, EventType, Platform, EventResponse,
    BatchEventPayload, HealthResponse
)


class TestEventPayload:
    """Tests for EventPayload model."""
    
    def test_valid_event_payload(self):
        """Test creating valid event payload."""
        event = EventPayload(
            event_type=EventType.LIKE,
            user_id="user_123",
            platform=Platform.WEB,
            content_id="post_456"
        )
        
        assert event.event_type == EventType.LIKE
        assert event.user_id == "user_123"
        assert event.platform == Platform.WEB
        assert event.content_id == "post_456"
    
    def test_event_payload_with_defaults(self):
        """Test event payload with auto-generated fields."""
        event = EventPayload(
            event_type=EventType.VIEW,
            user_id="user_789",
            platform=Platform.IOS
        )
        
        assert event.event_id is None  # Will be generated in API
        assert event.timestamp is None  # Will be set in API
        assert event.metadata == {}
    
    def test_invalid_user_id_empty(self):
        """Test validation fails for empty user_id."""
        with pytest.raises(ValidationError):
            EventPayload(
                event_type=EventType.LIKE,
                user_id="   ",  # Empty after strip
                platform=Platform.WEB
            )
    
    def test_invalid_event_type(self):
        """Test validation fails for invalid event type."""
        with pytest.raises(ValidationError):
            EventPayload(
                event_type="invalid_type",
                user_id="user_123",
                platform=Platform.WEB
            )
    
    def test_metadata_size_limit(self):
        """Test metadata size validation."""
        large_metadata = {"key": "x" * 1001}
        
        with pytest.raises(ValidationError):
            EventPayload(
                event_type=EventType.LIKE,
                user_id="user_123",
                platform=Platform.WEB,
                metadata=large_metadata
            )
    
    def test_all_platforms(self):
        """Test all platform types are valid."""
        platforms = [Platform.WEB, Platform.IOS, Platform.ANDROID, Platform.MOBILE_WEB]
        
        for platform in platforms:
            event = EventPayload(
                event_type=EventType.CLICK,
                user_id="user_test",
                platform=platform
            )
            assert event.platform == platform


class TestBatchEventPayload:
    """Tests for BatchEventPayload model."""
    
    def test_valid_batch(self):
        """Test valid batch payload."""
        batch = BatchEventPayload(
            events=[
                EventPayload(
                    event_type=EventType.LIKE,
                    user_id="user_1",
                    platform=Platform.WEB
                ),
                EventPayload(
                    event_type=EventType.COMMENT,
                    user_id="user_2",
                    platform=Platform.IOS
                )
            ]
        )
        
        assert len(batch.events) == 2
    
    def test_empty_batch_fails(self):
        """Test empty batch is invalid."""
        with pytest.raises(ValidationError):
            BatchEventPayload(events=[])
    
    def test_batch_size_limit(self):
        """Test batch size limit (max 1000)."""
        events = [
            EventPayload(
                event_type=EventType.VIEW,
                user_id=f"user_{i}",
                platform=Platform.WEB
            )
            for i in range(1001)
        ]
        
        with pytest.raises(ValidationError):
            BatchEventPayload(events=events)


class TestEventResponse:
    """Tests for EventResponse model."""
    
    def test_success_response(self):
        """Test successful response."""
        response = EventResponse(
            status="success",
            event_id="evt_123",
            message="Event ingested successfully"
        )
        
        assert response.status == "success"
        assert response.event_id == "evt_123"
        assert isinstance(response.timestamp, datetime)


class TestHealthResponse:
    """Tests for HealthResponse model."""
    
    def test_healthy_response(self):
        """Test healthy status response."""
        response = HealthResponse(
            status="healthy",
            kafka_connected=True
        )
        
        assert response.status == "healthy"
        assert response.kafka_connected is True
        assert response.service == "ingestion"
    
    def test_unhealthy_response(self):
        """Test unhealthy status response."""
        response = HealthResponse(
            status="unhealthy",
            kafka_connected=False
        )
        
        assert response.status == "unhealthy"
        assert response.kafka_connected is False
"""
Event models for the ingestion service.
"""
from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional
from datetime import datetime
from enum import Enum

class EventType(str, Enum):
    CLICK = "click"
    LIKE = "like"
    COMMENT = "comment"
    SHARE = "share"
    VIEW = "view"
    

class Platform(str, Enum):
    WEB = "web"
    IOS = "ios"
    ANDROID = "android"
    MOBILE_WEB = "mobile_web"

class EventPayload(BaseModel):
    event_id: Optional[str] = Field(None, description="Unique event identifier (generated if not provided)")
    event_type: EventType = Field(..., description="Type of the event")
    user_id: str = Field(..., min_length=1, max_length=100, description="User identifier")
    content_id: Optional[str] = Field(None, max_length=100, description="Content identifier (post, video, etc.)")
    platform: Platform = Field(..., description="Platform where event occurred")
    timestamp: Optional[datetime] = Field(None, description="Event timestamp (auto-generated if not provided)")

    # Optional metadata
    session_id: Optional[str] = Field(None, max_length=100, description="User Session identifier")
    device_type: Optional[str] = Field(None, max_length=50, description="Device type")
    location: Optional[str] = Field(None, max_length=100, description="User location (country/city)")
    metadata: Optional[dict] = Field(default_factory=dict, description="Additional metadata")

    @field_validator('user_id')
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        """Ensure user_id is not empty."""
        if not v.strip():
            raise ValueError("User ID cannot be empty.")
        return v.strip()

    @field_validator('metadata')
    @classmethod
    def validate_metadata(cls, v: Optional[dict]) -> dict:
        """Ensure metadata is not too large."""
        if v and len(str(v)) > 1000:
            raise ValueError("Metadata is too large (Max 1000 characters).")
        return v or {}
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "like",
                "user_id": "user_12345",
                "content_id": "post_67890",
                "platform": "ios",
                "session_id": "sess_abcdef",
                "device_type": "iphone 14",
                "location": "US-CA",
                "metadata": {"duration_ms": 150}
            }
        }

class EventResponse(BaseModel):
    """Response model for event ingestion."""
    status: Literal["success", "error"] = Field(..., description="Status of the operation")
    event_id: str = Field(..., description="Unique event identifier")
    message: str = Field(..., description="Response message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")

class HealthResponse(BaseModel):
    """Health check response model."""

    status: Literal["healthy", "unhealthy"] = Field(..., description="Health status of the service")
    service: str = Field(default="ingestion", description="Name of the service")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the health check")
    kafka_connected: bool = Field(..., description="Kafka connection status")
    version: str = Field(default="1.0.0", description="Service version")

class BatchEventPayload(BaseModel):
    """Model for batch event ingestion."""

    events: list[EventPayload] = Field(..., min_length=1, max_length=1000, description="List of event payloads")

    @field_validator('events')
    @classmethod
    def validate(cls, v: list) -> list:
        """Ensure batch is not empty and not too large."""
        if not v:
            raise ValueError("Event list cannot be empty.")
        if len(v) > 1000:
            raise ValueError("Event list exceeds maximum size of 1000.")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "events": [
                    {
                        "event_type": "like",
                        "user_id": "user_1",
                        "content_id": "post_1",
                        "platform": "web"
                    },
                    {
                        "event_type": "comment",
                        "user_id": "user_2",
                        "content_id": "post_1",
                        "platform": "ios"
                    }
                ]
            }
        }

class BatchEventResponse(BaseModel):
    """Response model for batch event ingestion."""

    status: Literal["success", "partial", "error"] = Field(..., description="Status of the batch operation")
    total_events: int = Field(..., description="Total number of events in the batch")
    successful: int = Field(..., description="Number of successfully ingested events")
    failed: int = Field(..., description="Number of failed events")
    errors: list[str] = Field(default_factory=list, description="Error messages for failed events")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
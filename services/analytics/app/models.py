"""
Data models for analytics service.
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum


class TimeWindow(str, Enum):
    """Time window options for analytics."""
    FIVE_MIN = "5min"
    ONE_HOUR = "1hr"
    ONE_DAY = "1day"


class EventType(str, Enum):
    """Event types."""
    CLICK = "click"
    LIKE = "like"
    COMMENT = "comment"
    SHARE = "share"
    VIEW = "view"


class Platform(str, Enum):
    """Platform types."""
    WEB = "web"
    IOS = "ios"
    ANDROID = "android"
    MOBILE_WEB = "mobile_web"


class EventMetric(BaseModel):
    """Event metrics model."""
    window_start: datetime
    window_end: datetime
    event_type: str
    platform: str
    event_count: int
    unique_users: int
    computed_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "window_start": "2024-01-15T10:00:00",
                "window_end": "2024-01-15T10:05:00",
                "event_type": "like",
                "platform": "ios",
                "event_count": 1250,
                "unique_users": 890,
                "computed_at": "2024-01-15T10:05:30"
            }
        }


class UserMetric(BaseModel):
    """User activity metrics model."""
    window_start: datetime
    window_end: datetime
    user_id: str
    platform: str
    total_events: int
    likes: int
    comments: int
    shares: int
    views: int
    clicks: int
    computed_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "window_start": "2024-01-15T10:00:00",
                "window_end": "2024-01-15T11:00:00",
                "user_id": "user_12345",
                "platform": "web",
                "total_events": 45,
                "likes": 20,
                "comments": 5,
                "shares": 3,
                "views": 15,
                "clicks": 2,
                "computed_at": "2024-01-15T11:00:30"
            }
        }


class EventMetricsResponse(BaseModel):
    """Response for event metrics query."""
    total_records: int
    time_window: str
    metrics: List[EventMetric]


class UserMetricsResponse(BaseModel):
    """Response for user metrics query."""
    total_records: int
    time_window: str
    metrics: List[UserMetric]


class TopUser(BaseModel):
    """Top user by activity."""
    user_id: str
    platform: str
    total_events: int
    rank: int


class TopUsersResponse(BaseModel):
    """Response for top users query."""
    total_users: int
    time_range: str
    top_users: List[TopUser]


class EventDistribution(BaseModel):
    """Event distribution by type."""
    event_type: str
    total_count: int
    percentage: float


class EventDistributionResponse(BaseModel):
    """Response for event distribution query."""
    total_events: int
    time_range: str
    distribution: List[EventDistribution]


class PlatformMetric(BaseModel):
    """Platform-wise metrics."""
    platform: str
    total_events: int
    unique_users: int


class PlatformMetricsResponse(BaseModel):
    """Response for platform metrics."""
    total_platforms: int
    time_range: str
    platforms: List[PlatformMetric]
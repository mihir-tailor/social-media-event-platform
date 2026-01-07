"""
FastAPI application for analytics service.
"""
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from .models import (
    EventMetricsResponse, EventMetric, UserMetricsResponse, UserMetric,
    TopUsersResponse, TopUser, EventDistributionResponse, EventDistribution,
    PlatformMetricsResponse, PlatformMetric, TimeWindow, EventType, Platform
)
from .database import Database, AnalyticsRepository
from .config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    logger.info(f"Starting {settings.SERVICE_NAME} v{settings.SERVICE_VERSION}")
    Database.initialize_pool()
    
    # Test connection
    if Database.test_connection():
        logger.info("Database connection successful")
    else:
        logger.error("Database connection failed")
    
    yield
    
    # Shutdown
    logger.info("Shutting down gracefully...")


# Create FastAPI app
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.SERVICE_VERSION,
    lifespan=lifespan
)

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint."""
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.SERVICE_VERSION,
        "status": "running"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    db_connected = Database.test_connection()
    
    return {
        "status": "healthy" if db_connected else "unhealthy",
        "service": settings.SERVICE_NAME,
        "version": settings.SERVICE_VERSION,
        "database_connected": db_connected
    }


@app.get(
    f"{settings.API_V1_PREFIX}/analytics/events",
    response_model=EventMetricsResponse,
    tags=["Analytics"]
)
async def get_event_metrics(
    time_window: TimeWindow = Query(TimeWindow.FIVE_MIN, description="Time window for aggregation"),
    hours_back: int = Query(1, ge=1, le=168, description="Hours to look back"),
    event_type: Optional[EventType] = Query(None, description="Filter by event type"),
    platform: Optional[Platform] = Query(None, description="Filter by platform"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results to return")
):
    """
    Get event metrics aggregated by time windows.
    
    - **time_window**: Aggregation window (5min, 1hr, 1day)
    - **hours_back**: How many hours of data to retrieve
    - **event_type**: Optional filter by event type
    - **platform**: Optional filter by platform
    - **limit**: Maximum number of records to return
    """
    try:
        results = AnalyticsRepository.get_event_metrics(
            time_window=time_window.value,
            hours_back=hours_back,
            event_type=event_type.value if event_type else None,
            platform=platform.value if platform else None,
            limit=limit
        )
        
        metrics = [EventMetric(**row) for row in results]
        
        return EventMetricsResponse(
            total_records=len(metrics),
            time_window=time_window.value,
            metrics=metrics
        )
        
    except Exception as e:
        logger.error(f"Error fetching event metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    f"{settings.API_V1_PREFIX}/analytics/users",
    response_model=UserMetricsResponse,
    tags=["Analytics"]
)
async def get_user_metrics(
    hours_back: int = Query(1, ge=1, le=168, description="Hours to look back"),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    platform: Optional[Platform] = Query(None, description="Filter by platform"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results to return")
):
    """
    Get user activity metrics.
    
    - **hours_back**: How many hours of data to retrieve
    - **user_id**: Optional filter by specific user
    - **platform**: Optional filter by platform
    - **limit**: Maximum number of records to return
    """
    try:
        results = AnalyticsRepository.get_user_metrics(
            hours_back=hours_back,
            user_id=user_id,
            platform=platform.value if platform else None,
            limit=limit
        )
        
        metrics = [UserMetric(**row) for row in results]
        
        return UserMetricsResponse(
            total_records=len(metrics),
            time_window="1hr",
            metrics=metrics
        )
        
    except Exception as e:
        logger.error(f"Error fetching user metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    f"{settings.API_V1_PREFIX}/analytics/top-users",
    response_model=TopUsersResponse,
    tags=["Analytics"]
)
async def get_top_users(
    hours_back: int = Query(24, ge=1, le=168, description="Hours to look back"),
    limit: int = Query(10, ge=1, le=100, description="Number of top users to return")
):
    """
    Get top users by activity level.
    
    - **hours_back**: Time range for aggregation
    - **limit**: Number of top users to return
    """
    try:
        results = AnalyticsRepository.get_top_users(
            hours_back=hours_back,
            limit=limit
        )
        
        top_users = [
            TopUser(rank=idx + 1, **row)
            for idx, row in enumerate(results)
        ]
        
        return TopUsersResponse(
            total_users=len(top_users),
            time_range=f"Last {hours_back} hours",
            top_users=top_users
        )
        
    except Exception as e:
        logger.error(f"Error fetching top users: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    f"{settings.API_V1_PREFIX}/analytics/event-distribution",
    response_model=EventDistributionResponse,
    tags=["Analytics"]
)
async def get_event_distribution(
    hours_back: int = Query(24, ge=1, le=168, description="Hours to look back")
):
    """
    Get distribution of events by type.
    
    - **hours_back**: Time range for aggregation
    """
    try:
        results = AnalyticsRepository.get_event_distribution(hours_back=hours_back)
        
        # Calculate total and percentages
        total_events = sum(row['total_count'] for row in results)
        
        distribution = [
            EventDistribution(
                event_type=row['event_type'],
                total_count=row['total_count'],
                percentage=round((row['total_count'] / total_events * 100), 2) if total_events > 0 else 0
            )
            for row in results
        ]
        
        return EventDistributionResponse(
            total_events=total_events,
            time_range=f"Last {hours_back} hours",
            distribution=distribution
        )
        
    except Exception as e:
        logger.error(f"Error fetching event distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    f"{settings.API_V1_PREFIX}/analytics/platforms",
    response_model=PlatformMetricsResponse,
    tags=["Analytics"]
)
async def get_platform_metrics(
    hours_back: int = Query(24, ge=1, le=168, description="Hours to look back")
):
    """
    Get metrics by platform.
    
    - **hours_back**: Time range for aggregation
    """
    try:
        results = AnalyticsRepository.get_platform_metrics(hours_back=hours_back)
        
        platforms = [PlatformMetric(**row) for row in results]
        
        return PlatformMetricsResponse(
            total_platforms=len(platforms),
            time_range=f"Last {hours_back} hours",
            platforms=platforms
        )
        
    except Exception as e:
        logger.error(f"Error fetching platform metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
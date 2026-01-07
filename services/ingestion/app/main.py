"""
FastAPI application for event ingestion service.
"""

import logging
import uuid
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .models import (EventPayload, EventResponse, HealthResponse, BatchEventPayload, BatchEventResponse)

from .kafka_producer import producer
from .config import settings

logging .basicConfig(level=getattr(logging, settings.LOG_LEVEL),format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    logger.info(f"Starting {settings.SERVICE_NAME} v{settings.SERVICE_VERSION}")
    logger.info(f"Kafka brokers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka topic: {settings.KAFKA_TOPIC}")

    yield
    # Shutdown
    logger.info("Shutting down gracefully...")
    producer.flush()
    producer.close()
    logger.info("Shutdown complete.")  

#create FastAPI app
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    lifespan=lifespan
)

#add CORS middleware
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

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint for load balancers and monitoring."""
    
    kafka_connected = producer.is_connected()

    return HealthResponse(
        status="healthy" if kafka_connected else "unhealthy",
        kafka_connected=kafka_connected,
        version=settings.SERVICE_VERSION
    )

@app.post(
    f"{settings.API_V1_PREFIX}/events", 
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Events"]
    )
async def ingest_event(event: EventPayload):
    """
    Ingest a single event and publish it to Kafka.
    - event_type: Type of event (click, like, comment, share, view)
    - user_id: User identifier
    - content_id: Content identifier (optional)
    - platform: Platform where event occurred
    - Additional fields: session_id, device_type, location, metadata
    """
    try:
        #Generate event_id if not provided
        if not event.event_id:
            event.event_id = str(uuid.uuid4())  
        
        #Set timestamp if not provided
        if not event.timestamp:
            event.timestamp = datetime.utcnow()
        
        #convert to dict for kafka
        event_data = event.model_dump(mode="json")

        #Ensure timestamp is ISO format string
        if isinstance(event_data.get('timestamp'), datetime):
            event_data['timestamp'] = event_data['timestamp'].isoformat()

        #send to kafka
        sucess = await producer.send_event(event_data, key=event.user_id)
        if not sucess:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Failed to publish event to Kafka.")
        logger.info(f"Event ingested: id={event.event_id}, "
                    f"type={event.event_type}, user={event.user_id} ")
        
        return EventResponse(
            status="success",
            event_id=event.event_id, 
            message="Event ingested sucessfully.")
    
    except HTTPException:
        raise   
    except Exception as e:
        logger.error(f"Error ingesting event: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Internal server error. {str(e)}")
    
@app.post(
    f"{settings.API_V1_PREFIX}/events/batch", 
    response_model=BatchEventResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Events"]
    )
async def ingest_batch(batch: BatchEventPayload):
    """
    Ingest multiple events in a single request.

    Maximum 1000 events per batch.
    Returns partial success status if some events fail.
    """
    try:
        events_to_send = []

        for event in batch.events:
            #Generate event_id if not provided
            if not event.event_id:
                event.event_id = str(uuid.uuid4())  
            
            #Set timestamp if not provided
            if not event.timestamp:
                event.timestamp = datetime.utcnow()
            
            #convert to dict for kafka
            event_data = event.model_dump(mode="json")

            #Ensure timestamp is ISO format string
            if isinstance(event_data.get('timestamp'), datetime):
                event_data['timestamp'] = event_data['timestamp'].isoformat()

            events_to_send.append(event_data)

        #send batch to kafka
        successful, failed = await producer.send_batch(events_to_send)

        #Determine status
        if failed == 0:
            response_status = "success"
        elif successful == 0:
            response_status= "error"
        else:
            response_status = "partial"
    
        logger.info(f"Batch ingested: total={len(batch.events)}, "
                    f"successful={successful}, failed={failed} ")
        
        return BatchEventResponse(
            status= response_status,
            total_events=len(batch.events),
            successful=successful,
            failed=failed,
            errors=[] if failed == 0 else [f"{failed} events failed to publish."]
        )
    except Exception as e:
        logger.error(f"Error ingesting batch: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Internal server error. {str(e)}")
    
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error."}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
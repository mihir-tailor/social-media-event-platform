"""
Test data generator for the social media event platform.
Generates realistic event data and sends to the ingestion API.
"""
import argparse
import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import List
import aiohttp


# Configuration
INGESTION_URL = "http://localhost:8000/api/v1/events"
BATCH_URL = "http://localhost:8000/api/v1/events/batch"

# Event types and their relative frequencies
EVENT_TYPES = {
    "view": 0.40,      # 40% of events
    "like": 0.30,      # 30%
    "click": 0.15,     # 15%
    "comment": 0.10,   # 10%
    "share": 0.05      # 5%
}

PLATFORMS = ["web", "ios", "android", "mobile_web"]
DEVICES = {
    "web": ["Chrome", "Firefox", "Safari", "Edge"],
    "ios": ["iPhone 14", "iPhone 13", "iPad Pro", "iPhone 12"],
    "android": ["Samsung Galaxy", "Pixel 7", "OnePlus", "Xiaomi"],
    "mobile_web": ["Mobile Chrome", "Mobile Safari", "Mobile Firefox"]
}
LOCATIONS = ["US-CA", "US-NY", "UK-LDN", "JP-TKY", "FR-PAR", "DE-BER", "AU-SYD"]


def generate_event() -> dict:
    """Generate a single random event."""
    event_type = random.choices(
        list(EVENT_TYPES.keys()),
        weights=list(EVENT_TYPES.values())
    )[0]
    
    platform = random.choice(PLATFORMS)
    user_id = f"user_{random.randint(1, 10000)}"
    content_id = f"content_{random.randint(1, 1000)}"
    
    event = {
        "event_type": event_type,
        "user_id": user_id,
        "content_id": content_id,
        "platform": platform,
        "session_id": f"sess_{random.randint(100000, 999999)}",
        "device_type": random.choice(DEVICES[platform]),
        "location": random.choice(LOCATIONS),
        "metadata": {
            "duration_ms": random.randint(100, 5000),
            "scroll_depth": random.randint(0, 100)
        }
    }
    
    return event


async def send_event(session: aiohttp.ClientSession, event: dict) -> bool:
    """Send a single event to the ingestion API."""
    try:
        async with session.post(INGESTION_URL, json=event) as response:
            return response.status == 201
    except Exception as e:
        print(f"Error sending event: {e}")
        return False


async def send_batch(session: aiohttp.ClientSession, events: List[dict]) -> bool:
    """Send a batch of events to the ingestion API."""
    try:
        payload = {"events": events}
        async with session.post(BATCH_URL, json=payload) as response:
            return response.status == 201
    except Exception as e:
        print(f"Error sending batch: {e}")
        return False


async def generate_and_send(
    num_events: int,
    rate: int,
    batch_size: int = 100,
    use_batch: bool = True
):
    """
    Generate and send events at specified rate.
    
    Args:
        num_events: Total number of events to generate
        rate: Events per second
        batch_size: Number of events per batch (if using batch API)
        use_batch: Whether to use batch API or single event API
    """
    print(f"Generating {num_events} events at {rate} events/sec...")
    print(f"Using {'batch' if use_batch else 'single'} API")
    print(f"Batch size: {batch_size if use_batch else 'N/A'}")
    print("-" * 50)
    
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        start_time = time.time()
        successful = 0
        failed = 0
        
        if use_batch:
            # Batch mode
            batches = []
            for i in range(0, num_events, batch_size):
                batch = [generate_event() for _ in range(min(batch_size, num_events - i))]
                batches.append(batch)
            
            for i, batch in enumerate(batches):
                success = await send_batch(session, batch)
                if success:
                    successful += len(batch)
                else:
                    failed += len(batch)
                
                # Rate limiting
                elapsed = time.time() - start_time
                expected_time = (i + 1) * batch_size / rate
                if elapsed < expected_time:
                    await asyncio.sleep(expected_time - elapsed)
                
                # Progress update
                if (i + 1) % 10 == 0:
                    print(f"Progress: {successful + failed}/{num_events} events "
                          f"({successful} successful, {failed} failed)")
        
        else:
            # Single event mode
            tasks = []
            for i in range(num_events):
                event = generate_event()
                task = send_event(session, event)
                tasks.append(task)
                
                # Send in waves to control rate
                if len(tasks) >= rate:
                    results = await asyncio.gather(*tasks)
                    successful += sum(results)
                    failed += len(results) - sum(results)
                    tasks = []
                    
                    # Progress update
                    if (i + 1) % 1000 == 0:
                        print(f"Progress: {i + 1}/{num_events} events "
                              f"({successful} successful, {failed} failed)")
                    
                    await asyncio.sleep(1)
            
            # Send remaining tasks
            if tasks:
                results = await asyncio.gather(*tasks)
                successful += sum(results)
                failed += len(results) - sum(results)
        
        end_time = time.time()
        duration = end_time - start_time
        actual_rate = num_events / duration if duration > 0 else 0
        
        print("-" * 50)
        print(f"Completed in {duration:.2f} seconds")
        print(f"Successful: {successful}/{num_events} ({successful/num_events*100:.1f}%)")
        print(f"Failed: {failed}/{num_events}")
        print(f"Actual rate: {actual_rate:.1f} events/sec")


def main():
    parser = argparse.ArgumentParser(
        description="Generate test data for social media event platform"
    )
    parser.add_argument(
        "--events",
        type=int,
        default=1000,
        help="Number of events to generate (default: 1000)"
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Events per second (default: 100)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Events per batch (default: 100)"
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Use single event API instead of batch"
    )
    
    args = parser.parse_args()
    
    asyncio.run(generate_and_send(
        num_events=args.events,
        rate=args.rate,
        batch_size=args.batch_size,
        use_batch=not args.single
    ))


if __name__ == "__main__":
    main()
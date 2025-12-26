"""
BMS Scheduler API - Cloud Scheduler Handler

This service is triggered by Cloud Scheduler on a schedule (e.g., hourly).
It queries BigQuery for active campaigns with BookMyShow URLs and enqueues jobs.
"""

from fastapi import FastAPI, HTTPException
from google.cloud import tasks_v2, bigquery
import json
import os
import logging
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BMS Scheduler Service")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
QUEUE_LOCATION = "us-central1"
QUEUE_NAME = "bhm-scraping-queue"
WORKER_URL = os.getenv("WORKER_URL", f"https://bms-worker-{PROJECT_ID}.run.app")

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "bms-scheduler"}

@app.post("/trigger-scheduled-jobs")
async def trigger_scheduled_jobs():
    """
    Cloud Scheduler webhook endpoint.
    
    1. Query BigQuery for active campaigns with BookMyShow URLs
    2. Extract event IDs from URLs
    3. Create Cloud Tasks jobs for BMS Worker
    4. Return summary of jobs created
    """
    try:
        if not PROJECT_ID:
            raise ValueError("GCP_PROJECT_ID environment variable not set")
        
        logger.info("Starting scheduled job trigger...")
        
        # 1. Query BigQuery for active BMS campaigns
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
        SELECT DISTINCT 
            movie_id, 
            movie_name,
            channels
        FROM `{PROJECT_ID}.blowhorn_apify_raw_dataset.movies`
        WHERE status = 'active'
          AND channels IS NOT NULL
          AND JSON_EXTRACT_SCALAR(channels, '$.bookmyshow') IS NOT NULL
        LIMIT 100
        """
        
        logger.info(f"Querying BigQuery: {query}")
        results = bq_client.query(query).result()
        
        campaigns = list(results)
        logger.info(f"Found {len(campaigns)} active campaigns with BookMyShow URLs")
        
        if not campaigns:
            logger.info("No active campaigns found")
            return {
                "status": "success",
                "jobs_created": 0,
                "message": "No active campaigns with BookMyShow URLs"
            }
        
        # 2. Create Cloud Tasks jobs
        task_client = tasks_v2.CloudTasksClient()
        parent = task_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_NAME)
        
        jobs_created = 0
        jobs_failed = 0
        
        for campaign in campaigns:
            try:
                # Extract BookMyShow URL
                channels = campaign.channels
                if isinstance(channels, str):
                    channels = json.loads(channels)
                
                bms_url = channels.get("bookmyshow") if isinstance(channels, dict) else None
                
                if not bms_url:
                    logger.warning(f"No BookMyShow URL for movie {campaign.movie_id}")
                    continue
                
                # Extract event ID from URL
                event_id = extract_event_id(bms_url)
                if not event_id:
                    logger.warning(f"Could not extract event ID from {bms_url}")
                    continue
                
                logger.info(f"Creating task for campaign: {campaign.movie_name}, Event: {event_id}")
                
                # Prepare task payload
                task_payload = {
                    "city": "MUMBAI",  # Could be extracted from campaign if stored
                    "event_id": event_id,
                    "date": datetime.now().strftime("%Y%m%d"),
                    "limit": 5
                }
                
                # Create Cloud Tasks job
                task = {
                    "http_request": {
                        "http_method": tasks_v2.HttpMethod.POST,
                        "url": f"{WORKER_URL}/process",
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({
                            "message": {
                                "data": __import__('base64').b64encode(
                                    json.dumps(task_payload).encode()
                                ).decode()
                            }
                        }).encode()
                    }
                }
                
                # Add OIDC token for authentication
                iam_client = task_client._transport._credentials
                task["http_request"]["oidc_token"] = {
                    "service_account_email": f"{PROJECT_ID}@appspot.gserviceaccount.com",
                    "audience": WORKER_URL
                }
                
                # Create task
                response = task_client.create_task(request={"parent": parent, "task": task})
                logger.info(f"Task created: {response.name}")
                jobs_created += 1
                
            except Exception as e:
                logger.error(f"Failed to create task for campaign: {str(e)}")
                jobs_failed += 1
                continue
        
        logger.info(f"Scheduled job trigger complete. Created: {jobs_created}, Failed: {jobs_failed}")
        
        return {
            "status": "success",
            "jobs_created": jobs_created,
            "jobs_failed": jobs_failed,
            "total_campaigns": len(campaigns),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/trigger-on-demand")
async def trigger_on_demand(event_id: str, city: str = "MUMBAI"):
    """
    Manually trigger a job for a specific event.
    
    Query params:
    - event_id: BookMyShow event ID (e.g., ET00452447)
    - city: City code (default: MUMBAI)
    """
    try:
        logger.info(f"Creating on-demand task: {city}, Event: {event_id}")
        
        task_client = tasks_v2.CloudTasksClient()
        parent = task_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_NAME)
        
        # Prepare task payload
        task_payload = {
            "city": city.upper(),
            "event_id": event_id,
            "date": datetime.now().strftime("%Y%m%d"),
            "limit": 5
        }
        
        # Create Cloud Tasks job
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{WORKER_URL}/process",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "message": {
                        "data": __import__('base64').b64encode(
                            json.dumps(task_payload).encode()
                        ).decode()
                    }
                }).encode()
            }
        }
        
        # Create task
        response = task_client.create_task(request={"parent": parent, "task": task})
        
        return {
            "status": "success",
            "message": f"Task created for {city} event {event_id}",
            "task_name": response.name
        }
        
    except Exception as e:
        logger.error(f"On-demand trigger error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campaigns")
async def list_active_campaigns():
    """
    List all active campaigns with BookMyShow integration.
    Useful for monitoring which campaigns are scheduled.
    """
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        query = f"""
        SELECT 
            movie_id,
            movie_name,
            status,
            created_at,
            JSON_EXTRACT_SCALAR(channels, '$.bookmyshow') as bookmyshow_url
        FROM `{PROJECT_ID}.blowhorn_apify_raw_dataset.campaigns`
        WHERE status = 'active'
          AND JSON_EXTRACT_SCALAR(channels, '$.bookmyshow') IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 50
        """
        
        results = bq_client.query(query).result()
        campaigns = [dict(row) for row in results]
        
        return {
            "status": "success",
            "count": len(campaigns),
            "campaigns": campaigns
        }
        
    except Exception as e:
        logger.error(f"Error listing campaigns: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def extract_event_id(bms_url: str) -> str:
    """
    Extract event ID from BookMyShow URL.
    
    Examples:
    - https://in.bookmyshow.com/explore/events/ET00452447
    - https://in.bookmyshow.com/mumbai/movies/ET00452447/buy-tickets
    
    Returns: ET00452447
    """
    try:
        # Pattern: ET followed by 8 digits
        match = re.search(r'(ET\d{8})', bms_url)
        if match:
            return match.group(1)
        
        # Try alternate pattern
        match = re.search(r'/event/([^/]+)/', bms_url)
        if match:
            return match.group(1)
        
        logger.warning(f"Could not extract event ID from URL: {bms_url}")
        return None
    except Exception as e:
        logger.error(f"Error extracting event ID: {str(e)}")
        return None

@app.get("/queue-status")
async def queue_status():
    """
    Get current Cloud Tasks queue status.
    Shows pending and failed tasks.
    """
    try:
        task_client = tasks_v2.CloudTasksClient()
        parent = task_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_NAME)
        
        queue = task_client.get_queue(request={"name": parent})
        
        return {
            "status": "success",
            "queue_name": queue.name,
            "state": tasks_v2.Queue.State(queue.state).name,
            "rate_limits": {
                "max_concurrent_dispatches": queue.rate_limits.max_concurrent_dispatches,
                "max_dispatches_per_second": queue.rate_limits.max_dispatches_per_second
            },
            "retry_config": {
                "max_attempts": queue.retry_config.max_attempts,
                "max_backoff": queue.retry_config.max_backoff
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting queue status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )

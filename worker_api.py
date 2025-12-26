"""
BMS Worker API - Cloud Tasks Webhook Handler

This service receives jobs from Cloud Tasks and processes BookMyShow analytics.
It's triggered by:
1. Cloud Scheduler (hourly scheduled jobs)
2. Backend API (on-demand requests)
"""

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import json
import base64
import os
import logging
from datetime import datetime

# Import the existing BMS analytics modules
from modules import scraper, parser, layout, analyzer, bq_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BMS Worker Service")

# Request model
class TaskPayload(BaseModel):
    city: str
    event_id: str
    date: str = ""
    limit: int = 5

class CloudTasksMessage(BaseModel):
    message: dict

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "bms-worker"}

@app.post("/process")
async def process_task(request: Request):
    """
    Cloud Tasks webhook endpoint.
    
    Receives Cloud Tasks message with base64-encoded payload:
    {
        "message": {
            "data": "base64_encoded_json_payload"
        }
    }
    
    Payload format:
    {
        "city": "MUMBAI",
        "event_id": "ET00452447",
        "date": "20251224",
        "limit": 5
    }
    """
    try:
        # Get request body
        envelope = await request.json()
        
        # Extract and decode payload
        payload_data = envelope.get("message", {}).get("data")
        if not payload_data:
            raise ValueError("No message data in request")
        
        # Decode base64 payload
        decoded_bytes = base64.b64decode(payload_data)
        payload = json.loads(decoded_bytes.decode("utf-8"))
        
        logger.info(f"Processing task: {payload}")
        
        # Extract parameters
        city = payload.get("city", "MUMBAI").upper()
        event_id = payload.get("event_id")
        date_code = payload.get("date", "")
        limit = payload.get("limit", 5)
        
        if not event_id:
            raise ValueError("event_id is required")
        
        # Validate city
        CITY_CONFIG = {
            "AHMEDABAD": {"code": "AHD", "lat": "23.039568", "lon": "72.566005"},
            "MUMBAI": {"code": "MUMBAI", "lat": "19.0760", "lon": "72.8777"},
            "VADODARA": {"code": "VAD", "lat": "22.3072", "lon": "73.1812"},
            "SURAT": {"code": "SURT", "lat": "21.1702", "lon": "72.8311"},
            "RAJKOT": {"code": "RAJK", "lat": "22.3039", "lon": "70.8022"}
        }
        
        if city not in CITY_CONFIG:
            raise ValueError(f"City '{city}' not configured")
        
        city_data = CITY_CONFIG[city]
        
        # Initialize BigQuery client
        bq = bq_client.BigQueryHandler()
        
        logger.info(f"Starting BMS analytics for {city} | Event: {event_id}")
        
        # Run the analytics pipeline (existing logic from main.py)
        result = await run_analytics_pipeline(
            city=city,
            city_data=city_data,
            event_id=event_id,
            date_code=date_code,
            limit=limit,
            bq=bq
        )
        
        logger.info(f"Task completed successfully: {result}")
        
        return {
            "status": "success",
            "message": f"Processed {city} event {event_id}",
            "data": result
        }
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Processing error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

async def run_analytics_pipeline(city, city_data, event_id, date_code, limit, bq):
    """
    Run the full BMS analytics pipeline.
    This is extracted from the main.py logic.
    """
    try:
        # 1. Fetch Schedule
        schedule_json = scraper.fetch_schedule(
            event_code=event_id,
            region_code=city_data['code'],
            lat=city_data['lat'],
            lon=city_data['lon'],
            date_code=date_code
        )
        
        if not schedule_json:
            logger.warning(f"No schedule found for {city}")
            return {"rows_processed": 0, "error": "No schedule data"}
        
        # 2. Parse Schedule
        df = parser.parse_schedule_to_df(schedule_json, event_id, city_data['code'])
        
        if df.empty:
            logger.warning(f"No shows found for {city}")
            return {"rows_processed": 0, "error": "No shows in schedule"}
        
        logger.info(f"Found {len(df)} shows in {city}, processing {limit}...")
        
        # 3. Process Shows
        batch_data = []
        processed_count = 0
        
        for index, row in df.head(limit).iterrows():
            try:
                logger.info(f"Processing: {row['VenueName']} @ {row['ShowTime']}")
                
                # Capture screenshot
                import tempfile
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
                    temp_img_path = tmp_file.name
                
                # Capture layout
                import asyncio
                success = asyncio.run(layout.capture_seat_layout(row['TicketLink'], temp_img_path))
                
                if success:
                    # Analyze seats
                    stats = analyzer.analyze_seats(temp_img_path)
                    
                    if stats:
                        # Prepare row for BigQuery
                        bq_row = row.to_dict()
                        bq_row.update(stats)
                        bq_row['City'] = city
                        bq_row['Status'] = 'COMPLETED'
                        bq_row['ProcessedAt'] = datetime.now().isoformat()
                        
                        batch_data.append(bq_row)
                        processed_count += 1
                        
                        logger.info(f"  ✓ Occupancy: {stats['filled_sold']}/{stats['total_seats']}")
                else:
                    logger.warning(f"  ✗ Failed to capture layout")
                
                # Cleanup
                if os.path.exists(temp_img_path):
                    os.remove(temp_img_path)
                    
            except Exception as e:
                logger.error(f"  Error processing show: {str(e)}")
                continue
        
        # 4. Upload to BigQuery
        if batch_data:
            logger.info(f"Uploading {len(batch_data)} rows to BigQuery...")
            
            import pandas as pd
            batch_df = pd.DataFrame(batch_data)
            
            # Define column order
            FINAL_COLUMN_ORDER = [
                "EventId", "VenueCode", "VenueName", "SessionId", "ShowDate", "ShowTime",
                "ShowDateTime", "ScrapeTriggerTime", "TicketLink", "Status",
                "total_seats", "filled_sold", "available", "bestseller", "total_unsold",
                "MovieName", "City"
            ]
            
            # Reindex columns
            batch_df = batch_df.reindex(columns=FINAL_COLUMN_ORDER)
            
            # Save to CSV and upload
            csv_file = f"batch_{city}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            batch_df.to_csv(csv_file, index=False)
            
            bq.load_csv(csv_file)
            
            # Cleanup
            if os.path.exists(csv_file):
                os.remove(csv_file)
            
            logger.info(f"Successfully uploaded {len(batch_data)} rows to BigQuery")
            
            return {
                "rows_processed": processed_count,
                "rows_uploaded": len(batch_data),
                "city": city,
                "event_id": event_id
            }
        else:
            logger.warning("No data to upload")
            return {"rows_processed": 0, "error": "No valid data collected"}
            
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}", exc_info=True)
        raise

@app.post("/process-on-demand")
async def process_on_demand(payload: TaskPayload):
    """
    Direct endpoint for on-demand processing (not via Cloud Tasks).
    Called from backend or manually.
    """
    try:
        CITY_CONFIG = {
            "AHMEDABAD": {"code": "AHD", "lat": "23.039568", "lon": "72.566005"},
            "MUMBAI": {"code": "MUMBAI", "lat": "19.0760", "lon": "72.8777"},
            "VADODARA": {"code": "VAD", "lat": "22.3072", "lon": "73.1812"},
            "SURAT": {"code": "SURT", "lat": "21.1702", "lon": "72.8311"},
            "RAJKOT": {"code": "RAJK", "lat": "22.3039", "lon": "70.8022"}
        }
        
        city = payload.city.upper()
        if city not in CITY_CONFIG:
            raise ValueError(f"City '{city}' not configured")
        
        bq = bq_client.BigQueryHandler()
        
        result = await run_analytics_pipeline(
            city=city,
            city_data=CITY_CONFIG[city],
            event_id=payload.event_id,
            date_code=payload.date,
            limit=payload.limit,
            bq=bq
        )
        
        return {"status": "success", "data": result}
        
    except Exception as e:
        logger.error(f"On-demand processing error: {str(e)}")
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

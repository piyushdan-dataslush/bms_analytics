import os, json
from datetime import datetime, timedelta
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import tasks_v2
import google.auth
from google.protobuf import timestamp_pb2
from modules import scraper, parser
import pytz

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace "*" with your actual frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIG ---
# Required environment variables
PROJECT_ID = os.environ["PROJECT_ID"]  # GCP Project ID
LOCATION = os.environ.get("TASK_QUEUE_LOCATION", "asia-south1")
QUEUE_ID = os.environ.get("TASK_QUEUE_ID", "bms-queue")
WORKER_URL = os.environ["WORKER_URL"]  # e.g., https://bms-worker-xxxx.a.run.app/scrape_session
SERVICE_ACCOUNT_EMAIL = os.environ.get(
    "SERVICE_ACCOUNT_EMAIL",
    f"{PROJECT_ID}@appspot.gserviceaccount.com"  # Default App Engine service account
)

# Validate required environment variables
required_vars = ["PROJECT_ID", "WORKER_URL"]
for var in required_vars:
    if not os.environ.get(var):
        raise ValueError(f"Missing required environment variable: {var}")

# For Cloud Scheduler self-triggering (if needed)
SCHEDULER_URL = os.environ.get("SCHEDULER_URL")  # Optional: Only needed for self-triggering

CITY_CONFIG = {
    "AHMEDABAD": {"code": "AHD", "slug": "ahd", "lat": "23.039568", "lon": "72.566005"},
    "MUMBAI": {"code": "MUMBAI", "slug": "mumbai", "lat": "19.0760", "lon": "72.8777"},
    "VADODARA": {"code": "VADO", "slug": "vad", "lat": "22.3072", "lon": "73.1812"},
    "SURAT": {"code": "SURT", "slug": "surt", "lat": "21.1702", "lon": "72.8311"},
    "RAJKOT": {"code": "RAJK", "slug": "rajk", "lat": "22.3039", "lon": "70.8022"}
}

class CampaignRequest(BaseModel):
    event_id: str
    target_date: str
    end_date: str
    run_time: str

def create_task(url, payload, dt):
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE_ID)
    # _, project = google.auth.default()
    # service_account_email = os.environ.get("SERVICE_ACCOUNT_EMAIL")
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {"service_account_email": SERVICE_ACCOUNT_EMAIL}
        }
    }
    if dt:
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(dt)
        task["schedule_time"] = ts
    client.create_task(request={"parent": parent, "task": task})

def run_logic(req: CampaignRequest):
    print(f"--- Processing Date: {req.target_date} ---")
    
    # 1. Schedule Workers for Today
    for city, cfg in CITY_CONFIG.items():
        data = scraper.fetch_schedule(req.event_id, cfg['code'], cfg['lat'], cfg['lon'], req.target_date)
        if not data: continue
        
        df = parser.parse_schedule_to_df(data, req.event_id, cfg['slug'])
        for _, row in df.iterrows():
            # trigger = row['ShowDateTime'] - timedelta(minutes=15)
            trigger_utc = row['Trigger_Object_UTC']
            if trigger_utc > datetime.now(pytz.utc):
                payload = row.to_dict()
                del payload['Trigger_Object_UTC']
                payload['ShowDateTime'] = str(payload['ShowDateTime'])
                payload['ScrapeTriggerTime'] = str(payload['ScrapeTriggerTime'])
                payload['City'] = city
                create_task(WORKER_URL, payload, trigger_utc)

    # 2. Schedule Self for Next Day
    curr_date = datetime.strptime(req.target_date, "%Y%m%d")
    end_date = datetime.strptime(req.end_date, "%Y%m%d")
    
    if curr_date < end_date:
        next_day = (curr_date + timedelta(days=1)).strftime("%Y%m%d")
        next_run = (curr_date + timedelta(days=1)).replace(
            hour=int(req.run_time.split(":")[0]), 
            minute=int(req.run_time.split(":")[1])
        )
        payload = req.dict()
        payload['target_date'] = next_day
        create_task(SCHEDULER_URL, payload, next_run)
        print(f"Next run scheduled for {next_day}")

@app.post("/process_day")
async def endpoint(req: CampaignRequest, bt: BackgroundTasks):
    bt.add_task(run_logic, req)
    return {"status": "ok"}
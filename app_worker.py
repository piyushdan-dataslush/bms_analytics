import os, tempfile
import sys
import asyncio
from datetime import datetime
from fastapi import FastAPI, Request
from modules import layout, analyzer, bq_client
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
app = FastAPI()
bq = bq_client.BigQueryHandler()

@app.post("/scrape_session")
async def scrape(req: Request):
    data = await req.json()
    print(f"Worker: {data['VenueName']} @ {data['ShowTime']}")
    
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        path = tmp.name

    try:
        success = await layout.capture_seat_layout(data['TicketLink'], path)
        if success:
            stats = analyzer.analyze_seats(path)
            if stats:
                data.update(stats)
                # data['ScrapedAt'] = datetime.now().isoformat()
                data['Status'] = 'COMPLETED'
                bq.stream_data(data)
                return {"status": "success"}
    finally:
        if os.path.exists(path): os.remove(path)
    
    return {"status": "failed"}
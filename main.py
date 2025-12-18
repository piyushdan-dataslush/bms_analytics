import os
import pandas as pd
import argparse
import json
import tempfile
from datetime import datetime
from modules import scraper, parser, layout, analyzer, bq_client

# --- Configuration ---
CITY_CONFIG = {
    "AHMEDABAD": {
        "code": "AHD", 
        "lat": "23.039568", 
        "lon": "72.566005"
    },
    "MUMBAI": {
        "code": "MUMBAI", 
        "lat": "19.0760", 
        "lon": "72.8777"
    },
    "VADODARA": {
        "code": "VAD", 
        "lat": "22.3072", 
        "lon": "73.1812"
    },
    "SURAT": {
        "code": "SURT", 
        "lat": "21.1702", 
        "lon": "72.8311"
    },
    "RAJKOT": {
        "code": "RAJK", 
        "lat": "22.3039", 
        "lon": "70.8022"
    }
}

BATCH_FILE = "current_batch_data.csv"

FINAL_COLUMN_ORDER = [
    "EventId", 
    "VenueCode", 
    "VenueName", 
    "SessionId", 
    "ShowDate", 
    "ShowTime", 
    "ShowDateTime", 
    "ScrapeTriggerTime", 
    "TicketLink", 
    "Status", 
    "total_seats", 
    "filled_sold", 
    "available", 
    "bestseller", 
    "total_unsold", 
    "MovieName",
    "City"
]

def main():
    # 1. Parse Arguments
    arg_parser = argparse.ArgumentParser(description="BookMyShow Analytics Pipeline")
    arg_parser.add_argument("--city", type=str, required=True, help="City Name")
    arg_parser.add_argument("--event", type=str, required=True, help="Event ID")
    arg_parser.add_argument("--date", type=str, default="", help="Date YYYYMMDD")
    arg_parser.add_argument("--limit", type=int, default=5, help="Number of shows to process")
    
    args = arg_parser.parse_args()
    
    city_key = args.city.upper()
    if city_key not in CITY_CONFIG:
        print(f"Error: City '{args.city}' not configured.")
        return

    city_data = CITY_CONFIG[city_key]
    
    # Initialize BigQuery Client
    bq = bq_client.BigQueryHandler()

    print(f"--- Starting Pipeline for {city_key} | Event: {args.event} ---")

    # 2. Scrape Schedule
    schedule_json = scraper.fetch_schedule(
        event_code=args.event,
        region_code=city_data['code'],
        lat=city_data['lat'],
        lon=city_data['lon'],
        date_code=args.date
    )
    
    if not schedule_json:
        print("Failed to fetch schedule.")
        return

    # 3. Parse to DataFrame
    df = parser.parse_schedule_to_df(schedule_json, args.event, city_data['code'])
    
    if df.empty:
        print("No shows found in schedule.")
        return
        
    print(f"Found {len(df)} shows. Processing first {args.limit}...")
    batch_data = []
    print("Done!")

    # 4. Process Sessions
    for index, row in df.head(args.limit).iterrows():
        print(f"\nProcessing: {row['VenueName']} @ {row['ShowTime']}")
        
        # Create a temporary file path for the screenshot
        # We delete this later to save space
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
            temp_img_path = tmp_file.name

        try:
            # A. Capture Screenshot to temp path
            success = layout.capture_seat_layout(row['TicketLink'], temp_img_path)
            
            if success:
                # B. Analyze (Get stats only)
                stats = analyzer.analyze_seats(temp_img_path)
                
                if stats:
                    # C. Merge Data for BigQuery
                    bq_row = row.to_dict()
                    bq_row.update(stats)
                    
                    # Add Metadata
                    # bq_row['ScrapedAt'] = datetime.now().isoformat()
                    bq_row['City'] = city_key
                    bq_row['Status'] = 'COMPLETED'

                    # D. Push to BigQuery
                    # bq.stream_data(bq_row)
                    batch_data.append(bq_row)
                    
                    # Console Log
                    print(f"   Movie: {bq_row['MovieName']} | Occupancy: {stats['filled_sold']}/{stats['total_seats']}")
            else:
                print("   Skipping analysis due to capture failure.")

        except Exception as e:
            print(f"   Error: {e}")

        finally:
            # E. Cleanup: Delete the temp image
            if os.path.exists(temp_img_path):
                os.remove(temp_img_path)

    # --- 3. BULK UPLOAD TO BIGQUERY ---
    if batch_data:
        print("\n--- Uploading Batch to BigQuery ---")
        
        # Convert list of dicts to DataFrame
        batch_df = pd.DataFrame(batch_data)
        batch_df = batch_df.reindex(columns=FINAL_COLUMN_ORDER)
        
        # Save to CSV locally first (Handling the file creation)
        batch_df.to_csv(BATCH_FILE, index=False)
        
        # Upload the CSV
        bq.load_csv(BATCH_FILE)
        
        # Clean up local CSV
        if os.path.exists(BATCH_FILE):
            os.remove(BATCH_FILE)
            print("Local batch file cleaned up.")
    else:
        print("\nNo data collected to upload.")

if __name__ == "__main__":
    main()
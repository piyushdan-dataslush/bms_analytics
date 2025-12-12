import os
import argparse
import time
from datetime import datetime
import json
import pandas as pd

# Import our custom modules
from modules import scraper, parser, layout, analyzer

# --- Configuration for Cities (Extend as needed) ---
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

def main():
    # 1. Parse Arguments
    arg_parser = argparse.ArgumentParser(description="BookMyShow Analytics Pipeline")
    arg_parser.add_argument("--city", type=str, required=True, help="City Name (e.g., Ahmedabad)")
    arg_parser.add_argument("--event", type=str, required=True, help="Event ID (e.g., ET00452447)")
    arg_parser.add_argument("--date", type=str, default="", help="Date YYYYMMDD (Optional, defaults to today/API default)")
    arg_parser.add_argument("--limit", type=int, default=5, help="Number of shows to process (to avoid overloading)")
    
    args = arg_parser.parse_args()
    
    city_key = args.city.upper()
    if city_key not in CITY_CONFIG:
        print(f"Error: City '{args.city}' not configured in CITY_CONFIG.")
        return

    city_data = CITY_CONFIG[city_key]
    
    # 2. Setup Output Directories
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_output_dir = os.path.join("output", f"{args.event}_{timestamp}")
    images_dir = os.path.join(base_output_dir, "images")
    processed_dir = os.path.join(base_output_dir, "processed")
    
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    print(f"--- Starting Pipeline for {city_key} | Event: {args.event} ---")

    # 3. Scrape Schedule (Script 1)
    schedule_json = scraper.fetch_schedule(
        event_code=args.event,
        region_code=city_data['code'],
        lat=city_data['lat'],
        lon=city_data['lon'],
        date_code=args.date
    )
    
    if not schedule_json:
        print("Failed to fetch schedule. Exiting.")
        return

    # Save raw JSON for debugging
    with open(os.path.join(base_output_dir, "raw_schedule.json"), "w") as f:
        json.dump(schedule_json, f, indent=4)

    # 4. Parse to CSV (Script 2)
    df = parser.parse_schedule_to_df(schedule_json, args.event)
    
    if df.empty:
        print("No shows found in schedule.")
        return
        
    csv_path = os.path.join(base_output_dir, "schedule.csv")
    df.to_csv(csv_path, index=False)
    print(f"Schedule CSV saved to: {csv_path}")

    # 5. Process Sessions (Script 3 & 4)
    final_report = []
    
    print(f"Processing first {args.limit} sessions...")
    
    # Loop through the dataframe
    for index, row in df.head(args.limit).iterrows():
        session_id = row['SessionId']
        venue_code = row['VenueCode']
        ticket_link = row['TicketLink']
        
        print(f"\nProcessing {index+1}/{args.limit}: {row['VenueName']} @ {row['ShowTime']}")
        
        # File paths for this specific session
        raw_img_name = f"{venue_code}_{session_id}_raw.png"
        proc_img_name = f"{venue_code}_{session_id}_proc.png"
        raw_img_path = os.path.join(images_dir, raw_img_name)
        proc_img_path = os.path.join(processed_dir, proc_img_name)

        # A. Capture Layout
        success = layout.capture_seat_layout(ticket_link, raw_img_path)
        
        if success:
            # B. Analyze Image
            stats = analyzer.analyze_seats(raw_img_path, proc_img_path)
            
            if stats:
                # Merge row data with stats
                session_result = row.to_dict()
                session_result.update(stats)
                final_report.append(session_result)
        else:
            print("Skipping analysis due to capture failure.")

    # 6. Save Final Report
    report_path = os.path.join(base_output_dir, "final_occupancy_report.json")
    with open(report_path, "w") as f:
        json.dump(final_report, f, indent=4)

    print("\n" + "="*40)
    print(f"Pipeline Complete. Output folder: {base_output_dir}")
    print(f"Final Report: {report_path}")
    print("="*40)

if __name__ == "__main__":
    main()
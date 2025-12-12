import os
import argparse
import pandas as pd
from datetime import datetime
from modules import scraper, parser

# Config
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

QUEUE_FILE = "master_queue.csv"

def main():
    parser_arg = argparse.ArgumentParser()
    parser_arg.add_argument("--event", required=True, help="Event Code (e.g., ET00452447)")
    # Default to today if no date provided
    default_date = datetime.now().strftime("%Y%m%d")
    parser_arg.add_argument("--date", default=default_date, help="Date YYYYMMDD")
    args = parser_arg.parse_args()

    all_city_data = []

    print(f"--- DAILY SCHEDULER STARTING FOR DATE: {args.date} ---")

    for city_name, config in CITY_CONFIG.items():
        print(f"Fetching schedule for: {city_name}...")
        
        json_data = scraper.fetch_schedule(
            event_code=args.event,
            region_code=config['code'],
            lat=config['lat'],
            lon=config['lon'],
            date_code=args.date
        )

        if json_data:
            df = parser.parse_schedule_to_df(json_data, args.event)
            if not df.empty:
                df['City'] = city_name # Add City column for reference
                all_city_data.append(df)
                print(f"-> Found {len(df)} shows.")
            else:
                print("-> No shows found.")
        else:
            print("-> API Error.")

    # Combine and Save
    if all_city_data:
        final_df = pd.concat(all_city_data, ignore_index=True)
        
        # Sort by Trigger Time so the worker picks them up in order
        final_df = final_df.sort_values(by='ScrapeTriggerTime')
        
        # Save to CSV
        final_df.to_csv(QUEUE_FILE, index=False)
        print("="*50)
        print(f"MASTER SCHEDULE GENERATED: {QUEUE_FILE}")
        print(f"Total Tasks Scheduled: {len(final_df)}")
        print("="*50)
    else:
        print("No data found for any city.")

if __name__ == "__main__":
    main()
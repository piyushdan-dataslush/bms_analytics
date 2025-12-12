import pandas as pd
import os
import time
from datetime import datetime
from modules import layout, analyzer

QUEUE_FILE = "master_queue.csv"
OUTPUT_BASE = "output_data"

def process_tasks():
    if not os.path.exists(QUEUE_FILE):
        print("No Master Queue file found. Please run daily_scheduler.py first.")
        return

    # Load Queue
    df = pd.read_csv(QUEUE_FILE)
    
    # Convert string dates back to objects for comparison
    df['ScrapeTriggerTime'] = pd.to_datetime(df['ScrapeTriggerTime'])
    
    current_time = datetime.now()
    
    # FILTER: Find rows that are PENDING and Time is Past or Now
    # Logic: If TriggerTime was 12:15, and now is 12:20, we scrape it.
    mask = (df['Status'] == 'PENDING') & (df['ScrapeTriggerTime'] <= current_time)
    
    pending_tasks = df.loc[mask]
    
    if pending_tasks.empty:
        print(f"[{current_time.strftime('%H:%M')}] No tasks due right now.")
        return

    print(f"--- WORKER STARTED: Found {len(pending_tasks)} due tasks ---")

    # Iterate through pending tasks
    for index, row in pending_tasks.iterrows():
        print(f"Processing: {row['VenueName']} | Show: {row['ShowTime']}")
        
        # 1. Setup paths
        date_str = str(row['ShowDate'])
        session_folder = os.path.join(OUTPUT_BASE, date_str, row['City'])
        os.makedirs(session_folder, exist_ok=True)
        
        raw_image_path = os.path.join(session_folder, f"{row['VenueCode']}_{row['SessionId']}_raw.png")
        proc_image_path = os.path.join(session_folder, f"{row['VenueCode']}_{row['SessionId']}_proc.png")
        json_path = os.path.join(session_folder, f"{row['VenueCode']}_{row['SessionId']}_data.json")

        try:
            # 2. Capture Screenshot (Playwright)
            success = layout.capture_seat_layout(row['TicketLink'], raw_image_path)
            
            if success:
                # 3. Analyze (OpenCV)
                stats = analyzer.analyze_seats(raw_image_path, proc_image_path)
                
                # 4. Save JSON Report for this session
                if stats:
                    import json
                    with open(json_path, "w") as f:
                        # Combine schedule info with occupancy stats
                        full_data = row.to_dict()
                        # Convert Timestamp objects to string for JSON serialization
                        full_data['ShowDateTime'] = str(full_data['ShowDateTime'])
                        full_data['ScrapeTriggerTime'] = str(full_data['ScrapeTriggerTime'])
                        full_data.update(stats)
                        json.dump(full_data, f, indent=4)
                
                # 5. Update Status in DataFrame
                df.at[index, 'Status'] = 'COMPLETED'
                df.at[index, 'ProcessedImage'] = proc_image_path
                print("-> Done.")
            else:
                df.at[index, 'Status'] = 'FAILED_CAPTURE'
                print("-> Failed to capture.")

        except Exception as e:
            print(f"-> Error: {e}")
            df.at[index, 'Status'] = 'ERROR'

        # Save CSV after every task (to prevent data loss if script crashes)
        df.to_csv(QUEUE_FILE, index=False)
        
        # Sleep briefly to be polite to the server
        time.sleep(2)

if __name__ == "__main__":
    process_tasks()
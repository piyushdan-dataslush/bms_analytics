import pandas as pd
from datetime import datetime, timedelta

def parse_schedule_to_df(json_data, event_code):
    all_rows = []
    
    # Safety check: Ensure data exists
    if not json_data or 'data' not in json_data:
        return pd.DataFrame()

    widgets = json_data.get('data', {}).get('showtimeWidgets', [])

    for widget in widgets:
        if widget.get('type') == 'groupList':
            for group in widget.get('data', []):
                if group.get('type') == 'venueGroup':
                    for venue in group.get('data', []):
                        if venue.get('type') == 'venue-card':
                            
                            venue_data = venue.get('additionalData', {})
                            venue_name = venue_data.get('venueName', 'Unknown Venue')
                            venue_code = venue_data.get('venueCode', 'Unknown')
                            
                            for show in venue.get('showtimes', []):
                                show_data = show.get('additionalData', {})
                                
                                session_id = show_data.get('sessionId')
                                
                                # --- FIX: Force string conversion and strip whitespace ---
                                show_date_code = str(show_data.get('showDateCode', '')).strip()
                                show_time_code = str(show_data.get('showTimeCode', '')).strip()
                                show_time_display = show.get('title')
                                
                                # Skip if data is missing
                                if not show_date_code or not show_time_code:
                                    continue
                                
                                try:
                                    # --- NEW LOGIC: Calculate Trigger Time ---
                                    # 1. Combine Date and Time
                                    full_time_str = f"{show_date_code} {show_time_code}"
                                    
                                    # 2. Parse DateTime (Format: 20251212 1245)
                                    show_datetime = datetime.strptime(full_time_str, "%Y%m%d %H%M")
                                    
                                    # 3. Subtract 30 minutes for the trigger
                                    trigger_datetime = show_datetime - timedelta(minutes=30)
                                    
                                    ticket_link = f"https://in.bookmyshow.com/movies/ahd/seat-layout/{event_code}/{venue_code}/{session_id}/{show_date_code}"

                                    row = {
                                        'EventId': event_code,
                                        'VenueCode': venue_code,
                                        'VenueName': venue_name,
                                        'SessionId': session_id,
                                        'ShowDate': show_date_code,
                                        'ShowTime': show_time_display,
                                        'ShowDateTime': show_datetime,
                                        'ScrapeTriggerTime': trigger_datetime, 
                                        'TicketLink': ticket_link,
                                        'Status': 'PENDING',
                                        'ProcessedImage': ''
                                    }
                                    all_rows.append(row)
                                except Exception as e:
                                    print(f"[Parser] Error parsing row {venue_name} {show_time_display}: {e}")
                                    continue

    df = pd.DataFrame(all_rows)
    return df
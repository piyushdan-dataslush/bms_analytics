import pandas as pd
from datetime import datetime, timedelta
import pytz

def parse_schedule_to_df(json_data, event_code, region_code):
    """
    Parses BMS JSON response into a Pandas DataFrame.
    Extracts Movie Name and details.
    """
    all_rows = []
    
    # Safety check
    if not json_data or 'data' not in json_data:
        return pd.DataFrame()

    # --- 1. EXTRACT MOVIE NAME ---
    try:
        # Path based on standard BMS API response structure
        movie_name = json_data.get('data', {}).get('header', {}).get('title', {}).get('text', 'Unknown Movie')
    except:
        movie_name = 'Unknown Movie'

    # Define Timezones
    ist_zone = pytz.timezone('Asia/Kolkata')
    utc_zone = pytz.utc

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
                                
                                # Force string conversion and strip
                                show_date_code = str(show_data.get('showDateCode', '')).strip()
                                show_time_code = str(show_data.get('showTimeCode', '')).strip()
                                show_time_display = show.get('title')
                                
                                if not show_date_code or not show_time_code:
                                    continue
                                
                                try:
                                    # 1. Parse as Naive Time (e.g. 18:45)
                                    naive_dt = datetime.strptime(f"{show_date_code} {show_time_code}", "%Y%m%d %H%M")
                                    
                                    # 2. Localize to IST (It is now 6:45 PM India Time)
                                    ist_dt = ist_zone.localize(naive_dt)
                                    
                                    # 3. Calculate Trigger (15 mins before)
                                    trigger_ist = ist_dt - timedelta(minutes=15)
                                    
                                    # 4. Convert Trigger to UTC (This is what Cloud Tasks needs)
                                    # Example: 6:30 PM IST becomes 1:00 PM UTC
                                    trigger_utc = trigger_ist.astimezone(utc_zone)
                                    
                                    # Ticket Link
                                    ticket_link = f"https://in.bookmyshow.com/movies/{region_code.lower()}/seat-layout/{event_code}/{venue_code}/{session_id}/{show_date_code}"

                                    row = {
                                        'MovieName': movie_name, # <--- Added Field
                                        'EventId': event_code,
                                        'VenueCode': venue_code,
                                        'VenueName': venue_name,
                                        'SessionId': int(session_id) if session_id else 0, # Ensure Int for BQ
                                        'ShowDate': show_date_code,
                                        'ShowTime': show_time_display,
                                        'ShowDateTime': str(ist_dt),
                                        'ScrapeTriggerTime': str(trigger_ist),
                                        'Trigger_Object_UTC': trigger_utc,
                                        'TicketLink': ticket_link,
                                        'Status': 'PENDING'
                                    }
                                    all_rows.append(row)
                                except Exception as e:
                                    continue

    df = pd.DataFrame(all_rows)
    return df
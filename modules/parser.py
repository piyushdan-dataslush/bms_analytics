import pandas as pd
from datetime import datetime, timedelta

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
                                    # Calculate DateTimes
                                    full_time_str = f"{show_date_code} {show_time_code}"
                                    show_datetime = datetime.strptime(full_time_str, "%Y%m%d %H%M")
                                    
                                    # Calculate Trigger (30 mins before)
                                    trigger_datetime = show_datetime - timedelta(minutes=30)
                                    
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
                                        'ShowDateTime': show_datetime,
                                        'ScrapeTriggerTime': trigger_datetime,
                                        'TicketLink': ticket_link,
                                        'Status': 'PENDING'
                                    }
                                    all_rows.append(row)
                                except Exception as e:
                                    continue

    df = pd.DataFrame(all_rows)
    return df
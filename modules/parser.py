import pandas as pd

def parse_schedule_to_df(json_data, event_code):
    """
    Parses BMS JSON response into a Pandas DataFrame.
    """
    all_rows = []
    
    # Traverse Widgets
    widgets = json_data.get('data', {}).get('showtimeWidgets', [])

    for widget in widgets:
        if widget.get('type') == 'groupList':
            for group in widget.get('data', []):
                if group.get('type') == 'venueGroup':
                    for venue in group.get('data', []):
                        if venue.get('type') == 'venue-card':
                            
                            venue_add_data = venue.get('additionalData', {})
                            venue_name = venue_add_data.get('venueName')
                            venue_code = venue_add_data.get('venueCode')
                            
                            for show in venue.get('showtimes', []):
                                show_add_data = show.get('additionalData', {})
                                
                                session_id = show_add_data.get('sessionId')
                                show_date = show_add_data.get('showDateCode')
                                show_time_title = show.get('title')
                                style_id = show.get('styleId')
                                screen_attr = show.get('screenAttr', 'Standard')
                                
                                # Construct Ticket Link
                                ticket_link = f"https://in.bookmyshow.com/movies/ahd/seat-layout/{event_code}/{venue_code}/{session_id}/{show_date}"

                                row = {
                                    'EventId': event_code,
                                    'Date': show_date,
                                    'VenueCode': venue_code,
                                    'VenueName': venue_name,
                                    'SessionId': session_id,
                                    'ShowTime': show_time_title,
                                    'Style_Id': style_id,
                                    'Format': screen_attr,
                                    'TicketLink': ticket_link
                                }
                                all_rows.append(row)

    df = pd.DataFrame(all_rows)
    return df
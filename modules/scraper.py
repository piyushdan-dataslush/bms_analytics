from curl_cffi import requests
import json
import os

def fetch_schedule(event_code, region_code, lat, lon, date_code=""):
    """
    Fetches movie schedule from BMS API.
    """
    url = "https://in.bookmyshow.com/api/movies-data/v4/showtimes-by-event/primary-dynamic"

    query_params = {
        "eventCode": event_code,
        "dateCode": date_code,
        "isDesktop": "true",
        "regionCode": region_code,
        "xLocationShared": "false",
        "lat": lat,
        "lon": lon
    }

    headers = {
        "Authority": "in.bookmyshow.com",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://in.bookmyshow.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "x-platform": "Web",
        "x-region-code": region_code
    }

    print(f"[Scraper] Fetching schedule for {event_code}...")
    try:
        response = requests.get(
            url, 
            params=query_params, 
            headers=headers, 
            impersonate="chrome110",
            timeout=15
        )

        if response.status_code != 200:
            print(f"[Scraper] Error: Status {response.status_code}")
            return None

        return response.json()

    except Exception as e:
        print(f"[Scraper] Error: {e}")
        return None
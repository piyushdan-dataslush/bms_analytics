import requests

# Replace with your deployed Scheduler URL
URL = "https://bms-scheduler-660956715067.asia-south1.run.app/process_day"

payload = {
    "event_id": "ET00452447",
    "target_date": "20251219",
    "end_date": "20251219",
    "run_time": "06:00"
}

print("Triggering Single Day Test...")
resp = requests.post(URL, json=payload)
print(resp.json())
# BookMyShow Analytics Pipeline

This tool automates the process of fetching movie schedules, scraping seat layouts, and analyzing seat occupancy using Computer Vision.

## Features
1. **Schedule Scraper**: Fetches JSON data for a specific movie & city.
2. **Data Parser**: Converts JSON to CSV and generates ticket links.
3. **Layout Scraper**: Uses Playwright to screenshot the seat map (handling popups).
4. **Analyzer**: Uses OpenCV to count Sold, Available, and Bestseller seats.

## Prerequisites

1. **Python 3.8+**
2. **Google Chrome** installed on the machine.

## Installation

1. Clone the repository.
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt

## 1. Install Playwright browsers

```bash
playwright install chromium
```
## 2. Usage

Run the main.py script with the required arguments:
```bash
python main.py --city Ahmedabad --event ET00452447
```

Optional Arguments

--date: Format YYYYMMDD (e.g., 20251212).
If omitted, uses the default date provided by BMS.

--limit: Number of shows to process (default is 5).
Use a higher number to process the full list.

 Example:
 ```bash
 python main.py --city Ahmedabad --event ET00452447 --date 20251212 --limit 10
```

### How to Run This

1.  Create the folder structure as shown above.
2.  Paste the code into the respective files.
3.  Open your terminal in the `bms_analytics` folder.
4.  Run: `pip install -r requirements.txt`
5.  Run: `playwright install chromium`
6.  Run the script:
    ```bash
    python main.py --city Ahmedabad --event ET00452447 --limit 3
    ```

This structure separates concerns (Scraping vs Analysis), makes debugging easier (files are separated), and handles the dynamic naming convention you requested using the `datetime` module in `main.py`.
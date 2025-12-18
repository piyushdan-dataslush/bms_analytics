from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
from dotenv import load_dotenv

load_dotenv()
# --- CONFIG ---
# KEY_PATH = os.getenv("KEY_PATH")  # Path to your downloaded key
PROJECT_ID = os.getenv("PROJECT_ID")     # Replace with your GCP Project ID
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")

class BigQueryHandler:
    def __init__(self):
        try:
            self.client = bigquery.Client(project=PROJECT_ID)
            self.table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            print("[BQ] Client initialized via ADC.")
        except Exception as e:
            print(f"[BQ] Error initializing client: {e}")
            self.client = None

    def load_csv(self, file_path):
        """
        Uploads CSV with Explicit Schema to prevent Type Mismatches.
        """
        if not self.client:
            return

        # --- CRITICAL FIX: Define Schema Explicitly ---
        # This prevents BigQuery from guessing "20251218" is an Integer.
        # It forces it to be loaded into the String column.
        schema_definition = [
            bigquery.SchemaField("EventId", "STRING"),
            bigquery.SchemaField("VenueCode", "STRING"),
            bigquery.SchemaField("VenueName", "STRING"),
            bigquery.SchemaField("SessionId", "INTEGER"),
            bigquery.SchemaField("ShowDate", "STRING"), # <--- The Fix
            bigquery.SchemaField("ShowTime", "STRING"),
            bigquery.SchemaField("ShowDateTime", "STRING"),
            bigquery.SchemaField("ScrapeTriggerTime", "STRING"),
            bigquery.SchemaField("TicketLink", "STRING"),
            bigquery.SchemaField("Status", "STRING"),
            bigquery.SchemaField("total_seats", "INTEGER"),
            bigquery.SchemaField("filled_sold", "INTEGER"),
            bigquery.SchemaField("available", "INTEGER"),
            bigquery.SchemaField("bestseller", "INTEGER"),
            bigquery.SchemaField("total_unsold", "INTEGER"),
            bigquery.SchemaField("MovieName", "STRING"),
            bigquery.SchemaField("City", "STRING")
            # bigquery.SchemaField("ScrapedAt", "STRING"), # Ensure this matches your BQ table
        ]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1, # Skip the header row in CSV
            schema=schema_definition, # Apply our strict schema
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # Allow 'jagged' rows just in case text contains newlines
            allow_quoted_newlines=True 
        )

        try:
            with open(file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file, 
                    self.table_ref, 
                    job_config=job_config
                )

            print(f"[BQ] Uploading {file_path}...")
            job.result()  # Waits for the job to complete.

            print(f"[BQ] Success! Loaded {job.output_rows} rows into {TABLE_ID}.")
            
        except Exception as e:
            print(f"[BQ] Critical Error loading CSV: {e}")
            # Print detailed error if available
            if hasattr(e, 'errors'):
                print(f"[BQ] Error Details: {e.errors}")
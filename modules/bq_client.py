import os
import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryHandler:
    def __init__(self):
        try:
            self.project_id = os.getenv("PROJECT_ID", "tenacious-camp-357012")
            self.dataset_id = os.getenv("DATASET_ID", "blowhorn_apify_raw_dataset")
            self.client = bigquery.Client(project=self.project_id)
            print(f"[BQ] Client initialized for project: {self.project_id}")
        except Exception as e:
            print(f"[BQ] Error initializing client: {e}")
            self.client = None

    def get_movie_initials(self, movie_name: str) -> str:
        """
        Example: 'Vande Bharat Via USA' -> 'VBVU'
        Example: 'Pushpa 2: The Rule' -> 'P2TR'
        """
        if not movie_name or str(movie_name).lower() == 'nan':
            return "MOVIE"
        
        # 1. Remove special characters but keep spaces and numbers
        clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', str(movie_name))
        
        # 2. Split by whitespace
        words = clean_name.split()
        
        if not words:
            return "M"

        # 3. Take the first character of every word and uppercase it
        initials = "".join([word[0] for word in words]).upper()
        
        # 4. BigQuery Safety: Table names cannot start with a number. 
        # If the first initial is a digit (e.g. '12th Fail' -> '1F'), prefix it.
        if initials[0].isdigit():
            initials = f"M{initials}"
            
        return initials

    def _get_schema(self):
        """Standardized schema for BMS data."""
        return [
            bigquery.SchemaField("EventId", "STRING"),
            bigquery.SchemaField("VenueCode", "STRING"),
            bigquery.SchemaField("VenueName", "STRING"),
            bigquery.SchemaField("SessionId", "INTEGER"),
            bigquery.SchemaField("ShowDate", "STRING"),
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
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("ProcessedAt", "TIMESTAMP"),
        ]

    def _ensure_table_exists(self, movie_name: str):
        """Generates initials and creates the table if it does not exist."""
        initials = self.get_movie_initials(movie_name)
        table_name = f"{initials}_bms_data"
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            return table_id
        except NotFound:
            print(f"[BQ] Table {table_name} not found. Creating table for movie: {movie_name}")
            schema = self._get_schema()
            table = bigquery.Table(table_id, schema=schema)
            
            # Partitioning by day makes queries cheaper and faster
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ProcessedAt"
            )
            
            self.client.create_table(table)
            print(f"[BQ] Successfully created table: {table_id}")
            return table_id

    def stream_data(self, row_dict):
        """Streams a single row to a movie-specific initials table."""
        if not self.client:
            print("[BQ] Client not initialized")
            return False

        try:
            clean_row = row_dict.copy()
            movie_name = clean_row.get("MovieName", "Default")
            
            # Ensure target table exists based on initials
            target_table_id = self._ensure_table_exists(movie_name)

            # --- Data Cleaning ---
            for key, value in clean_row.items():
                if "Time" in key or "Date" in key:
                    clean_row[key] = str(value) if value is not None else ""
            
            int_fields = ['SessionId', 'total_seats', 'filled_sold', 'available', 'bestseller', 'total_unsold']
            for key in int_fields:
                if key in clean_row:
                    try:
                        val = clean_row[key]
                        # Handle strings, floats, and Nones
                        clean_row[key] = int(float(val)) if val and str(val).strip() != 'None' else 0
                    except:
                        clean_row[key] = 0

            # Add Processed Timestamp
            clean_row['ProcessedAt'] = datetime.utcnow().isoformat()

            # --- Insert ---
            errors = self.client.insert_rows_json(target_table_id, [clean_row])
            
            if errors:
                print(f"[BQ] Stream Error: {errors}")
                return False
            else:
                print(f"[BQ] Success: Data streamed to {target_table_id}")
                return True

        except Exception as e:
            print(f"[BQ] Critical error in stream_data: {str(e)}")
            return False

    def load_csv(self, file_path, movie_name):
        """Used for batch uploads from local CSVs."""
        if not self.client: return
        
        target_table_id = self._ensure_table_exists(movie_name)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=self._get_schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True 
        )

        try:
            with open(file_path, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, target_table_id, job_config=job_config)
            job.result()
            print(f"[BQ] Loaded batch to {target_table_id}: {job.output_rows} rows.")
        except Exception as e:
            print(f"[BQ] CSV Load Error: {e}")
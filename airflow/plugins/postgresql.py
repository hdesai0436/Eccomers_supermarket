import psycopg2
import pandas as pd
import os
# from dotenv import load_dotenv
from io import StringIO
from google.cloud import storage
from datetime import datetime

# os.environ['key.json'] = 'C:/Users/hardi/Documents/Eccomers/config/key.json'
# creds  = os.getenv('key.json')

class PostgreSQLExtractor:
    def __init__(self, gcs_bucket_name):
        self.gcs_bucket_name = gcs_bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.gcs_bucket_name)

    def create_connection(self):
        """Create a connection to the PostgreSQL database."""
        try:
            conn = psycopg2.connect(
                dbname='eccomer',
                user='postgres',
                password='desai123',
                host='10.80.16.4',
                port='5432'
            )
            return conn
        except Exception as e:
            raise e

    def upload_to_gcs(self, csv_data, gcs_file_name):
        """Uploads CSV data to Google Cloud Storage."""
        try:
            blob = self.bucket.blob(gcs_file_name)
            blob.upload_from_string(csv_data.getvalue(), content_type='text/csv')
        except Exception as e:
            raise e

    def extract_table_to_gcs(self, conn, table_name):
        """Extracts data from a table and uploads it to Google Cloud Storage."""
        try:
            query = f'SELECT * FROM {table_name}'
            df = pd.read_sql(query, conn)

            # Convert DataFrame to CSV in-memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Get current date
            current_date = datetime.now().strftime('%Y-%m-%d')

            # Define GCS file path with table name and current date
            gcs_file_name = f"raw/{current_date}/{table_name}/{table_name}.csv"
            
            # Upload to GCS
            self.upload_to_gcs(csv_buffer, gcs_file_name)

        except Exception as e:
            raise e

    def extract_multiple_tables_to_gcs(self, conn, tables):
        """Extracts data from multiple tables and uploads it to Google Cloud Storage."""
        try:
            for table in tables:
                self.extract_table_to_gcs(conn, table)
        except Exception as e:
            raise e
        finally:
            conn.close()  # Ensure the connection is closed




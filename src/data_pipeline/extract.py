import sys
import os
sys.path.append('/home/airflow/gcs')
from plugins.config.postgresql import PostgreSQLExtractor

gcs_bucket_name = 'eccomer_supermarket'

    # List of tables to extract
tables = ['categories','customer','order_items','orders','products','promotion','promotion_product','returned_items','sales','store_locations','inventory'] # Replace with your actual table names

# Initialize the PostgreSQLExtractor with GCS bucket
extractor = PostgreSQLExtractor(gcs_bucket_name=gcs_bucket_name)

# Create PostgreSQL connection
conn = extractor.create_connection()

# Extract data from tables and upload to GCS
extractor.extract_multiple_tables_to_gcs(conn, tables)


    



      
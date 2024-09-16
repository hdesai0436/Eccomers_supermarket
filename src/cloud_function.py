from google.cloud import bigquery

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    file_name = event['name']
    bucket_name = event['bucket']
    dct = {
        'Event_ID': context.event_id,
        'Event_type': context.event_type,
        'Bucket_name': event['bucket'],
        'File_name': event['name'],
        'Created': event['timeCreated'],
        'Updated': event['updated']
    }

    project_id = "hive-spark"  # Project ID
    dataset_id = "eccomer_data"  # Only dataset name, not full project:dataset
    table_id = "fact"  # Only table name, not full project:dataset.table
    table_name = extract_table_name(file_name)
    print(table_name)

    load_data_from_gcs_to_bigquery(project_id, dataset_id, table_name, bucket_name, file_name)


def extract_table_name(file_name):
    """
    Extracts the table name from the file path.
    Assumes the file path is in the format: folder_name/file_name (e.g., sales_table/file.csv)
    """
    # Split the file name by '/' to get the folder (which is the table name)
    parts = file_name.split("/")
    
    if len(parts) > 1:
        # The first part is assumed to be the table name (folder name)
        return parts[0]
    return None

def load_data_from_gcs_to_bigquery(project_id, dataset_id, table_name, bucket_name, file_name):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define BigQuery table reference
    dataset_ref = client.dataset(dataset_id)  # Corrected: only dataset name
    table_ref = dataset_ref.table(table_name)

    # Define the URI of the file in GCS
    uri = f"gs://{bucket_name}/{file_name}"

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        
    )

    # Start the load job
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

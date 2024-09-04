from google.cloud import bigquery, storage
import base64
import json

def clean_field_names(record):
    new_record = {}
    for key, value in record.items():
        new_key = key.replace('/', '_').replace(' ', '_')
        new_record[new_key] = value
    return new_record

def load_to_bigquery(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_data = json.loads(pubsub_message)
    bucket_name = message_data['bucket']
    file_name = message_data['name']

    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    json_data = blob.download_as_text()
    records = json.loads(json_data)
    cleaned_records = clean_field_names(records)
    
    dataset_id = 'just-camera-432415-h9.001projectbigquery'
    table_id = 'covidIndia'
    
    # Check if record already exists
    query = f"""
    SELECT COUNT(1) as cnt FROM `{dataset_id}.{table_id}` 
    WHERE State_UTs = @State_UTs AND Population = @Population
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("State_UTs", "STRING", cleaned_records['State_UTs']),
            bigquery.ScalarQueryParameter("Population", "INT64", cleaned_records['Population']),
        ]
    )
    
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    
    for row in results:
        if row.cnt == 0:  # If no duplicate, insert the record
            load_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )
            load_job = client.load_table_from_json(
                [cleaned_records], 
                f"{dataset_id}.{table_id}",
                job_config=load_job_config,
            )
            load_job.result()  # Wait for the job to complete
            print(f"Loaded {file_name} into {dataset_id}.{table_id}.")
        else:
            print(f"Duplicate record found: {cleaned_records['State_UTs']}, not loaded.")

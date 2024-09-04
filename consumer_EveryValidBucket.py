from kafka import KafkaConsumer
import json
from google.cloud import storage
import os

def is_valid_json(data):
    try:
        json.loads(data)
        return True
    except ValueError as e:
        return False

def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

# Kafka topic from which messages will be consumed
topic = 'my-topic'
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         group_id='my-group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Start consuming messages and write each one to a JSON file
for message in consumer:
    json_data = json.dumps(message.value)
    if is_valid_json(json_data):
        file_name = f"consumer_message_{message.offset}.json"
        with open(file_name, 'w') as json_file:
            json_file.write(json_data)
        
        # Upload the valid JSON file to GCS
        upload_to_gcs('covid_streamingfiles', file_name, file_name)
        os.remove(file_name)
        print(f"Valid JSON message with offset {message.offset} written to {file_name} and uploaded to GCS.")
    else:
        print(f"Invalid JSON message at offset {message.offset} skipped.")

consumer.close()

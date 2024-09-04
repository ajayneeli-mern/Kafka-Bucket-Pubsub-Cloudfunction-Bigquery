from kafka import KafkaConsumer
import json
from google.cloud import storage
import os

# Initialize Google Cloud Storage client
client = storage.Client()

# Your GCS bucket name
bucket_name = 'covid_streamingfiles'
bucket = client.bucket(bucket_name)

# Kafka topic from which messages will be consumed
topic = 'my-topic'

# Create a KafkaConsumer instance
consumer = KafkaConsumer(topic,
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='my-group',
                         value_deserializer=lambda x: 
                         json.loads(x.decode('utf-8')))

# Start consuming messages and upload each one to GCS
for message in consumer:
    # Create a file name using the offset of the message
    file_name = f"covid_message_{message.offset}.json"
    
    # Convert the message value to JSON string
    json_content = json.dumps(message.value, indent=4)
    
    # Upload the JSON content to GCS
    blob = bucket.blob(file_name)
    blob.upload_from_string(json_content, content_type='application/json')
    
    print(f"Message with offset {message.offset} uploaded to GCS as {file_name}")

# Close the consumer when done
consumer.close()

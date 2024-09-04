from kafka import KafkaProducer
import json,time
import pandas as pd

# Kafka topic to which messages will be sent
topic = 'my-topic'

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))
#read csv 
df= pd.read_csv("Latest Covid-19 India Status.csv")


# Send a message to the Kafka topic
while True:
    message = df.sample(1).to_dict(orient="records")[0]
    producer.send(topic, message)
    time.sleep(10)



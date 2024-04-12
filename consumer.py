from kafka import KafkaConsumer
from pymongo import MongoClient
import json


consumer = KafkaConsumer(
    'iot-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group')

client = MongoClient('localhost:27017')
collection = client.iotdata.iotdata


for message in consumer:
    message = message.value
    message = message.decode().replace("'", '"')
    message = json.loads(message)
    collection.insert_one(message)
    print('{} added to {}'.format(message, collection))

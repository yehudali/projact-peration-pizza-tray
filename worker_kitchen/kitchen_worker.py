from confluent_kafka import Consumer
import os
import json
from pymongo import MongoClient
import time

## mongo
client =  MongoClient()
db = client['projact-peration-pizza-tray'] 
coll = db['orders']

## kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:29092')
conf:dict = {'bootstrap.servers':KAFKA_BOOTSTRAP_SERVERS,
             "group.id": "kitchen-team",
    "auto.offset.reset": "earliest"}

consumer = Consumer(conf)
consumer.subscribe(['orders'])
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            # decod from kafka:
        try:
            value = msg.value().decode("utf-8")
            data = json.loads(value)
            print(f"The data out of KAFKA: {data}")

            # updat to mongo:
            # try:
            #     # x = coll.update_one({}, {$set:{UPDATED_DATA}})
            #     # print('is insert to mongo:{x}')
            # except Exception as e:
            #     print(f"insert to mongo err:{e}")

        except Exception as e:
            print(e)
finally:
    consumer.close()
    

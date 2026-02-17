from confluent_kafka import Consumer
import os
import json
from pymongo import MongoClient
import time
import redis

## mongo
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://root:root123@localhost:27017/')
client =  MongoClient(MONGO_URL)
db = client['projact-peration-pizza-tray'] 
coll = db['orders']

## kafka
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv('KAFKA_PORT', '29092')

# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 'localhost:29092')
conf:dict = {'bootstrap.servers':f'{KAFKA_HOST}:{KAFKA_PORT}',
             "group.id": "kitchen-team",
    "auto.offset.reset": "earliest"}

consumer = Consumer(conf)
consumer.subscribe(['pizza-orders'])

## redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
CONF_REDIS =  {"host":REDIS_HOST, "port":6379,"decode_responses":True}
redis_client = redis.Redis(**CONF_REDIS)

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

            #slep דימוי אבטחת המטען:
            time.sleep(15)

            #  decod from kafka:
            #  {'order_id': 'order_1001',
            #  'pizza_type': 'Margherita', 
            # 'size': 'Medium', 'quantity': 1,
            #  'is_delivery': False,
            #  'special_instructions': 'The eagle has landed. Leave the box at the designated dead drop.', 
            # 'status': 'PREPARING'}

            # updat to mongo:
            try:
                id = data['order_id']
                is_in = coll.update_one({"order_id":id},{"$set":{"status":"DELIVERED"}})
                print(is_in)

                c = redis_client.getdel(id)
                print(f"dell in redis: {c}")
                
            except Exception as e:
                print(f"insert to mongo err:{e}")

        except Exception as e:
            print(e)
finally:
    consumer.close()
    

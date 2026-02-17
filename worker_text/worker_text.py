from confluent_kafka import Consumer
import os
import json
from pymongo import MongoClient
import time
import redis
import string

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
             "group.id": "text-team",
    "auto.offset.reset": "earliest"}

consumer = Consumer(conf)
consumer.subscribe(['pizza-orders'])


def clean_text(text):
    if not text:
        return ""
    text = text.translate(str.maketrans('', '', string.punctuation))
    return text.upper()

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            # decod from kafka:
        try:
            value = msg.value().decode("utf-8") # type: ignore
            data = json.loads(value)

            # updat to mongo:
            try:
                id = data['order_id']

                instructions = data.get('special_instructions', "")

                keywords = ["allergy", "peanut", "gluten"]
                is_allergy_flagged = any(word in instructions.lower() for word in keywords)
                cleaned_protocol = clean_text(instructions)
                is_in = coll.update_one({"order_id":id},{"$set":{"status":"DELIVERED"}})
                print(is_in)


                ##
                update_result = coll.update_one(
                {"order_id": id},
                {"$set": {
                    "allergies_flagged": is_allergy_flagged,
                    "protocol_cleaned": cleaned_protocol
                }}
            )
                print(f"Processed Order {id}: Allergy Flag: {is_allergy_flagged}")
                
            except Exception as e:
                print(f"insert to mongo err:{e}")

        except Exception as e:
            print(e)
finally:
    consumer.close()
    

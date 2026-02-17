from fastapi import FastAPI, UploadFile, HTTPException, status
from pydantic import BaseModel, Field
import uvicorn
from typing import Optional
import uuid
import json
import os
from pymongo import MongoClient
from confluent_kafka import Producer
import redis


## env's:
MONGO_URL = os.getenv('MONGO_URL', "mongodb://root:root123@localhost:27017/")
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv('KAFKA_PORT', '29092')
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
## fastapi
app = FastAPI()

## MONGO 
claient = MongoClient(MONGO_URL)
db = claient['projact-peration-pizza-tray']
coll = db['orders']

## kafka
CONFIG_PRODUCAR:dict = {"bootstrap.servers":f'{KAFKA_HOST}:{KAFKA_PORT}'}
producer = Producer(CONFIG_PRODUCAR)

## redis
CONF_REDIS =  {"host":REDIS_HOST, "port":6379,"decode_responses":True}
redis_client = redis.Redis(**CONF_REDIS)

class InOrders(BaseModel):
    order_id: str= Field(default_factory=lambda: str(uuid.uuid4()))
    pizza_type: str = Field(...) 
    size: str
    quantity: int
    is_delivery: bool = False
    special_instructions: Optional[str] = Field("")
    status: str = "PREPARING"

def delivery_report(err, msg):
    if err is not None:
        print(f'insert maseg to kafka is error: {err}')
    else:
        print(f"insert maseg to kafka in topic: {msg.topic()} [{msg.partition()}]  @ offset")


@app.post("/uploadfile/")
def uplaud_orders_file(dhe_file: UploadFile):
    content = dhe_file.file.read() 
    try:
        data_list = json.loads(content)
        orders = []
        for order in data_list:
            order_obj = InOrders(**order)
            order_dict = order_obj.model_dump()

            # mongo
            coll.insert_one(order_dict.copy())

            # kafka
            valu_insert = json.dumps(order_dict).encode("utf-8")
            producer.produce(topic='pizza-orders', value=valu_insert, callback=delivery_report)

            # python
            orders.append(order_dict)

        producer.flush()
        return {"status": "success", "count len": len(orders)}

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error processing file\n {str(e)}")

@app.get("/order/{order_id}")
def get_order_by_id(order_id:str):
    redis_cheq = redis_client.get(order_id)
    if redis_cheq:
        return {"source": "redis_cache", "value:": json.loads(redis_cheq)}
    order = coll.find_one({"order_id" : f'order_{order_id}'}, {"_id":0})
    if order:
        try:
            redis_client.set(order_id,json.dumps(order),30)
            return {"source": "mongo","valu": order}
        except  Exception as e:
            print(e)
            return {"error":"{e}"}


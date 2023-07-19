from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import config

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'my-topic',  # topic 이름
    bootstrap_servers= config.KAFKA_SERVER,  # 카프카 브로커의 주소
    auto_offset_reset='latest',
    enable_auto_commit=True  # 자동으로 커밋하도록 설정
)

# MongoDB 연결
client = MongoClient('localhost', 27017)
db = client[config.DATABASE]
mycollection = db[config.COLLECTION]

# 카프카에서 데이터를 가져와서 MongoDB에 저장
for message in consumer:
    a = json.loads(message.value.decode('utf-8'))
    print(a)
    mycollection.insert_one({'item': a})  # MongoDB에 저장
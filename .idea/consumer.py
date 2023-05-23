from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'testApi4',  # topic 이름
    bootstrap_servers='10.100.54.43:9092',  # 카프카 브로커의 주소
    auto_offset_reset='earliest',  # 가장 처음부터 데이터를 가져오도록 설정
    enable_auto_commit=True  # 자동으로 커밋하도록 설정
)

# MongoDB 연결
client = MongoClient('39.127.90.220', 27017)
db = client['TestAPI']
mycollection = db['try1']

# 카프카에서 데이터를 가져와서 MongoDB에 저장
for message in consumer:
    a = json.loads(message.value.decode('utf-8'))
    print(a)
    mycollection.insert_one({'item': a})  # MongoDB에 저장
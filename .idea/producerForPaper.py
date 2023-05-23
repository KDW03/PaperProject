import json
import requests
import xmltodict
from kafka import KafkaProducer

# 논문 데이터 api 주소
API_URL = "http://plus.kipris.or.kr/kipo-api/kipi/patUtiModInfoSearchSevice/getAdvancedSearch?astrtCont=%EB%B0%9C%EB%AA%85&inventionTitle=%EC%84%BC%EC%84%9C&ServiceKey=Xpb17ooet47xBN4bt9uy/tgcCRo=QCDRqVppx1sKkT8="
# 카프카 서버 ip
KAFKA_SERVER = '10.100.54.43:9092'
# 카프카 토픽 name
KAFKA_TOPIC = 'testApi4'


# process_api_response로 처리한 데이터를 카프카에 전송하는 함수
def send_data_to_kafka(data):
    try:
        producer.send(KAFKA_TOPIC, value=data)
    except Exception as e:
        print("Error occurred:", str(e))


# reponse를 파싱하는 함수
def process_api_response(api_response):
    content = api_response.content
    dict_type = xmltodict.parse(content)
    body = dict_type['response']['body']
    items = body['items']['item']

    for item in items:
        print(item)
        send_data_to_kafka(item)


# Kafka producer 객체
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


try:
    response = requests.get(API_URL)
    if response.status_code == 200:
        process_api_response(response)
        producer.flush()
    else:
        print("API request failed.")
except requests.exceptions.RequestException as e:
    print("Error occurred during API request:", str(e))

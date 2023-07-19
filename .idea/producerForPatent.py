import json
import math
import config


import requests
import xmltodict
from kafka import KafkaProducer

# 카프카 서버 IP
KAFKA_SERVER = config.KAFKA_SERVER

# 카프카 토픽 이름
KAFKA_TOPIC = config.KAFKA_TOPIC

# API 관련 상수
NUM_OF_ROWS = 500
# 검색년도범위(0~10)
YEAR = 10
SERVICE_KEY = config.PATENT_SERVICE_KEY

# 특허 데이터 API 주소 생성 함수
def generate_api_url(keyword, pageNo):
    return f"http://plus.kipris.or.kr/kipo-api/kipi/patUtiModInfoSearchSevice/getWordSearch?word={keyword}&numOfRows={NUM_OF_ROWS}&pageNo={pageNo}&year={YEAR}&ServiceKey={SERVICE_KEY}"


# process_api_response로 처리한 데이터를 카프카에 전송하는 함수
def send_data_to_kafka(data):
    try:
        print(data)
        producer.send(KAFKA_TOPIC, value=data)
    except Exception as e:
        print("Error occurred:", str(e))


# response를 파싱하는 함수
def process_api_response(api_response):
    content = api_response.content
    dict_type = xmltodict.parse(content)
    items = dict_type['response']['body']['items']
    item_list = items.get('item',[])

    for item in item_list:

        if item is None:
            continue

        send_data_to_kafka(item)


# Kafka producer 객체
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


try:
    keywords = config.keywords

    for keyword in keywords:
        # 최초의 API 요청을 보내서 전체 아이템 개수를 얻음
        response = requests.get(generate_api_url(keyword, 1))
        if response.status_code == 200:
            process_api_response(response)
            content = response.content
            dict_type = xmltodict.parse(content)
            total_items = int(dict_type['response']['count']['totalCount'])
            max_pages = math.ceil(total_items / NUM_OF_ROWS)

            # 각 페이지에 대해 API 요청 반복
            for page in range(2, max_pages + 1):
                response = requests.get(generate_api_url(keyword, page))
                if response.status_code == 200:
                    process_api_response(response)
                else:
                    print(f"API request failed for page {page}.")
                    break
        else:
            print("API request failed.")
except requests.exceptions.RequestException as e:
    print("Error occurred during API request:", str(e))
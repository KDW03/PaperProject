from kafka import KafkaProducer
import requests
import xmltodict
import json

# Kafka producer 객체
producer = KafkaProducer(bootstrap_servers='10.100.54.43:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# api 주소
api_url = "http://plus.kipris.or.kr/kipo-api/kipi/patUtiModInfoSearchSevice/getAdvancedSearch?astrtCont=%EB%B0%9C%EB%AA%85&inventionTitle=%EC%84%BC%EC%84%9C&ServiceKey=Xpb17ooet47xBN4bt9uy/tgcCRo=QCDRqVppx1sKkT8="

# request 객체 생성
response = requests.get(api_url)

# request를 성공적으로 받으면 topic만들어서 kafka server에 보냄
if response.status_code == 200:
    content = response.content
    # XML 형태를 딕셔너리 형태로 변경
    dict_type = xmltodict.parse(content)
    # 결과에서 body 부분만 추출
    body = dict_type['response']['body']
    # item 부분 추출
    data = body['items']['item']

    for item in data:
        print(item)
        try:
            producer.send('testApi4', value=item)
        except:
            print("오류")
    producer.flush()

else:
    print("실패")

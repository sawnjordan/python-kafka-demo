import json
import time
import requests
from kafka import KafkaProducer
from constants import ORDER_KAFKA_TOPIC, ORDER_LIMIT, BOOTSTRAP_SERVERS

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

API_URL = "http://localhost:5000/api/v1/order"

print("Going to be generating order after 3 seconds")
print("Will generate one unique order every 3 seconds")
time.sleep(3)

for i in range(ORDER_LIMIT):
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
        print(f"Done Sending Order ID {data['order_id']}")
    else:
        print(f"Failed to fetch data from API: {response.status_code}")

    time.sleep(3)

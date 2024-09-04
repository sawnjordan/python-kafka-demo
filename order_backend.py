import json
import time

from kafka import KafkaProducer
from constants import ORDER_KAFKA_TOPIC,ORDER_LIMIT

producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Going to be generating order after 3 seconds")
print("Will generate one unique order every 3 seconds")
time.sleep(3)

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"sanjog_lama_{i}",
        "total_cost": i * 50,
        "items": "burger,sandwich",
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"Done Sending..{i}")
    time.sleep(3)
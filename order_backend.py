import json
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 10

producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Going to be generating order after 3 seconds")
print("Will generate one unique order every 3 seconds")
time.sleep(5)

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"sanjog_{i}",
        "total_cost": i * 5,
        "items": "burger,sandwich",
    }

    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"Done Sending..{i}")
    time.sleep(3)
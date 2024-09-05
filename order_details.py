import json
import time
import requests
from kafka import KafkaProducer
from constants import ORDER_KAFKA_TOPIC, ORDER_LIMIT, BOOTSTRAP_SERVERS

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

# API URL to fetch order data
API_URL = "http://localhost:5000/api/v1/order"

def fetch_order_data():
    """
    Fetch order data from the API and return it as a dictionary.
    
    Returns:
        dict: Order data if request is successful, else None.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


def main():
    """
    Main function to generate orders and send them to Kafka.
    """
    print("Going to be generating orders after 3 seconds")
    print("Will generate one unique order every 3 seconds")
    time.sleep(3)

    for i in range(ORDER_LIMIT):
        data = fetch_order_data()
        if data:
            try:
                producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
                print(f"Done Sending Order ID {data.get('order_id', 'unknown')}")
            except Exception as e:
                print(f"Error sending message to Kafka: {e}")
        else:
            print("No data to send to Kafka")
        
        time.sleep(3)


if __name__ == "__main__":
    main()


"""This module processes Kafka messages related to transactions and confirms them."""

import json
from kafka import KafkaConsumer, KafkaProducer
from constants import ORDER_KAFKA_TOPIC, ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS

def main():
    """Main function to consume messages from Kafka and produce confirmation messages."""
    consumer = KafkaConsumer(
        ORDER_KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="order-processing-group",  # Specify the group ID
        auto_offset_reset='earliest',       # Start reading from the beginning of the topic
    )
    
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    try:
        print("Gonna start listening")
        for message in consumer:
            print("Ongoing transaction..")
            consumed_message = json.loads(message.value.decode())
            print(consumed_message)
            user_id = consumed_message.get("user_id")
            total_cost = consumed_message.get("total_cost")
            
            if user_id is not None and total_cost is not None:
                data = {
                    "customer_id": user_id,
                    "customer_email": f"{user_id}@gmail.com",
                    "total_cost": total_cost
                }
                print("Successful transaction..")
                producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
                producer.flush()  # Ensure messages are sent before continuing
    
    except KeyboardInterrupt:
        print("Interrupted. Closing connections...")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Properly close the producer and consumer
        consumer.close()
        producer.close()
        print("Connections closed.")

if __name__ == "__main__":
    main()

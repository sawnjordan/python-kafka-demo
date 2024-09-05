"""
Analytics service to consume Kafka messages and update analytics.
"""

import json
from kafka import KafkaConsumer
from constants import ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Create a Kafka consumer instance
def create_consumer(topic, servers):
    """
    Create and return a Kafka consumer instance.
    
    Args:
        topic (str): The Kafka topic to consume from.
        servers (str): Comma-separated list of Kafka bootstrap servers.
    
    Returns:
        KafkaConsumer: The Kafka consumer instance.
    """
    return KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        auto_offset_reset='earliest',  # Start from the earliest message
        enable_auto_commit=True,       # Auto-commit offsets
        group_id='analytics-group'     # Consumer group ID
    )

def process_message(message):
    """
    Process each Kafka message and update analytics.
    
    Args:
        message (KafkaConsumerMessage): The Kafka message to process.
    """
    global total_orders_count, total_revenue
    total_orders_count = 0
    total_revenue = 0
    try:
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message.get("total_cost", 0))  # Default to 0 if key is missing
        total_orders_count += 1
        total_revenue += total_cost

        # Output results
        print(f"Orders so far today: {total_orders_count}")
        print(f"Revenue so far today: {total_revenue}")
    except (json.JSONDecodeError, KeyError):
        print("Error decoding JSON message or missing 'total_cost' field")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    """
    Main function to start the Kafka consumer and process messages.
    """
    global total_orders_count, total_revenue

    consumer = create_consumer(ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS)
    print("Gonna start listening")

    try:
        for message in consumer:
            process_message(message)
    except KeyboardInterrupt:
        print("Interrupted by user. Exiting...")
    finally:
        consumer.close()  # Ensure the consumer is closed properly
        print("Consumer closed")

if __name__ == "__main__":
    main()


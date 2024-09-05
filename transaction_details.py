"""
This module processes Kafka messages related to transactions and confirms them.
"""

import json
from kafka import KafkaConsumer, KafkaProducer
from constants import ORDER_KAFKA_TOPIC, ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS

class KafkaTransactionProcessor:
    def __init__(self):
        """Initialize the Kafka consumer and producer."""
        self.consumer = KafkaConsumer(
            ORDER_KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="order-processing-group",  # Specify the group ID
            auto_offset_reset='earliest',       # Start reading from the beginning of the topic
        )
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    def process_messages(self):
        """Process incoming Kafka messages and produce confirmation messages."""
        try:
            print("Starting to listen for messages...")
            for message in self.consumer:
                print("Processing transaction...")
                consumed_message = json.loads(message.value.decode())
                print(consumed_message)

                # Extract details from the message
                user_id = consumed_message.get("user_id")
                total_cost = consumed_message.get("total_cost")
                user_email = consumed_message.get("user_email")
                username = consumed_message.get("username")

                if user_id is not None and total_cost is not None:
                    # Create confirmation message
                    confirmation_data = {
                        "customer_id": user_id,
                        "customer_name": username,
                        "customer_email": user_email,
                        "total_cost": total_cost
                    }
                    print("Transaction confirmed.")
                    # Send confirmation message to Kafka topic
                    self.producer.send(
                        ORDER_CONFIRMED_KAFKA_TOPIC,
                        json.dumps(confirmation_data).encode("utf-8")
                    )
                    # Ensure messages are sent before continuing
                    self.producer.flush()

        except KeyboardInterrupt:
            print("Interrupted. Closing connections...")
        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            # Properly close the producer and consumer
            self.consumer.close()
            self.producer.close()
            print("Connections closed.")

def main():
    """Main function to start the Kafka transaction processor."""
    processor = KafkaTransactionProcessor()
    processor.process_messages()

if __name__ == "__main__":
    main()

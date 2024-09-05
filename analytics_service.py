"""
Analytics service to consume Kafka messages and update analytics.
"""

import json
from kafka import KafkaConsumer, KafkaError
from constants import ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS


class AnalyticsService:
    """
    A service that consumes Kafka messages to update order analytics.
    """

    def __init__(self, topic: str, servers: str):
        """
        Initialize the AnalyticsService with Kafka consumer.

        Args:
            topic (str): The Kafka topic to consume from.
            servers (str): Comma-separated list of Kafka bootstrap servers.
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=servers,
            auto_offset_reset='earliest',  # Start from the earliest message
            enable_auto_commit=True,       # Auto-commit offsets
            group_id='analytics-group'     # Consumer group ID
        )
        self.total_orders_count = 0
        self.total_revenue = 0.0

    def process_message(self, message):
        """
        Process each Kafka message and update analytics.

        Args:
            message (KafkaConsumerMessage): The Kafka message to process.
        """
        try:
            consumed_message = json.loads(message.value.decode())
            total_cost = float(
                consumed_message.get("total_cost", 0)  # Default to 0 if key is missing
            )
            self.total_orders_count += 1
            self.total_revenue += total_cost

            # Output results
            print(f"Orders so far today: {self.total_orders_count}")
            print(f"Revenue so far today: {self.total_revenue}")
        except json.JSONDecodeError:
            print("Error decoding JSON message.")
        except KeyError:
            print("Missing 'total_cost' field in the message.")
        except KafkaError as e:
            print(f"Kafka error occurred: {e}")
        except Exception as e:
            # Log unexpected exceptions
            print(f"An unexpected error occurred: {e}")

    def run(self):
        """
        Start consuming messages and processing them.
        """
        print("Gonna start listening")

        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print("Interrupted by user. Exiting...")
        finally:
            self.consumer.close()  # Ensure the consumer is closed properly
            print("Consumer closed")


def main():
    """
    Main function to start the AnalyticsService.
    """
    service = AnalyticsService(ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS)
    service.run()


if __name__ == "__main__":
    main()

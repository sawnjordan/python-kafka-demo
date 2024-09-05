"""
This module handles sending emails based on Kafka messages.
"""

import json
import kafka
from constants import ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS

class KafkaEmailSender:
    def __init__(self):
        """Initialize the Kafka consumer and set up tracking for unique emails."""
        self.consumer = kafka.KafkaConsumer(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='email-sender-group'
        )
        self.emails_sent_so_far = set()

    def process_message(self, kafka_message):
        """Process each Kafka message to send emails."""
        try:
            consumed_message = json.loads(kafka_message.value.decode())
            customer_email = consumed_message.get("customer_email")
            if customer_email:
                if customer_email not in self.emails_sent_so_far:
                    print(f"Sending email to {customer_email}")
                    self.emails_sent_so_far.add(customer_email)
                    print(f"So far, emails sent to {len(self.emails_sent_so_far)} unique emails")
                else:
                    print(f"Email already sent to {customer_email}")
            else:
                print("Received message without 'customer_email' field")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {e}")
        except kafka.errors.KafkaError as e:
            print(f"Kafka error occurred: {e}")

    def start_listening(self):
        """Start listening for messages and processing them."""
        print("Starting to listen for messages...")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print("Interrupted. Closing consumer...")
        finally:
            self.consumer.close()
            print("Consumer closed")

def main():
    """Main function to start the Kafka email sender."""
    email_sender = KafkaEmailSender()
    email_sender.start_listening()

if __name__ == "__main__":
    main()

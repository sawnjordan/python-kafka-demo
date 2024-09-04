import json
import signal
import sys
from kafka import KafkaConsumer
from constants import ORDER_CONFIRMED_KAFKA_TOPIC, BOOTSTRAP_SERVERS

# Initialize Kafka consumer
consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',  # Start reading from the earliest message
    enable_auto_commit=True,       # Auto-commit offsets
    group_id='email-sender-group'  # Consumer group ID
)

# Set to track unique emails
emails_sent_so_far = set()

def signal_handler(sig, frame):
    print("Signal received, shutting down gracefully...")
    consumer.close()  # Close the consumer connection
    sys.exit(0)

# Register signal handler for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def process_message(message):
    try:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message.get("customer_email")
        if customer_email:
            print(f"Sending email to {customer_email}")
            emails_sent_so_far.add(customer_email)
            print(f"So far emails sent to {len(emails_sent_so_far)} unique emails")
        else:
            print("Received message without 'customer_email' field")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON message: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

print("Gonna start listening")

# Main loop to listen for messages
try:
    for message in consumer:
        process_message(message)
except Exception as e:
    print(f"Consumer error: {e}")
finally:
    consumer.close()  # Ensure the consumer is closed properly

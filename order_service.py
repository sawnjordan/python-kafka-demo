from flask import Flask, jsonify, request
import json
import random
import re
from kafka import KafkaProducer
from constants import ORDER_KAFKA_TOPIC, BOOTSTRAP_SERVERS

class KafkaOrderService:
    def __init__(self):
        # Initialize Kafka producer
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    def calculate_total_cost(self, items):
        """Calculate the total cost based on the items."""
        total_cost = 0.0
        for item in items:
            item_name = item.get('item_name')
            qty = item.get('qty', 1)  # Default quantity to 1 if not provided
            price = item.get('price', 0.0)  # Default price to 0 if not provided
            total_cost += qty * price
        return round(total_cost, 2)

    def is_valid_email(self, email):
        """Validate the email format using a simple regex."""
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(email_regex, email) is not None

    def create_order(self, data):
        """Create a new order and send it to Kafka."""
        if not data or 'user_id' not in data or 'items' not in data or 'username' not in data or 'user_email' not in data:
            return {
                "success": False,
                "message": "Invalid input. 'user_id', 'items', 'username', and 'user_email' are required."
            }, 400

        user_email = data['user_email']
        username = data['username']
        
        if not self.is_valid_email(user_email):
            return {
                "success": False,
                "message": "Invalid email format."
            }, 400

        items = data.get('items')
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            return {
                "success": False,
                "message": "'items' must be a list of objects."
            }, 400

        # Calculate total cost from items
        total_cost = self.calculate_total_cost(items)

        order = {
            "order_id": random.randint(1, 1000),
            "user_id": data['user_id'],
            "username": username,
            "user_email": user_email,
            "total_cost": total_cost,
            "items": items
        }

        # Send order to Kafka
        self.producer.send(ORDER_KAFKA_TOPIC, json.dumps(order).encode('utf-8'))
        self.producer.flush()  # Ensure the message is sent before responding

        return {
            "success": True,
            "message": "Order placed successfully.",
            "confirmation": {
                "order_id": order["order_id"],
                "total_cost": order["total_cost"],
                "username": order["username"],
                "user_email": order["user_email"]
            }
        }, 201

app = Flask(__name__)
order_service = KafkaOrderService()

@app.route('/api/v1/order', methods=['POST'])
def create_order():
    """Create a new order using the KafkaOrderService class."""
    data = request.json
    response, status_code = order_service.create_order(data)
    return jsonify(response), status_code

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)

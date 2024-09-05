"""This module provides a Flask API for order generation."""

import random
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/api/v1/order', methods=['GET'])
def get_order():
    """Generate a sample order."""
    order = {
        "order_id": random.randint(1, 1000),
        "user_id": f"user_{random.randint(1, 1000)}",
        "total_cost": round(random.uniform(10.0, 100.0), 2),
        "items": "burger,sandwich"
    }
    return jsonify(order)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)

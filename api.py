from flask import Flask, jsonify
import random

app = Flask(__name__)

@app.route('/api/v1/order', methods=['GET'])
def get_order():
    # Generate random data for the order
    order_id = random.randint(1, 1000)
    user_id = f"user_{order_id}"
    total_cost = random.uniform(10.0, 100.0)
    items = "burger,sandwich"  # You can expand this to include more items or generate them randomly

    data = {
        "order_id": order_id,
        "user_id": user_id,
        "total_cost": total_cost,
        "items": items,
    }

    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

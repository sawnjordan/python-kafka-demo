# python-kafka-demo

### Steps to run the project

- Create a virtual environment

  `python -m venv myenv`

- Activate the virtual environment:

  `source myenv/bin/activate`

- Install Dependencies

  `pip install -r requirements.txt`

- Activate the Zookeeper and Kafka server

  `docker compose up -d`

- Run transaction backend

  `python3 transaction_details.py`

- Run email service

  `python3 email_service.py`

- Run order backend

  `python3 order_details.py`

- Run api to post order details

  `python3 order_service.py`

- You need to use POST method with below JSON payload

```js
{
    "user_id": "user_2",
    "items": [
        {
          "item_name": "Black T-shirt", "qty": 2, "price": 500.0
        },
        {
          "item_name": "Jeans", "qty": 5, "price": 5800
        }
    ],
    "username": "sanjog",
    "user_email": "sanjog@example.com"
}
```

### To check the list of topics

```
docker exec -it <container_id> kafka-topics --list --bootstrap-server localhost:9092
```

### To delete topics

```
docker exec -it <container_id> kafka-topics --delete --topic <topic_name> --bootstrap-server localhost:9092
```

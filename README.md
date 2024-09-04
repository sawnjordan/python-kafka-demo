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

- Run api to get random order details

  `python3 api.py`

- Run email service

  `python3 email_service.py`

- Run transaction backend

  `python3 transaction_details.py`

- Run order backend

  `python3 order_details.py`

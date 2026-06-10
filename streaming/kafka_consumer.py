import json
import os
import sys

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(0, ROOT_DIR)

from kafka import KafkaConsumer
from config import config

consumer = KafkaConsumer(
    config.KAFKA_ORDER_TOPIC,
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    ),
    auto_offset_reset="earliest"
)

print("Listening for orders...\n")

for message in consumer:

    order = message.value

    print(
        f"Order ID: {order['order_id']} | "
        f"City: {order['city']} | "
        f"Amount: ₹{order['amount_inr']}"
    )
import json
import time
import logging
import sys
import os

sys.path.append(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)

from kafka import KafkaProducer
from ingestion.event_generator import MeeshoEventGenerator
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class MeeshoKafkaProducer:

    def __init__(self):

        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        logger.info(
            f"Connected to {config.KAFKA_BOOTSTRAP_SERVERS}"
        )

    def publish_order(self, order_data):

        self.producer.send(
            topic=config.KAFKA_ORDER_TOPIC,
            value=order_data
        )

        self.producer.flush()


if __name__ == "__main__":

    gen = MeeshoEventGenerator()

    producer = MeeshoKafkaProducer()

    while True:

        order = gen.generate_order_event()

        producer.publish_order(
            order.to_dict()
        )

        print("Published:", order.order_id)

        time.sleep(1)
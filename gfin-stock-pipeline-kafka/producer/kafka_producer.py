from kafka import KafkaProducer
import json
from common.logger import get_logger

log = get_logger("producer")

class StockProducer:
    def __init__(self, servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def send(self, record):
        self.producer.send(self.topic, record)
        log.info(f"Sent → {record['ticker']} @ {record['price']}")

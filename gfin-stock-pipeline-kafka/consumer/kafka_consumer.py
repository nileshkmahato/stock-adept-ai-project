from kafka import KafkaConsumer
import json

def create_consumer(servers, topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock-consumer",
    )

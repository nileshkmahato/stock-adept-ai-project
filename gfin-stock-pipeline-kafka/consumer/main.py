import yaml
from consumer.kafka_consumer import create_consumer
from consumer.gcs_writer import GCSWriter
from consumer.bigquery_writer import BigQueryWriter
from common.logger import get_logger

log = get_logger("consumer-main")
cfg = yaml.safe_load(open("config/app.yaml"))

consumer = create_consumer(
    cfg["kafka"]["bootstrap_servers"],
    cfg["kafka"]["topic"]
)

gcs = GCSWriter(
    cfg["storage"]["gcs_bucket"],
    cfg["storage"]["gcs_prefix"]
)

bq = BigQueryWriter(
    cfg["storage"]["bq"]["project_id"],
    cfg["storage"]["bq"]["dataset"],
    cfg["storage"]["bq"]["table"]
)

for msg in consumer:
    record = msg.value

    gcs.write(record)
    bq.insert(record)

    log.info(f"Processed → {record['ticker']}")

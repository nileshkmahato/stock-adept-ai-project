from google.cloud import bigquery
from common.logger import get_logger
from datetime import datetime

log = get_logger("bigquery-writer")

class BigQueryWriter:
    def __init__(self, project, dataset, table):
        self.client = bigquery.Client(project=project)
        self.table_id = f"{project}.{dataset}.{table}"

    def insert(self, record):
        row = {
            **record,
            "ingestion_time": datetime.utcnow().isoformat()
        }

        errors = self.client.insert_rows_json(self.table_id, [row])

        if errors:
            log.error(f"BigQuery insert errors: {errors}")
        else:
            log.info(f"BigQuery inserted → {record['ticker']}")

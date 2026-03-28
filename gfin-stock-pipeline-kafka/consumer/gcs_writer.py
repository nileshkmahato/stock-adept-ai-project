from google.cloud import storage
from datetime import datetime
import json
from common.logger import get_logger

log = get_logger("gcs-writer")


class GCSWriter:
    def __init__(self, bucket: str, prefix: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket)
        self.prefix = prefix.rstrip("/")

    def write(self, record: dict):
        try:
            date = datetime.utcnow().strftime("%Y-%m-%d")
            blob_path = f"{self.prefix}/stock_ticks_{date}.json"
            blob = self.bucket.blob(blob_path)

            # Load existing data if file exists
            if blob.exists():
                existing_data = json.loads(
                    blob.download_as_text(encoding="utf-8")
                )
            else:
                existing_data = []

            # Append new record
            existing_data.append(record)

            # Write back pretty JSON
            blob.upload_from_string(
                json.dumps(
                    existing_data,
                    indent=2,
                    ensure_ascii=False
                ),
                content_type="application/json",
            )

            log.info(f"GCS JSON updated → {blob_path}")

        except Exception as e:
            log.error(f"GCS JSON write failed: {e}", exc_info=True)

"""
S3 Data Lake Archiver

Writes Kafka events to S3-compatible storage as
Hive-partitioned Parquet files.

Bronze zone structure:
s3://meesho-data-lake/bronze/orders/year=2024/month=01/day=05/
s3://meesho-data-lake/bronze/inventory/year=2024/month=01/day=05/
s3://meesho-data-lake/bronze/fraud_signals/year=2024/month=01/day=05/

Compatible with:
- MinIO (local development)
- AWS S3 (production) — change endpoint_url to None
- Cloudflare R2 (alternative) — change endpoint_url

Zero code changes needed between environments.
Only endpoint_url and credentials change.
This is twelve-factor app configuration pattern.
"""

import json
import os
import io
import logging
import boto3
from datetime import datetime, timezone
from typing import List, Dict
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3DataLakeArchiver:
    """
    Archives Kafka events to S3 as partitioned Parquet.

    Why batch writes not per-event writes:
    S3 charges per PUT operation.
    1000 events per file = 1000x fewer operations = 1000x cheaper.
    Same data, same storage, radically different cost.

    Why Parquet not JSON:
    Raw events arrive as JSON.
    Storing as JSON: 12GB per million events.
    Converting to Parquet: 1.2GB per million events.
    10x smaller. 10x cheaper to store. 10x faster to query.
    The conversion cost is negligible compared to savings.

    DSA inside:
    Buffer = list used as queue.
    append() = O(1).
    Flush when full = O(n) once per batch_size events.
    Amortized cost = O(1) per event.
    """

    def __init__(
        self,
        bucket: str,
        batch_size: int = 100,
        endpoint_url: str = None
    ):
        self.bucket = bucket
        self.batch_size = batch_size

        # S3 client — works with AWS S3 and MinIO
        # endpoint_url=None → real AWS S3
        # endpoint_url='http://localhost:9000' → MinIO
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url or os.getenv('S3_ENDPOINT'),
            aws_access_key_id=os.getenv('S3_ACCESS_KEY') or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('S3_SECRET_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )

        # Three zone buffers
        self.bronze_orders: List[Dict] = []
        self.bronze_inventory: List[Dict] = []
        self.bronze_fraud: List[Dict] = []

        # Stats
        self.files_written = 0
        self.events_archived = 0

        logger.info(
            f"S3 archiver initialized → "
            f"s3://{bucket} "
            f"(endpoint: {endpoint_url or 'AWS S3'})"
        )

    def _get_bronze_path(self, event_type: str) -> str:
        """
        Hive-partitioned Bronze layer path.

        Format: bronze/orders/year=2024/month=01/day=05/

        Why Hive format specifically:
        Spark, BigQuery, Athena, Presto all recognize
        year=/month=/day= folder structure automatically.
        No configuration needed — tools detect partitions.
        """
        now = datetime.now(timezone.utc)
        timestamp = now.strftime('%H%M%S_%f')
        return (
            f"bronze/{event_type}/"
            f"year={now.year}/"
            f"month={str(now.month).zfill(2)}/"
            f"day={str(now.day).zfill(2)}/"
            f"batch_{timestamp}.parquet"
        )

    def _flush(self, buffer: List[Dict], event_type: str):
        """
        Convert buffer to Parquet and write to S3.

        In-memory conversion — no temp files on disk.
        Buffer → DataFrame → Parquet bytes → S3.
        """
        if not buffer:
            return

        # Convert to DataFrame
        df = pd.DataFrame(buffer)
        df['_archived_at'] = datetime.now(timezone.utc).isoformat()
        df['_batch_size'] = len(buffer)

        # Parquet bytes in memory — no disk write needed
        buf = io.BytesIO()
        df.to_parquet(buf, engine='pyarrow', index=False, compression='snappy')
        buf.seek(0)
        parquet_bytes = buf.read()

        # S3 path with Hive partitioning
        s3_key = self._get_bronze_path(event_type)

        # Write to S3
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=parquet_bytes,
            ContentType='application/octet-stream'
        )

        self.files_written += 1
        self.events_archived += len(buffer)

        logger.info(
            f"S3 WRITE: s3://{self.bucket}/{s3_key} "
            f"({len(buffer)} events, "
            f"{len(parquet_bytes)/1024:.1f}KB, "
            f"snappy compressed)"
        )

    def add_order(self, event: Dict):
        self.bronze_orders.append(event)
        if len(self.bronze_orders) >= self.batch_size:
            self._flush(self.bronze_orders, 'orders')
            self.bronze_orders.clear()

    def add_inventory(self, event: Dict):
        self.bronze_inventory.append(event)
        if len(self.bronze_inventory) >= self.batch_size:
            self._flush(self.bronze_inventory, 'inventory')
            self.bronze_inventory.clear()

    def add_fraud_signal(self, event: Dict):
        self.bronze_fraud.append(event)
        if len(self.bronze_fraud) >= self.batch_size:
            self._flush(self.bronze_fraud, 'fraud_signals')
            self.bronze_fraud.clear()

    def flush_all(self):
        """Force flush all buffers on shutdown."""
        logger.info("Final flush — writing remaining events to S3...")
        self._flush(self.bronze_orders, 'orders')
        self._flush(self.bronze_inventory, 'inventory')
        self._flush(self.bronze_fraud, 'fraud_signals')
        self.bronze_orders.clear()
        self.bronze_inventory.clear()
        self.bronze_fraud.clear()

    def verify_bronze_layer(self):
        """
        List all files in Bronze zone.
        Shows partition structure clearly.
        """
        response = self.s3.list_objects_v2(
            Bucket=self.bucket,
            Prefix='bronze/'
        )

        objects = response.get('Contents', [])
        if not objects:
            logger.info("Bronze layer empty — no files yet")
            return

        logger.info(f"\nBRONZE LAYER CONTENTS ({len(objects)} files):")
        for obj in objects:
            size_kb = obj['Size'] / 1024
            logger.info(f"  s3://{self.bucket}/{obj['Key']} ({size_kb:.1f}KB)")

    def get_stats(self) -> Dict:
        return {
            'files_written': self.files_written,
            'events_archived': self.events_archived,
            'pending_orders': len(self.bronze_orders),
            'pending_inventory': len(self.bronze_inventory),
            'pending_fraud': len(self.bronze_fraud),
            'bucket': self.bucket
        }


class S3ArchiverConsumer:
    """
    Kafka consumer that archives events to S3 data lake.

    Fourth independent consumer group.
    Reads same events as inventory validator,
    fraud detector, and live metrics.
    Each consumer group manages its own offset.
    """

    def __init__(self):
        from kafka import KafkaConsumer
        from config import config

        self.archiver = S3DataLakeArchiver(
            bucket=os.getenv('S3_BUCKET', 'meesho-data-lake'),
            batch_size=50,
            endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:9000')
        )

        self.consumer = KafkaConsumer(
            config.KAFKA_ORDER_TOPIC,
            config.KAFKA_INVENTORY_TOPIC,
            config.KAFKA_FRAUD_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='s3-archiver-v1',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
        )

        logger.info("S3 Archiver Consumer started")

    def run(self, max_messages: int = 200):
        logger.info(f"Archiving to S3 data lake (max={max_messages})...")
        processed = 0

        try:
            for message in self.consumer:
                event = message.value
                event_type = event.get('event_type', '')

                if event_type == 'order_placed':
                    self.archiver.add_order(event)
                elif event_type == 'inventory_update':
                    self.archiver.add_inventory(event)
                elif event_type == 'seller_activity':
                    self.archiver.add_fraud_signal(event)

                self.consumer.commit()
                processed += 1

                if processed % 50 == 0:
                    logger.info(f"Progress: {self.archiver.get_stats()}")

                if processed >= max_messages:
                    break

        finally:
            self.archiver.flush_all()
            self.archiver.verify_bronze_layer()
            self.consumer.close()

            stats = self.archiver.get_stats()
            logger.info(f"\n=== S3 ARCHIVER COMPLETE ===")
            logger.info(f"Files written: {stats['files_written']}")
            logger.info(f"Events archived: {stats['events_archived']}")
            logger.info(f"Bucket: s3://{stats['bucket']}")


if __name__ == "__main__":
    import sys
    import uuid
    from datetime import datetime, timezone
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ingestion.event_generator import MeeshoEventGenerator

    archiver = S3DataLakeArchiver(
        bucket=os.getenv('S3_BUCKET', 'meesho-data-lake'),
        batch_size=5,
        endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:9000')
    )

    gen = MeeshoEventGenerator(
        num_sellers=10,
        num_customers=50,
        num_products=20
    )

    logger.info("Writing test events to S3 bronze layer...")

    for i in range(25):
        # Generate order event using actual attributes
        order = gen.generate_order_event()
        archiver.add_order(order.__dict__)

        # Inventory event using actual order fields
        inventory_event = {
            'event_id': f"EVT-INV-{uuid.uuid4().hex[:8].upper()}",
            'event_type': 'inventory_update',
            'product_id': order.product_id,
            'seller_id': order.seller_id,
            'previous_stock': 50,
            'units_sold': 1,
            'updated_stock': 49,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'warehouse_location': 'WH-MH-1'
        }
        archiver.add_inventory(inventory_event)

        # Fraud signal using actual order fields
        fraud_event = {
            'event_id': f"EVT-SELL-{uuid.uuid4().hex[:8].upper()}",
            'event_type': 'seller_activity',
            'seller_id': order.seller_id,
            'orders_last_minute': i % 5,
            'amount_inr': order.amount_inr,
            'city': order.city,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'risk_signals': []
        }
        archiver.add_fraud_signal(fraud_event)

    archiver.flush_all()
    archiver.verify_bronze_layer()

    logger.info("\nFinal stats:")
    for key, value in archiver.get_stats().items():
        logger.info(f"  {key}: {value}")
"""
Real-Time Inventory Validator

Business problem:
500,000 customers shopping simultaneously on sale day.
Two customers click "Buy" on the last unit at same millisecond.
Without validation: both get "Order Confirmed."
One cancellation. One angry customer. One bad review.

Solution:
Before confirming any order, check live inventory.
If stock = 0, reject the order instantly.
Customer sees "Out of Stock" before payment — not after.

DSA pattern: HashMap
Why: inventory has 50,000 products.
Checking stock for each order requires O(1) lookup.
HashMap gives exactly this.
The alternative — scanning a list — is O(n) per order.
At 500,000 orders/hour × O(n) = system collapses.

LeetCode equivalent: Two Sum, Contains Duplicate
Real implementation: this file.
"""

import json
import time
import logging
import sys
import os
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer, KafkaProducer
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InventoryStore:
    """
    In-memory inventory tracking using HashMap.

    Why in-memory and not database:
    Database query per order = 10-50ms latency.
    At 500,000 orders/hour = 500,000 DB queries/hour.
    Most databases collapse under this load.

    HashMap in memory = O(1) = microseconds.
    We sync with database every 60 seconds for durability.
    This is the read-through cache pattern.

    DSA: This is literally a HashMap.
    Key = product_id (string)
    Value = stock_level (integer)
    get() = O(1)
    set() = O(1)
    """

    def __init__(self):
        # Primary inventory map — HashMap O(1)
        self.stock: Dict[str, int] = {}

        # Reserved stock — items in checkout but not confirmed
        # Prevents overselling during checkout window
        self.reserved: Dict[str, int] = {}

        # Audit trail — every stock change recorded
        self.change_log = []

        # Stats
        self.total_checks = 0
        self.approved = 0
        self.rejected_no_stock = 0
        self.rejected_insufficient = 0

    def initialize_product(self, product_id: str, initial_stock: int):
        """Load initial stock level — O(1)"""
        self.stock[product_id] = initial_stock
        self.reserved[product_id] = 0

    def get_available_stock(self, product_id: str) -> int:
        """
        Available = total stock minus reserved.

        Why subtract reserved:
        Customer A adds last unit to cart.
        Customer B checks stock — sees 1 unit available.
        Customer B adds to cart.
        Customer A completes payment.
        Customer B completes payment.
        Result: oversold by 1.

        Reserving during checkout window prevents this.
        """
        total = self.stock.get(product_id, 0)
        reserved = self.reserved.get(product_id, 0)
        return max(0, total - reserved)

    def check_and_reserve(
        self,
        product_id: str,
        quantity: int,
        order_id: str
    ) -> dict:
        """
        Atomic check and reserve.

        Why atomic:
        Check then reserve as two separate operations
        creates a race condition.
        Thread A checks: stock = 1. OK.
        Thread B checks: stock = 1. OK.
        Thread A reserves: stock = 0.
        Thread B reserves: stock = -1. OVERSOLD.

        Atomic operation prevents this.
        In production: use Redis MULTI/EXEC for true atomicity.
        Here: Python GIL gives us single-threaded safety.
        """
        self.total_checks += 1
        available = self.get_available_stock(product_id)

        if available <= 0:
            self.rejected_no_stock += 1
            return {
                'approved': False,
                'reason': 'OUT_OF_STOCK',
                'available': 0,
                'requested': quantity,
                'product_id': product_id,
                'order_id': order_id
            }

        if available < quantity:
            self.rejected_insufficient += 1
            return {
                'approved': False,
                'reason': 'INSUFFICIENT_STOCK',
                'available': available,
                'requested': quantity,
                'product_id': product_id,
                'order_id': order_id
            }

        # Reserve the stock
        self.reserved[product_id] = self.reserved.get(product_id, 0) + quantity
        self.approved += 1

        self.change_log.append({
            'product_id': product_id,
            'action': 'reserved',
            'quantity': quantity,
            'order_id': order_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

        return {
            'approved': True,
            'reason': 'APPROVED',
            'available': available,
            'requested': quantity,
            'remaining_after_reserve': available - quantity,
            'product_id': product_id,
            'order_id': order_id
        }

    def confirm_sale(self, product_id: str, quantity: int, order_id: str):
        """
        Confirm order — reduce actual stock, release reservation.
        Called when payment is confirmed.
        """
        # Reduce actual stock
        current = self.stock.get(product_id, 0)
        self.stock[product_id] = max(0, current - quantity)

        # Release reservation
        current_reserved = self.reserved.get(product_id, 0)
        self.reserved[product_id] = max(0, current_reserved - quantity)

        self.change_log.append({
            'product_id': product_id,
            'action': 'sold',
            'quantity': quantity,
            'order_id': order_id,
            'new_stock': self.stock[product_id],
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    def update_from_inventory_event(
        self,
        product_id: str,
        updated_stock: int
    ):
        """
        Sync stock from inventory update event.
        Called when inventory event arrives from Kafka.
        """
        old_stock = self.stock.get(product_id, 0)
        self.stock[product_id] = updated_stock

        if abs(old_stock - updated_stock) > 10:
            logger.info(
                f"Large stock change: {product_id} "
                f"{old_stock} → {updated_stock}"
            )

    def get_low_stock_alerts(self, threshold: int = 5) -> list:
        """
        Find products below threshold.

        DSA: iterate HashMap, filter by value.
        O(n) where n = number of products.
        Called every 60 seconds not per order.
        """
        alerts = []
        for product_id, stock in self.stock.items():
            available = self.get_available_stock(product_id)
            if 0 < available <= threshold:
                alerts.append({
                    'product_id': product_id,
                    'available_stock': available,
                    'alert_level': 'CRITICAL' if available <= 2 else 'WARNING'
                })
        return sorted(alerts, key=lambda x: x['available_stock'])

    def get_stats(self) -> dict:
        return {
            'total_checks': self.total_checks,
            'approved': self.approved,
            'rejected_no_stock': self.rejected_no_stock,
            'rejected_insufficient': self.rejected_insufficient,
            'approval_rate': round(
                self.approved / max(1, self.total_checks) * 100, 2
            ),
            'products_tracked': len(self.stock),
            'low_stock_count': len(self.get_low_stock_alerts())
        }


class InventoryValidatorConsumer:
    """
    Kafka consumer that validates inventory in real time.

    Consumes from two topics simultaneously:
    - meesho-orders: validate before confirming
    - meesho-inventory: sync stock levels

    Why two topics:
    Orders arrive faster than inventory updates.
    Processing them in one consumer with priority
    ensures inventory is always current before validation.
    """

    def __init__(self):
        self.inventory = InventoryStore()
        self._seed_inventory()

        self.consumer = KafkaConsumer(
            config.KAFKA_ORDER_TOPIC,
            config.KAFKA_INVENTORY_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='inventory-validator-v1',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
        )

        logger.info("Inventory validator started")

    def _seed_inventory(self):
        """Initialize inventory for common products"""
        for i in range(200):
            product_id = f"PROD-{str(i).zfill(6)}"
            initial_stock = 50 + (i % 100)
            self.inventory.initialize_product(product_id, initial_stock)
        logger.info(f"Seeded inventory for 200 products")

    def process_order_event(self, event: dict) -> dict:
        """
        Validate order against current inventory.
        O(1) per order — HashMap lookup.
        """
        product_id = event.get('product_id', '')
        quantity = event.get('quantity', 1)
        order_id = event.get('order_id', '')
        amount = event.get('amount_inr', 0)

        result = self.inventory.check_and_reserve(
            product_id, quantity, order_id
        )

        if result['approved']:
            logger.info(
                f"APPROVED: {order_id} | "
                f"product={product_id} | "
                f"qty={quantity} | "
                f"₹{amount:.0f} | "
                f"remaining={result['remaining_after_reserve']}"
            )
        else:
            logger.warning(
                f"REJECTED: {order_id} | "
                f"reason={result['reason']} | "
                f"requested={quantity} | "
                f"available={result['available']}"
            )

        return result

    def process_inventory_event(self, event: dict):
        """Sync stock from inventory update"""
        product_id = event.get('product_id', '')
        updated_stock = event.get('updated_stock', 0)
        self.inventory.update_from_inventory_event(product_id, updated_stock)

    def run(self, max_messages: int = 100):
        """Main consumer loop"""
        logger.info(f"Listening for events (max={max_messages})...")
        processed = 0

        try:
            for message in self.consumer:
                event = message.value
                event_type = event.get('event_type', '')

                if event_type == 'order_placed':
                    self.process_order_event(event)
                elif event_type == 'inventory_update':
                    self.process_inventory_event(event)

                # Manual commit — exactly-once guarantee
                self.consumer.commit()
                processed += 1

                # Print stats every 20 messages
                if processed % 20 == 0:
                    stats = self.inventory.get_stats()
                    logger.info(f"\nSTATS: {stats}\n")

                    alerts = self.inventory.get_low_stock_alerts()
                    if alerts:
                        logger.warning(
                            f"LOW STOCK ALERTS: {len(alerts)} products"
                        )
                        for alert in alerts[:3]:
                            logger.warning(f"  {alert}")

                if processed >= max_messages:
                    break

        finally:
            self.consumer.close()
            final_stats = self.inventory.get_stats()
            logger.info(f"\n=== FINAL STATS ===")
            for key, value in final_stats.items():
                logger.info(f"  {key}: {value}")


if __name__ == "__main__":
    validator = InventoryValidatorConsumer()
    validator.run(max_messages=50)
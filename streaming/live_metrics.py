"""
Real-Time Business Metrics Aggregator

Business problem:
CEO asks at 2pm on sale day:
"Which categories are selling right now?"
"Who are our top 10 sellers this hour?"
"What is our revenue in the last 5 minutes?"

Without real-time: answer comes from last night's batch.
With this consumer: answer is 30 seconds old.

DSA patterns used:
1. HashMap — O(1) revenue tracking by category
2. Min-Heap of size k — O(log k) top-k sellers
3. Sliding window — rolling revenue calculation
4. Running counter — total orders and revenue

LeetCode equivalents:
- Top K Frequent Elements (heap)
- Moving Average from Data Stream (sliding window)
- Design HashMap (category revenue)
"""

import json
import time
import heapq
import logging
import sys
import os
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Dict, List, Tuple
from dataclasses import dataclass

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TopKTracker:
    """
    Maintains top-k items by score using min-heap.

    Why min-heap not sort:
    Sort every update: O(n log n) per update
    Min-heap size k: O(log k) per update

    At 10,000 sellers updating every second:
    Sort: 10,000 × log(10,000) = 130,000 operations/sec
    Heap: 10,000 × log(10) = 33,000 operations/sec
    4x faster. At 100,000 sellers: 10x faster.

    How min-heap of size k works:
    Heap always contains k LARGEST values.
    Heap root = SMALLEST of the top-k.
    New value > root → replace root → reheapify.
    New value <= root → not top-k → discard.

    LeetCode 347: Top K Frequent Elements
    LeetCode 215: Kth Largest Element in an Array
    """

    def __init__(self, k: int = 10):
        self.k = k
        # Min-heap: (score, identifier)
        # Python heapq is a min-heap
        self.heap: List[Tuple[float, str]] = []
        # Full scores: identifier → score
        # Needed to update existing entries
        self.scores: Dict[str, float] = {}

    def update(self, identifier: str, score: float):
        """Update score and maintain top-k. O(log k)"""
        self.scores[identifier] = score

        # Rebuild heap if identifier already in it
        # More efficient approaches exist but this is clear
        self._rebuild_if_needed(identifier, score)

    def _rebuild_if_needed(self, identifier: str, score: float):
        """Add to heap if qualifies for top-k"""
        # Check if already in heap
        heap_ids = {item[1] for item in self.heap}

        if identifier in heap_ids:
            # Remove old entry and re-add
            self.heap = [(s, i) for s, i in self.heap if i != identifier]
            heapq.heapify(self.heap)

        if len(self.heap) < self.k:
            heapq.heappush(self.heap, (score, identifier))
        elif score > self.heap[0][0]:
            # Better than current minimum — replace
            heapq.heapreplace(self.heap, (score, identifier))

    def get_top_k(self) -> List[Tuple[str, float]]:
        """Return top-k sorted descending. O(k log k)"""
        return sorted(
            [(identifier, score) for score, identifier in self.heap],
            key=lambda x: x[1],
            reverse=True
        )


class RollingMetric:
    """
    Compute rolling metric over a time window.

    Sliding window with deque.
    Add new values at right, remove expired at left.
    Sum maintained incrementally — O(1) per update.

    LeetCode 346: Moving Average from Data Stream
    """

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        # Deque of (timestamp, value)
        self.window: deque = deque()
        self.running_sum: float = 0.0

    def add(self, value: float, timestamp: float = None) -> float:
        """Add value and return current window sum. O(1) amortized"""
        if timestamp is None:
            timestamp = time.time()

        # Add to right
        self.window.append((timestamp, value))
        self.running_sum += value

        # Remove expired from left
        cutoff = timestamp - self.window_seconds
        while self.window and self.window[0][0] < cutoff:
            _, expired_value = self.window.popleft()
            self.running_sum -= expired_value

        return self.running_sum

    def get_average(self) -> float:
        """Current window average"""
        if not self.window:
            return 0.0
        return self.running_sum / len(self.window)

    def get_sum(self) -> float:
        return self.running_sum

    def get_count(self) -> int:
        return len(self.window)


class LiveMetricsAggregator:
    """
    Maintains all real-time business metrics.

    This is what leadership sees on their
    sale-day war room dashboard.
    Updated every 30 seconds from Kafka events.

    Data structures:
    - category_revenue: HashMap O(1)
    - top_sellers: Min-heap O(log k)
    - rolling_revenue: Sliding window O(1)
    - city_orders: HashMap O(1)
    """

    def __init__(self):
        # Total counters
        self.total_orders = 0
        self.total_revenue = 0.0
        self.total_revenue_inr = 0.0

        # Category metrics — HashMap
        # O(1) update per order
        self.category_revenue: Dict[str, float] = defaultdict(float)
        self.category_orders: Dict[str, int] = defaultdict(int)

        # City metrics — HashMap
        self.city_orders: Dict[str, int] = defaultdict(int)
        self.city_revenue: Dict[str, float] = defaultdict(float)

        # Payment method distribution — HashMap
        self.payment_counts: Dict[str, int] = defaultdict(int)

        # Top sellers — Min-heap
        self.top_sellers = TopKTracker(k=10)
        self.seller_revenue: Dict[str, float] = defaultdict(float)

        # Rolling metrics — Sliding windows
        self.rolling_5min = RollingMetric(window_seconds=300)
        self.rolling_1min = RollingMetric(window_seconds=60)

        # City tier distribution
        self.tier_orders: Dict[str, int] = defaultdict(int)

        self.start_time = time.time()
        self.last_dashboard_print = time.time()

    def process_order(self, event: dict):
        """
        Update all metrics for one order event.
        Total: O(log k) for heap update, O(1) for everything else.
        """
        amount = float(event.get('amount_inr', 0))
        category = event.get('category', 'Unknown')
        city = event.get('city', 'Unknown')
        payment = event.get('payment_method', 'Unknown')
        seller_id = event.get('seller_id', '')
        city_tier = event.get('city_tier', 'unknown')
        now = time.time()

        # Total counters — O(1)
        self.total_orders += 1
        self.total_revenue += amount

        # Category — HashMap O(1)
        self.category_revenue[category] += amount
        self.category_orders[category] += 1

        # City — HashMap O(1)
        self.city_orders[city] += 1
        self.city_revenue[city] += amount

        # Tier distribution
        self.tier_orders[city_tier] += 1

        # Payment method — HashMap O(1)
        self.payment_counts[payment] += 1

        # Top sellers — Min-heap O(log k)
        self.seller_revenue[seller_id] += amount
        self.top_sellers.update(seller_id, self.seller_revenue[seller_id])

        # Rolling windows — O(1) amortized
        self.rolling_5min.add(amount, now)
        self.rolling_1min.add(amount, now)

    def get_dashboard(self) -> dict:
        """Compile complete dashboard snapshot"""
        elapsed = time.time() - self.start_time
        throughput = self.total_orders / max(1, elapsed)

        # Top 5 categories by revenue
        top_categories = sorted(
            self.category_revenue.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        # Top 5 cities by orders
        top_cities = sorted(
            self.city_orders.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]

        # Payment distribution
        total_payments = sum(self.payment_counts.values())
        payment_dist = {
            method: round(count/max(1, total_payments)*100, 1)
            for method, count in sorted(
                self.payment_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        }

        return {
            'snapshot_time': datetime.now(timezone.utc).isoformat(),
            'summary': {
                'total_orders': self.total_orders,
                'total_revenue_inr': round(self.total_revenue, 2),
                'avg_order_value': round(
                    self.total_revenue / max(1, self.total_orders), 2
                ),
                'throughput_per_sec': round(throughput, 2)
            },
            'rolling': {
                'revenue_last_1min': round(self.rolling_1min.get_sum(), 2),
                'revenue_last_5min': round(self.rolling_5min.get_sum(), 2),
                'orders_last_1min': self.rolling_1min.get_count(),
                'avg_order_last_5min': round(
                    self.rolling_5min.get_average(), 2
                )
            },
            'top_categories': [
                {
                    'category': cat,
                    'revenue': round(rev, 2),
                    'orders': self.category_orders[cat],
                    'share_pct': round(
                        rev / max(1, self.total_revenue) * 100, 1
                    )
                }
                for cat, rev in top_categories
            ],
            'top_sellers': [
                {'seller_id': sid, 'revenue': round(rev, 2)}
                for sid, rev in self.top_sellers.get_top_k()
            ],
            'top_cities': [
                {
                    'city': city,
                    'orders': orders,
                    'revenue': round(self.city_revenue.get(city, 0), 2)
                }
                for city, orders in top_cities
            ],
            'payment_distribution': payment_dist,
            'city_tier_distribution': dict(self.tier_orders)
        }

    def print_dashboard(self):
        """Print formatted dashboard"""
        d = self.get_dashboard()

        print("\n" + "="*60)
        print("MEESHO LIVE COMMERCE DASHBOARD")
        print(f"Snapshot: {d['snapshot_time']}")
        print("="*60)

        s = d['summary']
        print(f"\nSUMMARY:")
        print(f"  Total Orders    : {s['total_orders']:,}")
        print(f"  Total Revenue   : ₹{s['total_revenue_inr']:,.2f}")
        print(f"  Avg Order Value : ₹{s['avg_order_value']:,.2f}")
        print(f"  Throughput      : {s['throughput_per_sec']} orders/sec")

        r = d['rolling']
        print(f"\nROLLING METRICS:")
        print(f"  Last 1 min revenue : ₹{r['revenue_last_1min']:,.2f}")
        print(f"  Last 5 min revenue : ₹{r['revenue_last_5min']:,.2f}")
        print(f"  Orders last 1 min  : {r['orders_last_1min']}")

        print(f"\nTOP CATEGORIES:")
        for i, cat in enumerate(d['top_categories'], 1):
            bar = "█" * int(cat['share_pct'] / 3)
            print(
                f"  {i}. {cat['category']:<20} "
                f"₹{cat['revenue']:>10,.0f} "
                f"({cat['share_pct']}%) {bar}"
            )

        print(f"\nTOP SELLERS:")
        for i, seller in enumerate(d['top_sellers'][:5], 1):
            print(
                f"  {i}. {seller['seller_id']:<20} "
                f"₹{seller['revenue']:>10,.0f}"
            )

        print(f"\nTOP CITIES:")
        for i, city in enumerate(d['top_cities'], 1):
            print(
                f"  {i}. {city['city']:<15} "
                f"{city['orders']:>5} orders | "
                f"₹{city['revenue']:>10,.0f}"
            )

        print(f"\nPAYMENT METHODS:")
        for method, pct in d['payment_distribution'].items():
            bar = "█" * int(pct / 3)
            print(f"  {method:<20} {pct:>5.1f}% {bar}")

        print(f"\nCITY TIER DISTRIBUTION:")
        total = sum(d['city_tier_distribution'].values())
        for tier, count in sorted(d['city_tier_distribution'].items()):
            pct = count / max(1, total) * 100
            print(f"  {tier:<10} {count:>5} orders ({pct:.1f}%)")

        print("="*60)


class LiveMetricsConsumer:
    """Kafka consumer feeding the metrics aggregator"""

    def __init__(self):
        self.aggregator = LiveMetricsAggregator()

        self.consumer = KafkaConsumer(
            config.KAFKA_ORDER_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='live-metrics-v1',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
        )

        logger.info("Live metrics consumer started")

    def run(self, max_messages: int = 100, dashboard_interval: int = 20):
        """
        Main consumer loop.
        Prints dashboard every dashboard_interval messages.
        """
        logger.info(f"Listening for events (max={max_messages})...")
        processed = 0

        try:
            for message in self.consumer:
                event = message.value

                if event.get('event_type') == 'order_placed':
                    self.aggregator.process_order(event)

                self.consumer.commit()
                processed += 1

                if processed % dashboard_interval == 0:
                    self.aggregator.print_dashboard()

                if processed >= max_messages:
                    break

        finally:
            self.consumer.close()
            # Final dashboard
            self.aggregator.print_dashboard()
            logger.info("Live metrics consumer stopped")


if __name__ == "__main__":
    consumer = LiveMetricsConsumer()
    consumer.run(max_messages=100, dashboard_interval=25)
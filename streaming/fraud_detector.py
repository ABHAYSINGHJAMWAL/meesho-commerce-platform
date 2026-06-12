"""
Real-Time Fraud Detector

Business problem:
Seller creates 500 fake orders in 60 seconds
to manipulate sales rankings and get promoted on platform.
Batch pipeline detects this next morning.
By then: ranked #1 for 12 hours, real sellers lost business.

Solution:
Detect velocity fraud within seconds.
If any seller places > threshold orders per minute:
flag immediately, alert ops team, freeze seller account.

DSA pattern: Sliding Window
Why: count events in a moving time window.
Each seller has a deque of timestamps.
When new order arrives: add timestamp to deque.
Remove timestamps older than 60 seconds from front.
Length of deque = orders in last 60 seconds.

This is exactly:
LeetCode 239 — Sliding Window Maximum
LeetCode 485 — Max Consecutive Ones
Applied to fraud detection.

Time: O(1) amortized per event
Space: O(k) where k = window size
"""

import json
import time
import logging
import sys
import os
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict
from typing import Dict, List, Tuple
from dataclasses import dataclass, asdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FraudAlert:
    """Structured fraud alert for downstream systems"""
    alert_id: str
    seller_id: str
    alert_type: str
    severity: str
    description: str
    evidence: dict
    timestamp: str
    action_recommended: str


class SlidingWindowCounter:
    """
    Count events in a sliding time window using deque.

    This is the core DSA of fraud detection.

    How it works:
    deque stores timestamps of recent events.
    On each new event:
      1. Add current timestamp to right of deque
      2. Remove timestamps older than window from left
      3. Length = count in window

    Why deque not list:
    list.pop(0) is O(n) — shifts all elements
    deque.popleft() is O(1) — constant time
    At 500,000 events/hour this difference is massive.

    Visualization:
    Window = 60 seconds
    Time = 10:00:00

    deque: [09:59:05, 09:59:30, 09:59:45, 10:00:00]
                                                    ← new event added here
    10:00:05 arrives → 09:59:05 is > 60s old → removed
    deque: [09:59:30, 09:59:45, 10:00:00, 10:00:05]
    length = 4 = events in last 60 seconds
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        # Each seller has their own deque
        # HashMap of seller_id → deque of timestamps
        self.windows: Dict[str, deque] = defaultdict(deque)

    def add_event(self, key: str, timestamp: float) -> int:
        """
        Add event and return count in window.
        O(1) amortized — each timestamp added once, removed once.
        """
        window = self.windows[key]

        # Add new timestamp to right
        window.append(timestamp)

        # Remove expired timestamps from left
        cutoff = timestamp - self.window_seconds
        while window and window[0] < cutoff:
            window.popleft()

        return len(window)

    def get_count(self, key: str, current_time: float) -> int:
        """Get current count for key without adding event"""
        window = self.windows[key]
        cutoff = current_time - self.window_seconds
        while window and window[0] < cutoff:
            window.popleft()
        return len(window)

    def get_all_active_keys(self) -> List[str]:
        """Return keys with at least one event in current window"""
        now = time.time()
        active = []
        for key, window in self.windows.items():
            if window and window[-1] >= now - self.window_seconds:
                active.append(key)
        return active


class FraudRuleEngine:
    """
    Evaluates fraud rules against seller behavior.

    Rules are configured not hardcoded.
    Why: fraud patterns change. Rules must be updatable
    without redeployment.

    Each rule returns (is_fraud, alert_type, severity, evidence).
    Multiple rules can fire for same seller simultaneously.
    """

    RULES = [
        {
            'name': 'VELOCITY_1MIN',
            'description': 'More than 30 orders in 60 seconds',
            'window': 60,
            'threshold': 30,
            'severity': 'HIGH',
            'action': 'FLAG_FOR_REVIEW'
        },
        {
            'name': 'VELOCITY_5MIN',
            'description': 'More than 100 orders in 5 minutes',
            'window': 300,
            'threshold': 100,
            'severity': 'CRITICAL',
            'action': 'FREEZE_SELLER_ACCOUNT'
        },
        {
            'name': 'AMOUNT_SPIKE',
            'description': 'Average order value 10x normal',
            'window': 300,
            'threshold': None,  # Dynamic threshold
            'severity': 'MEDIUM',
            'action': 'MANUAL_REVIEW'
        }
    ]

    def __init__(self):
        # One sliding window per rule time window
        self.counters: Dict[int, SlidingWindowCounter] = {}
        for rule in self.RULES:
            window = rule['window']
            if window not in self.counters:
                self.counters[window] = SlidingWindowCounter(window)

        # Amount tracking per seller
        # HashMap: seller_id → deque of recent amounts
        self.amount_windows: Dict[str, deque] = defaultdict(deque)
        self.baseline_amounts: Dict[str, float] = {}

    def evaluate(
        self,
        seller_id: str,
        order_amount: float,
        timestamp: float
    ) -> List[FraudAlert]:
        """
        Evaluate all fraud rules for this order.
        Returns list of triggered alerts.
        """
        alerts = []
        import uuid

        # Update amount window
        self.amount_windows[seller_id].append(
            (timestamp, order_amount)
        )
        # Keep only last 5 minutes
        cutoff = timestamp - 300
        while (self.amount_windows[seller_id] and
               self.amount_windows[seller_id][0][0] < cutoff):
            self.amount_windows[seller_id].popleft()

        # Rule 1: 1-minute velocity
        count_1min = self.counters[60].add_event(seller_id, timestamp)
        if count_1min > self.RULES[0]['threshold']:
            alerts.append(FraudAlert(
                alert_id=f"ALERT-{uuid.uuid4().hex[:8].upper()}",
                seller_id=seller_id,
                alert_type='VELOCITY_1MIN',
                severity='HIGH',
                description=f"Seller placed {count_1min} orders in 60 seconds",
                evidence={
                    'orders_in_window': count_1min,
                    'threshold': self.RULES[0]['threshold'],
                    'window_seconds': 60
                },
                timestamp=datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).isoformat(),
                action_recommended='FLAG_FOR_REVIEW'
            ))

        # Rule 2: 5-minute velocity
        count_5min = self.counters[300].add_event(seller_id, timestamp)
        if count_5min > self.RULES[1]['threshold']:
            alerts.append(FraudAlert(
                alert_id=f"ALERT-{uuid.uuid4().hex[:8].upper()}",
                seller_id=seller_id,
                alert_type='VELOCITY_5MIN',
                severity='CRITICAL',
                description=f"Seller placed {count_5min} orders in 5 minutes",
                evidence={
                    'orders_in_window': count_5min,
                    'threshold': self.RULES[1]['threshold'],
                    'window_seconds': 300
                },
                timestamp=datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).isoformat(),
                action_recommended='FREEZE_SELLER_ACCOUNT'
            ))

        # Rule 3: Amount anomaly
        recent_amounts = [
            amt for _, amt in self.amount_windows[seller_id]
        ]
        if len(recent_amounts) >= 5:
            avg = sum(recent_amounts) / len(recent_amounts)
            baseline = self.baseline_amounts.get(seller_id, avg)

            if order_amount > baseline * 10:
                alerts.append(FraudAlert(
                    alert_id=f"ALERT-{uuid.uuid4().hex[:8].upper()}",
                    seller_id=seller_id,
                    alert_type='AMOUNT_ANOMALY',
                    severity='MEDIUM',
                    description=f"Order ₹{order_amount:.0f} is 10x above avg ₹{baseline:.0f}",
                    evidence={
                        'order_amount': order_amount,
                        'seller_avg': round(baseline, 2),
                        'ratio': round(order_amount / max(1, baseline), 1)
                    },
                    timestamp=datetime.fromtimestamp(
                        timestamp, tz=timezone.utc
                    ).isoformat(),
                    action_recommended='MANUAL_REVIEW'
                ))

            # Update baseline with exponential moving average
            self.baseline_amounts[seller_id] = (
                0.9 * baseline + 0.1 * order_amount
            )

        return alerts


class FraudDetectorConsumer:
    """
    Kafka consumer that detects fraud in real time.
    """

    def __init__(self):
        self.rule_engine = FraudRuleEngine()
        self.total_orders = 0
        self.total_alerts = 0
        self.alerts_by_type: Dict[str, int] = defaultdict(int)
        self.flagged_sellers: Dict[str, int] = {}

        self.consumer = KafkaConsumer(
            config.KAFKA_ORDER_TOPIC,
            config.KAFKA_FRAUD_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='fraud-detector-v1',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
        )

        logger.info("Fraud detector started")

    def process_event(self, event: dict):
        """Process one event through fraud rules"""
        event_type = event.get('event_type', '')

        if event_type == 'order_placed':
            self.total_orders += 1
            seller_id = event.get('seller_id', '')
            amount = float(event.get('amount_inr', 0))
            timestamp = time.time()

            alerts = self.rule_engine.evaluate(seller_id, amount, timestamp)

            for alert in alerts:
                self.total_alerts += 1
                self.alerts_by_type[alert.alert_type] += 1
                self.flagged_sellers[seller_id] = \
                    self.flagged_sellers.get(seller_id, 0) + 1

                if alert.severity in ('HIGH', 'CRITICAL'):
                    logger.warning(
                        f"\n{'='*50}\n"
                        f"FRAUD ALERT [{alert.severity}]\n"
                        f"Seller: {alert.seller_id}\n"
                        f"Type: {alert.alert_type}\n"
                        f"Detail: {alert.description}\n"
                        f"Action: {alert.action_recommended}\n"
                        f"Evidence: {alert.evidence}\n"
                        f"{'='*50}"
                    )
                else:
                    logger.info(
                        f"FRAUD SIGNAL [{alert.severity}]: "
                        f"{alert.seller_id} — {alert.description}"
                    )

        elif event_type == 'seller_activity':
            risk_signals = event.get('risk_signals', [])
            if risk_signals:
                logger.info(
                    f"Seller signals: {event.get('seller_id')} "
                    f"→ {risk_signals}"
                )

    def run(self, max_messages: int = 100):
        """Main consumer loop"""
        logger.info(f"Fraud detector listening (max={max_messages})...")
        processed = 0

        try:
            for message in self.consumer:
                self.process_event(message.value)
                self.consumer.commit()
                processed += 1

                if processed % 25 == 0:
                    logger.info(
                        f"\nFRAUD STATS: "
                        f"orders={self.total_orders} | "
                        f"alerts={self.total_alerts} | "
                        f"flagged_sellers={len(self.flagged_sellers)} | "
                        f"by_type={dict(self.alerts_by_type)}"
                    )

                if processed >= max_messages:
                    break

        finally:
            self.consumer.close()
            logger.info(
                f"\n=== FRAUD DETECTOR COMPLETE ===\n"
                f"Orders processed: {self.total_orders}\n"
                f"Alerts generated: {self.total_alerts}\n"
                f"Flagged sellers: {len(self.flagged_sellers)}\n"
                f"Alert breakdown: {dict(self.alerts_by_type)}"
            )


if __name__ == "__main__":
    detector = FraudDetectorConsumer()
    detector.run(max_messages=50)
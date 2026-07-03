"""
Unit Tests for DSA Algorithm Implementations

Every test follows this pattern:
1. Arrange — set up test data
2. Act — call the function
3. Assert — verify result

These tests prove your implementations work correctly
and give you confidence before OA rounds.
"""

import pytest
import time
from batch.utils.algorithms import (
    InventoryHashMap,
    TopKSellers,
    FraudSlidingWindow,
    PipelineTopologicalSort,
    SparkPartitionMerger,
    PipelineBinarySearch
)


# ════════════════════════════════════════════
# HASHMAP TESTS
# ════════════════════════════════════════════

class TestInventoryHashMap:

    def setup_method(self):
        self.inv = InventoryHashMap()
        self.inv.set_stock("PROD-001", 100)
        self.inv.set_stock("PROD-002", 5)
        self.inv.set_stock("PROD-003", 0)

    def test_get_available_full_stock(self):
        assert self.inv.get_available("PROD-001") == 100

    def test_get_available_zero_stock(self):
        assert self.inv.get_available("PROD-003") == 0

    def test_get_available_unknown_product(self):
        assert self.inv.get_available("PROD-999") == 0

    def test_reserve_success(self):
        result = self.inv.reserve("PROD-001", 10)
        assert result is True
        assert self.inv.get_available("PROD-001") == 90

    def test_reserve_insufficient_stock(self):
        result = self.inv.reserve("PROD-002", 10)
        assert result is False
        assert self.inv.get_available("PROD-002") == 5

    def test_reserve_zero_stock(self):
        result = self.inv.reserve("PROD-003", 1)
        assert result is False

    def test_confirm_sale(self):
        self.inv.reserve("PROD-001", 10)
        self.inv.confirm_sale("PROD-001", 10)
        assert self.inv.get_available("PROD-001") == 90

    def test_find_low_stock(self):
        low = self.inv.find_low_stock(threshold=10)
        assert "PROD-002" in low
        assert "PROD-001" not in low

    def test_find_duplicates(self):
        ids = ["O1", "O2", "O1", "O3", "O2"]
        dupes = self.inv.find_duplicates(ids)
        assert "O1" in dupes
        assert "O2" in dupes
        assert "O3" not in dupes

    def test_find_duplicates_empty(self):
        assert self.inv.find_duplicates([]) == []

    def test_two_sum_orders(self):
        amounts = [200.0, 500.0, 300.0, 800.0]
        result = self.inv.two_sum_orders(amounts, 800.0)
        assert result == [1, 2]

    def test_two_sum_no_result(self):
        amounts = [100.0, 200.0, 300.0]
        result = self.inv.two_sum_orders(amounts, 999.0)
        assert result == []

    def test_lookup_is_o1(self):
        for i in range(1000):
            self.inv.set_stock(f"PROD-{i}", i * 10)

        start = time.time()
        for i in range(10000):
            self.inv.get_available(f"PROD-{i % 1000}")
        elapsed = time.time() - start

        assert elapsed < 0.5, \
            f"10,000 lookups took {elapsed:.3f}s — should be < 0.5s"


# ════════════════════════════════════════════
# MIN-HEAP TESTS
# ════════════════════════════════════════════

class TestTopKSellers:

    def setup_method(self):
        self.tracker = TopKSellers(k=3)

    def test_top_k_basic(self):
        self.tracker.update_seller("S1", 1000)
        self.tracker.update_seller("S2", 5000)
        self.tracker.update_seller("S3", 3000)
        self.tracker.update_seller("S4", 2000)

        top = self.tracker.get_top_k()
        top_ids = [s for s, r in top]

        assert "S2" in top_ids
        assert "S3" in top_ids
        assert "S4" in top_ids
        assert "S1" not in top_ids

    def test_top_k_revenue_accumulates(self):
        self.tracker.update_seller("S1", 1000)
        self.tracker.update_seller("S1", 2000)

        assert self.tracker.seller_revenue["S1"] == 3000

    def test_find_kth_largest(self):
        revenues = [3000, 1500, 8000, 500, 12000, 2500]
        assert self.tracker.find_kth_largest(revenues, 1) == 12000
        assert self.tracker.find_kth_largest(revenues, 2) == 8000
        assert self.tracker.find_kth_largest(revenues, 3) == 3000

    def test_top_k_frequent(self):
        events = [
            "purchase", "view", "purchase",
            "cart", "view", "purchase"
        ]
        top2 = self.tracker.top_k_frequent(events, 2)
        assert top2[0] == "purchase"
        assert top2[1] == "view"

    def test_top_k_empty(self):
        top = self.tracker.get_top_k()
        assert top == []


# ════════════════════════════════════════════
# SLIDING WINDOW TESTS
# ════════════════════════════════════════════

class TestFraudSlidingWindow:

    def setup_method(self):
        self.detector = FraudSlidingWindow(window_seconds=60)

    def test_count_increases_in_window(self):
        now = time.time()
        for i in range(5):
            count = self.detector.add_event("S1", now + i)
        assert count == 5

    def test_old_events_expire(self):
        now = time.time()
        for i in range(10):
            self.detector.add_event("S1", now - 120 + i)

        count = self.detector.add_event("S1", now)
        assert count == 1

    def test_fraud_detection(self):
        now = time.time()
        for i in range(35):
            self.detector.add_event("FRAUD-SELLER", now + i * 0.1)

        assert self.detector.is_fraud("FRAUD-SELLER", threshold=30)

    def test_no_fraud_below_threshold(self):
        now = time.time()
        for i in range(10):
            self.detector.add_event("GOOD-SELLER", now + i)

        assert not self.detector.is_fraud("GOOD-SELLER", threshold=30)

    def test_different_sellers_isolated(self):
        now = time.time()
        for i in range(35):
            self.detector.add_event("S1", now + i * 0.1)

        self.detector.add_event("S2", now)
        assert not self.detector.is_fraud("S2", threshold=30)

    def test_max_consecutive_active_days(self):
        daily = [10, 5, 8, 12, 15, 3, 9, 11]
        result = self.detector.max_consecutive_active_days(
            daily, threshold=7
        )
        assert result == 3

    def test_rolling_average(self):
        values = [100, 200, 300, 400, 500]
        result = self.detector.rolling_average(values, window_size=3)
        assert result[0] == 100.0
        assert result[2] == 200.0
        assert result[4] == 400.0


# ════════════════════════════════════════════
# TOPOLOGICAL SORT TESTS
# ════════════════════════════════════════════

class TestPipelineTopologicalSort:

    def setup_method(self):
        self.sorter = PipelineTopologicalSort()

    def test_valid_dag(self):
        tasks = ["A", "B", "C", "D"]
        deps = [("A", "B"), ("B", "C"), ("C", "D")]
        order, valid = self.sorter.get_execution_order(tasks, deps)

        assert valid is True
        assert order.index("A") < order.index("B")
        assert order.index("B") < order.index("C")
        assert order.index("C") < order.index("D")

    def test_cycle_detection(self):
        tasks = ["A", "B", "C"]
        deps = [("A", "B"), ("B", "C"), ("C", "A")]
        order, valid = self.sorter.get_execution_order(tasks, deps)
        assert valid is False

    def test_no_dependencies(self):
        tasks = ["A", "B", "C"]
        order, valid = self.sorter.get_execution_order(tasks, [])
        assert valid is True
        assert len(order) == 3

    def test_can_complete_valid(self):
        result = self.sorter.can_complete(
            4, [(1, 0), (2, 1), (3, 2)]
        )
        assert result is True

    def test_can_complete_cycle(self):
        result = self.sorter.can_complete(
            2, [(1, 0), (0, 1)]
        )
        assert result is False

    def test_airflow_dag_ordering(self):
        tasks = [
            "check_kafka", "check_freshness",
            "run_spark", "validate_output",
            "send_summary"
        ]
        deps = [
            ("check_kafka", "check_freshness"),
            ("check_freshness", "run_spark"),
            ("run_spark", "validate_output"),
            ("validate_output", "send_summary")
        ]
        order, valid = self.sorter.get_execution_order(tasks, deps)

        assert valid is True
        assert order[0] == "check_kafka"
        assert order[-1] == "send_summary"


# ════════════════════════════════════════════
# K-WAY MERGE TESTS
# ════════════════════════════════════════════

class TestSparkPartitionMerger:

    def setup_method(self):
        self.merger = SparkPartitionMerger()

    def test_merge_two_partitions(self):
        p1 = [1.0, 3.0, 5.0]
        p2 = [2.0, 4.0, 6.0]
        result = self.merger.merge_sorted_partitions([p1, p2])
        assert result == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

    def test_merge_four_partitions(self):
        partitions = [
            [1.0, 5.0, 9.0],
            [2.0, 6.0, 10.0],
            [3.0, 7.0, 11.0],
            [4.0, 8.0, 12.0]
        ]
        result = self.merger.merge_sorted_partitions(partitions)
        assert result == list(range(1, 13))

    def test_merge_empty_partition(self):
        p1 = [1.0, 2.0]
        p2 = []
        p3 = [3.0, 4.0]
        result = self.merger.merge_sorted_partitions([p1, p2, p3])
        assert result == [1.0, 2.0, 3.0, 4.0]

    def test_merge_single_partition(self):
        result = self.merger.merge_sorted_partitions([[1.0, 2.0, 3.0]])
        assert result == [1.0, 2.0, 3.0]

    def test_merge_two_sorted(self):
        a = [1.0, 3.0, 5.0]
        b = [2.0, 4.0, 6.0]
        result = self.merger.merge_two_sorted(a, b)
        assert result == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]

    def test_merge_result_is_sorted(self):
        import random
        partitions = [
            sorted([random.random() * 100 for _ in range(20)])
            for _ in range(5)
        ]
        result = self.merger.merge_sorted_partitions(partitions)
        assert result == sorted(result)


# ════════════════════════════════════════════
# BINARY SEARCH TESTS
# ════════════════════════════════════════════

class TestPipelineBinarySearch:

    def setup_method(self):
        self.bs = PipelineBinarySearch()

    def test_first_breach_found(self):
        latencies = [100, 150, 200, 250, 300, 350]
        result = self.bs.first_breach(latencies, threshold=220)
        assert result == 3
        assert latencies[result] == 250

    def test_first_breach_not_found(self):
        latencies = [100, 150, 200]
        result = self.bs.first_breach(latencies, threshold=500)
        assert result == -1

    def test_first_breach_all_above(self):
        latencies = [300, 400, 500]
        result = self.bs.first_breach(latencies, threshold=100)
        assert result == 0

    def test_min_batch_size(self):
        orders = [3, 6, 7, 11]
        result = self.bs.min_batch_size(orders, time_limit=8)
        assert result == 4

    def test_min_batch_size_single(self):
        orders = [10]
        result = self.bs.min_batch_size(orders, time_limit=10)
        assert result == 1

    def test_search_sorted_found(self):
        arr = [1.0, 3.0, 5.0, 7.0, 9.0]
        assert self.bs.search_sorted(arr, 5.0) == 2
        assert self.bs.search_sorted(arr, 1.0) == 0
        assert self.bs.search_sorted(arr, 9.0) == 4

    def test_search_sorted_not_found(self):
        arr = [1.0, 3.0, 5.0]
        assert self.bs.search_sorted(arr, 4.0) == -1

    def test_find_peak(self):
        values = [1.0, 3.0, 7.0, 12.0, 8.0, 4.0, 2.0]
        assert self.bs.find_peak(values) == 3
        assert values[self.bs.find_peak(values)] == 12.0
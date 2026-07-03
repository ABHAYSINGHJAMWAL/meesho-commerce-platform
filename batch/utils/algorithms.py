"""
DSA Algorithms — Production Implementations

Every algorithm here serves two purposes:
1. Solves a real business problem in the pipeline
2. Directly maps to LeetCode interview patterns

This file is the bridge between DSA practice
and production data engineering.
"""

from collections import defaultdict, deque
from typing import List, Dict, Tuple, Optional
import heapq
import time


# ════════════════════════════════════════════
# ALGORITHM 1 — HASHMAP
# Business use: inventory validation O(1) lookup
# LeetCode equivalent: Two Sum, Contains Duplicate
# ════════════════════════════════════════════

class InventoryHashMap:
    """
    HashMap for real-time inventory tracking.

    Why HashMap and not database query:
    Database query per order = 10-50ms latency.
    At 500,000 orders/hour = 500,000 DB queries/hour.
    HashMap in memory = O(1) = microseconds.

    Key: product_id (string)
    Value: available stock (integer)

    All operations O(1).
    """

    def __init__(self):
        self.stock: Dict[str, int] = {}
        self.reserved: Dict[str, int] = {}
        self.lookup_count = 0

    def set_stock(self, product_id: str, quantity: int):
        """Initialize or update stock. O(1)"""
        self.stock[product_id] = quantity
        self.reserved[product_id] = 0

    def get_available(self, product_id: str) -> int:
        """
        Get available stock = total minus reserved. O(1)

        Why subtract reserved:
        Prevents overselling during checkout window.
        Two customers cannot both buy the last unit.
        """
        self.lookup_count += 1
        total = self.stock.get(product_id, 0)
        reserved = self.reserved.get(product_id, 0)
        return max(0, total - reserved)

    def reserve(self, product_id: str, quantity: int) -> bool:
        """
        Atomic check and reserve. O(1)

        Returns True if reservation successful.
        Returns False if insufficient stock.
        """
        available = self.get_available(product_id)
        if available < quantity:
            return False
        self.reserved[product_id] = \
            self.reserved.get(product_id, 0) + quantity
        return True

    def confirm_sale(self, product_id: str, quantity: int):
        """Confirm order — reduce stock, release reservation. O(1)"""
        self.stock[product_id] = max(
            0, self.stock.get(product_id, 0) - quantity
        )
        self.reserved[product_id] = max(
            0, self.reserved.get(product_id, 0) - quantity
        )

    def find_low_stock(self, threshold: int = 5) -> List[str]:
        """
        Find products below threshold. O(n)
        Called every 60 seconds, not per order.
        """
        return [
            pid for pid in self.stock
            if 0 < self.get_available(pid) <= threshold
        ]

    def find_duplicates(self, product_ids: List[str]) -> List[str]:
        """
        Find duplicate product IDs in a batch load. O(n)
        LeetCode: Contains Duplicate
        """
        count = {}
        for pid in product_ids:
            count[pid] = count.get(pid, 0) + 1
        return [pid for pid, cnt in count.items() if cnt > 1]

    def two_sum_orders(
        self,
        amounts: List[float],
        target: float
    ) -> List[int]:
        """
        Find two order amounts that sum to target. O(n)
        Used in payment reconciliation.
        LeetCode: Two Sum
        """
        seen = {}
        for i, amount in enumerate(amounts):
            complement = target - amount
            if complement in seen:
                return [seen[complement], i]
            seen[amount] = i
        return []


# ════════════════════════════════════════════
# ALGORITHM 2 — MIN-HEAP
# Business use: real-time top-K sellers
# LeetCode equivalent: Top K Frequent Elements,
#                      Kth Largest Element
# ════════════════════════════════════════════

class TopKSellers:
    """
    Maintains top-k sellers by revenue using min-heap.

    Why min-heap not sort:
    Sort all sellers every update: O(n log n)
    Min-heap of size k: O(log k) per update

    At 10,000 sellers updating every second:
    Sort: 10,000 × log(10,000) = 130,000 ops/sec
    Heap: 10,000 × log(10) = 33,000 ops/sec
    4x faster. At 100,000 sellers: 10x faster.

    How it works:
    Heap always contains k LARGEST revenues.
    Heap root = SMALLEST of the top-k.
    New revenue > root → replace root → reheapify.
    New revenue <= root → not top-k → discard.
    """

    def __init__(self, k: int = 10):
        self.k = k
        self.heap: List[Tuple[float, str]] = []
        self.seller_revenue: Dict[str, float] = {}

    def update_seller(self, seller_id: str, revenue: float):
        """
        Update seller revenue and maintain top-k. O(log k)
        """
        self.seller_revenue[seller_id] = \
            self.seller_revenue.get(seller_id, 0) + revenue

        total = self.seller_revenue[seller_id]

        # Remove old entry if exists
        self.heap = [
            (r, s) for r, s in self.heap
            if s != seller_id
        ]
        heapq.heapify(self.heap)

        if len(self.heap) < self.k:
            heapq.heappush(self.heap, (total, seller_id))
        elif total > self.heap[0][0]:
            heapq.heapreplace(self.heap, (total, seller_id))

    def get_top_k(self) -> List[Tuple[str, float]]:
        """Return top-k sorted descending. O(k log k)"""
        return sorted(
            [(s, r) for r, s in self.heap],
            key=lambda x: x[1],
            reverse=True
        )

    def find_kth_largest(
        self,
        revenues: List[float],
        k: int
    ) -> float:
        """
        Find kth largest revenue. O(n log k)
        LeetCode: Kth Largest Element in Array
        """
        heap = []
        for revenue in revenues:
            heapq.heappush(heap, revenue)
            if len(heap) > k:
                heapq.heappop(heap)
        return heap[0]

    def top_k_frequent(
        self,
        events: List[str],
        k: int
    ) -> List[str]:
        """
        Find k most frequent events. O(n log k)
        LeetCode: Top K Frequent Elements
        """
        freq = {}
        for event in events:
            freq[event] = freq.get(event, 0) + 1

        heap = []
        for event, count in freq.items():
            heapq.heappush(heap, (count, event))
            if len(heap) > k:
                heapq.heappop(heap)

        return [event for count, event in
                sorted(heap, reverse=True)]


# ════════════════════════════════════════════
# ALGORITHM 3 — SLIDING WINDOW
# Business use: fraud velocity detection
# LeetCode equivalent: Sliding Window Maximum,
#                      Max Consecutive Ones
# ════════════════════════════════════════════

class FraudSlidingWindow:
    """
    Detects fraud using sliding time windows.

    Each seller has a deque of event timestamps.
    When new order arrives:
      1. Add timestamp to right of deque
      2. Remove timestamps older than window from left
      3. Length = orders in current window

    Why deque not list:
    list.pop(0) is O(n) — shifts all elements
    deque.popleft() is O(1) — constant time
    At 500,000 events/hour this matters enormously.

    LeetCode: Sliding Window Maximum uses same pattern.
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        self.windows: Dict[str, deque] = defaultdict(deque)
        self.flagged_sellers: Dict[str, int] = {}

    def add_event(self, seller_id: str, timestamp: float) -> int:
        """
        Add event and return count in window. O(1) amortized.

        Each timestamp added once and removed once.
        Total cost across all events = O(n).
        """
        window = self.windows[seller_id]
        window.append(timestamp)

        cutoff = timestamp - self.window_seconds
        while window and window[0] < cutoff:
            window.popleft()

        count = len(window)

        if count > 30:
            self.flagged_sellers[seller_id] = count

        return count

    def is_fraud(
        self,
        seller_id: str,
        threshold: int = 30
    ) -> bool:
        """Check if seller exceeded threshold in window."""
        return len(self.windows.get(seller_id, [])) > threshold

    def max_consecutive_active_days(
        self,
        daily_active: List[int],
        threshold: int
    ) -> int:
        """
        Find longest streak of days above threshold.
        LeetCode: Max Consecutive Ones variant.
        O(n) time, O(1) space.
        """
        max_streak = 0
        current = 0
        for count in daily_active:
            if count >= threshold:
                current += 1
                max_streak = max(max_streak, current)
            else:
                current = 0
        return max_streak

    def rolling_average(
        self,
        values: List[float],
        window_size: int
    ) -> List[float]:
        """
        Compute rolling average. O(n).
        Used in revenue trend monitoring.
        """
        result = []
        window_sum = 0.0

        for i, val in enumerate(values):
            window_sum += val
            if i >= window_size:
                window_sum -= values[i - window_size]
            size = min(i + 1, window_size)
            result.append(round(window_sum / size, 2))

        return result


# ════════════════════════════════════════════
# ALGORITHM 4 — TOPOLOGICAL SORT
# Business use: Airflow DAG task ordering
# LeetCode equivalent: Course Schedule,
#                      Find Order
# ════════════════════════════════════════════

class PipelineTopologicalSort:
    """
    Determines valid execution order for pipeline tasks.

    This is exactly what Airflow does internally
    when you define task dependencies with >>
    It runs topological sort to find execution order.

    Algorithm: Kahn's algorithm
    1. Count incoming edges (dependencies) per task
    2. Start with tasks having zero dependencies
    3. Process queue: remove task, decrement dependents
    4. If dependents reach zero, add to queue
    5. If all tasks processed = valid DAG
    6. If tasks remain = cycle detected

    O(V + E) where V = tasks, E = dependencies
    """

    def get_execution_order(
        self,
        tasks: List[str],
        dependencies: List[Tuple[str, str]]
    ) -> Tuple[List[str], bool]:
        """
        Returns (execution_order, is_valid).
        is_valid = False if cycle detected.

        LeetCode: Course Schedule II
        """
        graph = defaultdict(list)
        in_degree = {task: 0 for task in tasks}

        for src, dst in dependencies:
            graph[src].append(dst)
            in_degree[dst] += 1

        queue = deque([
            t for t in tasks if in_degree[t] == 0
        ])
        order = []

        while queue:
            task = queue.popleft()
            order.append(task)
            for dependent in graph[task]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        is_valid = len(order) == len(tasks)
        return order, is_valid

    def can_complete(
        self,
        num_tasks: int,
        prerequisites: List[Tuple[int, int]]
    ) -> bool:
        """
        Can all tasks complete given prerequisites?
        LeetCode: Course Schedule I
        """
        graph = defaultdict(list)
        in_degree = [0] * num_tasks

        for task, prereq in prerequisites:
            graph[prereq].append(task)
            in_degree[task] += 1

        queue = deque([
            i for i in range(num_tasks)
            if in_degree[i] == 0
        ])
        completed = 0

        while queue:
            task = queue.popleft()
            completed += 1
            for next_task in graph[task]:
                in_degree[next_task] -= 1
                if in_degree[next_task] == 0:
                    queue.append(next_task)

        return completed == num_tasks


# ════════════════════════════════════════════
# ALGORITHM 5 — K-WAY MERGE
# Business use: merging sorted Spark partitions
# LeetCode equivalent: Merge K Sorted Lists,
#                      Merge Sorted Array
# ════════════════════════════════════════════

class SparkPartitionMerger:
    """
    Merges k sorted outputs from Spark partitions.

    When Spark writes output in overwrite mode,
    each partition writes sorted data independently.
    Merging these sorted outputs is a k-way merge.

    Algorithm uses min-heap:
    1. Push first element from each partition
    2. Pop minimum → add to result
    3. Push next element from that partition
    4. Repeat until all partitions empty

    O(n log k) where n = total elements, k = partitions
    Much faster than naive merge O(nk).

    LeetCode: Merge K Sorted Lists (exact same pattern)
    """

    def merge_sorted_partitions(
        self,
        partitions: List[List[float]]
    ) -> List[float]:
        """
        Merge k sorted lists into one sorted list.
        O(n log k)
        """
        result = []
        heap = []

        for i, partition in enumerate(partitions):
            if partition:
                heapq.heappush(heap, (partition[0], i, 0))

        while heap:
            val, part_idx, elem_idx = heapq.heappop(heap)
            result.append(val)

            next_idx = elem_idx + 1
            if next_idx < len(partitions[part_idx]):
                heapq.heappush(heap, (
                    partitions[part_idx][next_idx],
                    part_idx,
                    next_idx
                ))

        return result

    def merge_two_sorted(
        self,
        a: List[float],
        b: List[float]
    ) -> List[float]:
        """
        Merge two sorted arrays. O(m + n)
        LeetCode: Merge Sorted Array
        """
        result = []
        i = j = 0

        while i < len(a) and j < len(b):
            if a[i] <= b[j]:
                result.append(a[i])
                i += 1
            else:
                result.append(b[j])
                j += 1

        result.extend(a[i:])
        result.extend(b[j:])
        return result


# ════════════════════════════════════════════
# ALGORITHM 6 — BINARY SEARCH
# Business use: finding SLA breach timestamp
# LeetCode equivalent: Search in Rotated Array,
#                      First Bad Version
# ════════════════════════════════════════════

class PipelineBinarySearch:
    """
    Binary search on sorted pipeline metrics.

    When did latency first exceed SLA threshold?
    When did fraud score first exceed limit?
    Binary search finds these in O(log n)
    instead of O(n) linear scan.

    LeetCode: First Bad Version uses exact same pattern.
    """

    def first_breach(
        self,
        values: List[float],
        threshold: float
    ) -> int:
        """
        Find index of first value exceeding threshold.
        Returns -1 if no breach found.
        O(log n)

        LeetCode: First Bad Version
        """
        left, right = 0, len(values) - 1
        result = -1

        while left <= right:
            mid = (left + right) // 2

            if values[mid] > threshold:
                result = mid
                right = mid - 1
            else:
                left = mid + 1

        return result

    def min_batch_size(
        self,
        orders_per_hour: List[int],
        time_limit: int
    ) -> int:
        """
        Find minimum batch size to process all orders
        within time limit.
        O(n log m) where m = max orders.

        Binary search on the answer — common DE pattern
        for capacity planning.
        LeetCode: Koko Eating Bananas
        """
        import math

        def hours_needed(batch_size: int) -> int:
            return sum(
                math.ceil(orders / batch_size)
                for orders in orders_per_hour
            )

        left = 1
        right = max(orders_per_hour)

        while left < right:
            mid = (left + right) // 2
            if hours_needed(mid) <= time_limit:
                right = mid
            else:
                left = mid + 1

        return left

    def search_sorted(
        self,
        arr: List[float],
        target: float
    ) -> int:
        """
        Standard binary search. O(log n)
        LeetCode: Binary Search
        """
        left, right = 0, len(arr) - 1

        while left <= right:
            mid = (left + right) // 2
            if arr[mid] == target:
                return mid
            elif arr[mid] < target:
                left = mid + 1
            else:
                right = mid - 1

        return -1

    def find_peak(self, values: List[float]) -> int:
        """
        Find peak load hour without scanning all hours.
        O(log n)
        LeetCode: Find Peak Element
        """
        left, right = 0, len(values) - 1

        while left < right:
            mid = (left + right) // 2
            if values[mid] > values[mid + 1]:
                right = mid
            else:
                left = mid + 1

        return left
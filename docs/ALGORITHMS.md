# DSA Algorithms in Production

## Why This Document Exists

Every algorithm in this platform solves a real business
problem AND maps directly to a LeetCode interview pattern.
This document explains both for each algorithm.

---

## 1. HashMap — Inventory Validation

**Business problem:**
Check stock for 500,000 orders per hour.
Database query per order = system collapse.
HashMap = O(1) = microseconds.

**Implementation:** InventoryHashMap in algorithms.py
- Key: product_id
- Value: available stock
- reserve() prevents oversell race conditions
- confirm_sale() reduces stock after payment

**LeetCode patterns:**
- Two Sum (find two amounts summing to target)
- Contains Duplicate (find duplicate order IDs)
- Group Anagrams (group events by type)

---

## 2. Min-Heap — Top K Sellers

**Business problem:**
Show top 10 sellers on live dashboard.
Sorting all sellers every second = O(n log n) per update.
Min-heap of size 10 = O(log 10) per update.

**Implementation:** TopKSellers in algorithms.py
- Heap root = smallest of top-k (easy eviction)
- New revenue > root → replace → reheapify
- get_top_k() returns sorted descending

**LeetCode patterns:**
- Top K Frequent Elements
- Kth Largest Element in Array
- Find Median from Data Stream

---

## 3. Sliding Window — Fraud Detection

**Business problem:**
Count orders per seller in last 60 seconds.
Naively scanning all events = O(n) per check.
Deque sliding window = O(1) amortized.

**Implementation:** FraudSlidingWindow in algorithms.py
- Each seller has its own deque
- Add timestamp to right on new event
- Remove expired timestamps from left
- Length = current count in window

**LeetCode patterns:**
- Sliding Window Maximum
- Max Consecutive Ones
- Minimum Window Substring

---

## 4. Topological Sort — Pipeline Ordering

**Business problem:**
Airflow tasks must run in dependency order.
check_kafka must complete before run_spark.
run_spark must complete before validate_output.

**Implementation:** PipelineTopologicalSort in algorithms.py
- Kahn's algorithm with in-degree counting
- Cycle detection (invalid DAG caught early)
- Same algorithm Airflow uses internally

**LeetCode patterns:**
- Course Schedule I (can all tasks complete?)
- Course Schedule II (find execution order)
- Alien Dictionary

---

## 5. K-Way Merge — Spark Partition Merging

**Business problem:**
PySpark writes one sorted file per partition.
Merging k sorted outputs efficiently.
Naive approach O(nk). Heap approach O(n log k).

**Implementation:** SparkPartitionMerger in algorithms.py
- Push first element from each partition to heap
- Pop minimum → output → push next from that partition
- Repeat until all partitions exhausted

**LeetCode patterns:**
- Merge K Sorted Lists
- Merge Sorted Array
- Sort an Array

---

## 6. Binary Search — Threshold Detection

**Business problem:**
When did pipeline latency first exceed SLA threshold?
Linear scan O(n). Binary search O(log n).
On 1 million log entries: 1M vs 20 operations.

**Implementation:** PipelineBinarySearch in algorithms.py
- first_breach: find first index above threshold
- min_batch_size: binary search on the answer
- find_peak: find peak load hour

**LeetCode patterns:**
- First Bad Version
- Search in Rotated Sorted Array
- Koko Eating Bananas
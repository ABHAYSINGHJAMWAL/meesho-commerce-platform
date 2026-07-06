# Business Problem

## The Three Failures Every Indian E-Commerce Platform Faces

### Problem 1 — Inventory Overselling

A seller lists 100 units of a kurta.
500 customers place orders simultaneously during a sale.
All 500 receive "Order Confirmed."
Only 100 can be fulfilled.
400 cancellations follow.
400 angry customers. 400 bad reviews.
Trust is destroyed in minutes.

**Root cause:** No real-time inventory validation before
order confirmation. Batch systems check stock too late.

**This platform solves it:** Real-time inventory validator
using HashMap O(1) lookups checks stock before confirming
every single order. Reserved stock prevents race conditions
between simultaneous buyers.

---

### Problem 2 — Stale Business Intelligence

CEO asks at 2pm on a sale day:
"Which categories are selling fastest right now?"

Without real-time pipeline:
Answer comes from last night's batch — 14 hours stale.
₹10 crore of marketing budget allocated on wrong data.
Decisions made on information that is no longer true.

**Root cause:** Traditional batch pipelines run once daily.
Business moves faster than the data.

**This platform solves it:** Live metrics aggregator
processes every order event within seconds.
Revenue by category, top sellers, payment method
distribution — all updated in real time.

---

### Problem 3 — Invisible Seller Fraud

A seller creates 500 fake orders in 60 seconds
to manipulate rankings and appear as a top seller.
Batch pipeline detects this next morning.
By then the seller has ranked #1 for 12 hours.
Legitimate sellers lost business.
Platform credibility damaged.

**Root cause:** Batch fraud detection is too slow.
By the time fraud is caught, damage is done.

**This platform solves it:** Real-time fraud detector
using sliding window algorithm flags sellers
exceeding 30 orders per minute within seconds.
Batch layer catches sophisticated patterns
requiring historical context that real-time cannot see.

---

## Scale Context

This platform is designed for Meesho-scale commerce:
- 500,000+ order events per hour at peak
- Sale days see 10x normal traffic spikes
- Tier 2 and tier 3 cities dominate order volume
- UPI accounts for 45% of payments
- Women Fashion is 35% of order volume

All simulated data reflects these real distributions.
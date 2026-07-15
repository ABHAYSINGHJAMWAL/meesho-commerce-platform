"""
Customer Review and Support Ticket Generator

Generates realistic Meesho-style unstructured text data.
This simulates what real e-commerce platforms receive:
- Customer reviews after delivery
- Seller support tickets
- Product quality complaints
- Delivery feedback

Why simulate this:
Real review data is private. Simulated data with
realistic Indian e-commerce patterns lets us build
and test the entire AI pipeline.
"""

import random
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List
import json

POSITIVE_REVIEWS = [
    "Bahut accha product mila! Quality bilkul expected jaise thi. Fast delivery bhi. 5 star deta hoon.",
    "Excellent quality kurta. Stitching is perfect and material is very soft. Will order again.",
    "Package came in 3 days, product exactly as shown in photo. Very happy with purchase.",
    "Good quality saree at amazing price. My family loved it. Highly recommend this seller.",
    "Mobile cover fit perfectly. Material is strong. Delivery was quick. Thank you Meesho!",
    "Best deal I found online. Product quality exceeded my expectations. Fast shipping.",
    "Legging quality is great, very comfortable. Ordered 3 pieces, all perfect. Great seller.",
    "Earrings look exactly like photos, beautiful design. Packed very well. Happy customer.",
]

NEGATIVE_REVIEWS = [
    "Product quality is very bad. Color completely different from photo. Requesting refund.",
    "Delivery took 15 days, product damaged when it arrived. Very disappointed.",
    "Size mentioned XL but received M. Wrong product sent. Want replacement immediately.",
    "Seller not responding to messages. Product is fake, not original brand. Reporting this.",
    "Worst experience ever. Product broke after one day. Quality is zero. Avoid this seller.",
    "Duplicate product received. Photos show different item. Fraud seller on platform.",
    "Package was torn, product inside damaged. No proper packaging. Very bad service.",
    "Color is completely different from website photos. Misleading advertisement. Want refund.",
]

NEUTRAL_REVIEWS = [
    "Product is okay. Quality is average for the price. Delivery was on time.",
    "Decent quality. Nothing special but nothing bad either. Will consider ordering again.",
    "Package arrived on time. Product is as described. Average quality.",
    "Material is okay, not premium but acceptable. Price is reasonable.",
    "Delivery was fast but product quality is just average. Expected better.",
]

SUPPORT_TICKETS = [
    "My order {order_id} has not arrived yet. It has been 10 days. Please help.",
    "I received wrong size in order {order_id}. I ordered XL but got S. Need exchange.",
    "Seller is not picking up return request for order {order_id}. Please intervene.",
    "Payment deducted but order {order_id} not confirmed. Please check immediately.",
    "Product in order {order_id} looks fake and different from photos. Want full refund.",
    "Delivery person demanded extra cash for order {order_id}. This is not acceptable.",
]

CATEGORIES = ["Women Fashion", "Electronics", "Home Decor", "Beauty", "Kids", "Men Fashion"]
CITIES = ["Jaipur", "Mumbai", "Delhi", "Bangalore", "Surat", "Patna", "Lucknow", "Indore"]


@dataclass
class CustomerReview:
    review_id: str
    order_id: str
    seller_id: str
    customer_id: str
    product_category: str
    city: str
    rating: int
    review_text: str
    is_support_ticket: bool
    timestamp: str
    language_hint: str

    def to_dict(self):
        return asdict(self)

    def to_json(self):
        return json.dumps(asdict(self), default=str)


class ReviewGenerator:
    """
    Generates realistic customer reviews and support tickets.

    Distribution mirrors real e-commerce:
    - 60% positive (happy customers rarely write reviews
                     but Meesho's tier2/tier3 users do)
    - 25% negative (unhappy customers always write)
    - 15% neutral
    """

    def __init__(self):
        self.seller_ids = [f"SELL-{i:04d}" for i in range(50)]
        self.customer_ids = [f"CUST-{i:06d}" for i in range(10000)]

    def generate_review(self) -> CustomerReview:
        sentiment_roll = random.random()
        is_ticket = random.random() < 0.15

        if is_ticket:
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            text = random.choice(SUPPORT_TICKETS).format(order_id=order_id)
            rating = random.choice([1, 2])
        elif sentiment_roll < 0.60:
            text = random.choice(POSITIVE_REVIEWS)
            rating = random.choice([4, 5])
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        elif sentiment_roll < 0.85:
            text = random.choice(NEGATIVE_REVIEWS)
            rating = random.choice([1, 2])
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        else:
            text = random.choice(NEUTRAL_REVIEWS)
            rating = 3
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"

        return CustomerReview(
            review_id=f"REV-{uuid.uuid4().hex[:12].upper()}",
            order_id=order_id,
            seller_id=random.choice(self.seller_ids),
            customer_id=random.choice(self.customer_ids),
            product_category=random.choice(CATEGORIES),
            city=random.choice(CITIES),
            rating=rating,
            review_text=text,
            is_support_ticket=is_ticket,
            timestamp=datetime.now(timezone.utc).isoformat(),
            language_hint="hinglish" if any(
                word in text.lower()
                for word in ["bahut", "accha", "mila", "bilkul", "deta", "hoon"]
            ) else "english"
        )

    def generate_batch(self, n: int = 50) -> List[CustomerReview]:
        return [self.generate_review() for _ in range(n)]


if __name__ == "__main__":
    gen = ReviewGenerator()
    batch = gen.generate_batch(10)

    print(f"Generated {len(batch)} reviews\n")
    for review in batch[:3]:
        print(f"ID: {review.review_id}")
        print(f"Rating: {review.rating}/5")
        print(f"Text: {review.review_text[:100]}...")
        print(f"Category: {review.product_category} | City: {review.city}")
        print(f"Support ticket: {review.is_support_ticket}")
        print()
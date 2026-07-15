from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import random
from typing import Dict, List, Optional
import uuid

CITIES_BY_TIER = {
    'tier1': {
        'Mumbai': 'Maharashtra',
        'Delhi': 'Delhi',
        'Bangalore': 'Karnataka',
        'Hyderabad': 'Telangana',
        'Chennai': 'Tamil Nadu',
    },
    'tier2': {
        'Jaipur': 'Rajasthan',
        'Ahmedabad': 'Gujarat',
        'Lucknow': 'Uttar Pradesh',
        'Indore': 'Madhya Pradesh',
    },
    'tier3': {
        'Jodhpur': 'Rajasthan',
        'Patna': 'Bihar',
        'Nashik': 'Maharashtra',
    },
}

ALL_CITIES = {
    **CITIES_BY_TIER['tier1'],
    **CITIES_BY_TIER['tier2'],
    **CITIES_BY_TIER['tier3'],
}

CITY_LIST = list(ALL_CITIES.keys())

PAYMENT_METHODS = [
    'UPI',
    'Credit Card',
    'Debit Card',
    'Net Banking',
    'Cash on Delivery',
]

PAYMENT_WEIGHTS = [45, 20, 15, 10, 10]

SELLER_REGIONS = ['MH', 'DL', 'KA', 'TN', 'RJ']


@dataclass
class OrderEvent:
    event_id: str
    event_type: str
    order_id: str
    customer_id: str
    seller_id: str
    product_id: str
    amount_inr: float
    payment_method: str
    city: str
    state: str
    timestamp: str

    def to_dict(self):
        return asdict(self)

    def to_json(self):
        return json.dumps(asdict(self))


class MeeshoEventGenerator:

    def __init__(self, num_sellers=100, num_customers=1000, num_products=500):

        self.seller_ids = [f"SELL-{i}" for i in range(num_sellers)]

        self.customer_ids = [f"CUST-{i}" for i in range(num_customers)]

        self.product_ids = [f"PROD-{i}" for i in range(num_products)]

    def generate_order_event(self):

        city = random.choice(CITY_LIST)
        state = ALL_CITIES[city]

        return OrderEvent(
            event_id=str(uuid.uuid4()),
            event_type="order_placed",
            order_id=str(uuid.uuid4()),
            customer_id=random.choice(self.customer_ids),
            seller_id=random.choice(self.seller_ids),
            product_id=random.choice(self.product_ids),
            amount_inr=round(random.uniform(100, 5000), 2),
            payment_method=random.choices(
                PAYMENT_METHODS, weights=PAYMENT_WEIGHTS
            )[0],
            city=city,
            state=state,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )


if __name__ == "__main__":

    gen = MeeshoEventGenerator()

    order = gen.generate_order_event()

    print(order.to_dict())
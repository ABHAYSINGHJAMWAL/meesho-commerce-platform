class Config:
    KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

    KAFKA_ORDER_TOPIC = "meesho-orders"
    KAFKA_INVENTORY_TOPIC = "meesho-inventory"
    KAFKA_FRAUD_TOPIC = "meesho-fraud-signals"

config = Config()
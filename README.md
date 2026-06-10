# Meesho Commerce Platform

## Architecture

Event Generator
↓
Kafka Producer
↓
Kafka Topic
↓
Kafka Consumer

## Tech Stack

- Python
- Kafka
- Docker
- Zookeeper

## Components

### event_generator.py
Generates fake Indian e-commerce orders.

### kafka_producer.py
Publishes orders to Kafka topic.

### kafka_consumer.py
Consumes orders from Kafka topic.
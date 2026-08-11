import json
import time
import random
from datetime import datetime,timezone
from kafka import KafkaProducer
from faker import Faker


fake = Faker()


producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'raw_orders'

PRODUCTS = [
    {"product_id": "P101", "name": "Mechanical Keyboard", "category": "Electronics", "price": 120.0},
    {"product_id": "P102", "name": "Wireless Mouse", "category": "Electronics", "price": 45.0},
    {"product_id": "P103", "name": "Ergonomic Chair", "category": "Furniture", "price": 299.99},
    {"product_id": "P104", "name": "Coffee Beans 1kg", "category": "Groceries", "price": 22.50},
    {"product_id": "P105", "name": "Python Data Engineering Book", "category": "Books", "price": 55.0},
]

def generate_order_event():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1,3)

    return {
        "order_id": fake.uuid4(),
        "user_id": f"USER_{random.randint(1000, 1050)}",
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": quantity,
        "total_amount": round(product["price"] * quantity, 2),
        "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "BLIK"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    print('Starting Kafka Producer')

    try:
        while True:
            event = generate_order_event()
            producer.send(TOPIC_NAME,value=event)
            time.sleep(random.uniform(0.5,2.0))
    except KeyboardInterrupt:
        print('stopping producer')
        producer.flush()
        producer.close()

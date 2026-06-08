import json
import time
import random
from datetime import datetime, timezone, timedelta  # Updated import
from confluent_kafka import Producer

# Configuration mapping to your local Docker Kafka broker
conf = {'bootstrap.servers': '127.0.0.1:9094'}
producer = Producer(conf)

def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Event delivered to {msg.topic()} [{msg.partition()}]")

while True:
    # Modern, timezone-aware UTC timestamp
    now = datetime.now(timezone.utc)
    
    # Spark Internals Trick: Simulate 5% late data
    if random.random() < 0.05:
        event_time = now - timedelta(minutes=15) 
    else:
        event_time = now

    payload = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "session_id": f"sess_{random.randint(100000, 999999)}",
        "event_time": event_time.isoformat(),  # Includes '+00:00' timezone offset cleanly
        "page_url": random.choice(["/home", "/product/99", "/cart", "/checkout"]),
        "action": random.choice(["view", "click", "add_to_cart", "purchase"]),
        "ip_address": f"192.168.1.{random.randint(1, 254)}"
    }

    # Asynchronously push JSON serialized strings to the clickstream topic
    producer.produce(
        'clickstream', 
        value=json.dumps(payload).encode('utf-8'), 
        callback=delivery_callback
    )
    producer.poll(0) 
    
    time.sleep(random.uniform(0.1, 0.5))
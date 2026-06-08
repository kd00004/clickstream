from confluent_kafka import Consumer, KafkaError
import json

# Consumer configuration mapping to your local Docker Kafka broker
conf = {
    'bootstrap.servers': '127.0.0.1:9094',
    'group.id': 'local-sanity-check',
    'auto.offset.reset': 'earliest' # Read from the beginning of the topic if no offset is committed
}

consumer = Consumer(conf)
consumer.subscribe(['clickstream'])

print("🚀 Listening for live clickstream events... Press Ctrl+C to stop.\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event; not an actual error
                continue
            else:
                print(f"❌ Kafka Error: {msg.error()}")
                break
        
        # Deserialize the incoming JSON bytes
        payload = json.loads(msg.value().decode('utf-8'))
        
        # Format a clean terminal printout
        print(f"📥 [RECIEVED] User: {payload['user_id']} | "
              f"Action: {payload['action'].upper():<11} | "
              f"Time: {payload['event_time']}")

except KeyboardInterrupt:
    print("\nStopping sanity check consumer...")
finally:
    # Clean up and close connection cleanly
    consumer.close()
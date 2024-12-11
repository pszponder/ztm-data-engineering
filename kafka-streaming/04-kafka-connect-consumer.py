import base64
from decimal import Decimal
import json
from confluent_kafka import Consumer, KafkaException, KafkaError


# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'postgres-price-consumer',  # Consumer group ID
    'auto.offset.reset': 'earliest',        # Start consuming from the beginning
}

def main():
    # Create the Kafka consumer
    consumer = Consumer(conf)

    # Define the topic to subscribe to
    topic = 'postgres-.public.orders'
    consumer.subscribe([topic])

    try:
        print(f"Consuming messages from topic '{topic}'...")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue 
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}]")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_message(msg)

    except KeyboardInterrupt:
        print("Consumer interrupted by user.")

    finally:
        print("Closing consumer...")
        consumer.close()

def process_message(msg):
    value = msg.value()
    try:
        order = json.loads(value.decode('utf-8'))
        total_amount_bytes = order.get('payload', {}).get('after', {}).get('total_amount')
        total_amount = decode_decimal(total_amount_bytes)
        print(f"Received order with total amount={total_amount}")
    except json.JSONDecodeError as e:
        print(f"[{consumer_name}] Failed to decode JSON: {e}")

def decode_decimal(encoded_string, scale=2):
    """Decode a base64-encoded Kafka Connect Decimal to a Python Decimal."""
    # Decode the base64 string to bytes
    value_bytes = base64.b64decode(encoded_string)
    # Convert the bytes to an integer (big-endian, signed)
    unscaled_value = int.from_bytes(value_bytes, byteorder="big", signed=True)
    # Adjust for the scale
    return Decimal(unscaled_value) / Decimal(10 ** scale)

if __name__ == '__main__':
    main()
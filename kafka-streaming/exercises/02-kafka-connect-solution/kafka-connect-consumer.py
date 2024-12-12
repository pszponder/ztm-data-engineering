import json

from confluent_kafka import Consumer, KafkaError, KafkaException

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-price-consumer",
    "auto.offset.reset": "earliest",
}


def main():
    consumer = Consumer(conf)

    topic = "postgres-.public.orders"
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
        order = json.loads(value.decode("utf-8"))
        payload = order.get("payload", {})

        before = payload.get("before", None)
        after = payload.get("after", None)

        if not before or not after:
            return

        before_status = before.get("status")
        after_status = after.get("status")

        if before_status == "processed" and after_status == "refunded":
            print(
                f"Status changed from 'processed' to 'refunded' for order: {order.get('payload', {}).get('after', {}).get('id')}"
            )
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")


if __name__ == "__main__":
    main()

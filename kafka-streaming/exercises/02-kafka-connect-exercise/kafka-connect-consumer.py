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

    finally:
        print("Closing consumer...")
        consumer.close()


def process_message(msg):
    # TODO: Process incoming WAL record
    # Print a string message if two conditions are true:
    # * If a message is for an update operation
    # * If an order status has changed from "processed" to "refunded"
    pass

if __name__ == "__main__":
    main()

import json

from confluent_kafka import Consumer, KafkaError, KafkaException

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "postgres-price-consumer",
    "auto.offset.reset": "earliest",
}

# TODO: Read the "README.md" for instructions on how to set up this exercise

def main():
    consumer = Consumer(conf)

    topic = "postgres-.public.orders"
    consumer.subscribe([topic])

    try:
        print(f"Consuming messages from topic '{topic}'")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            process_message(msg)

    finally:
        consumer.close()


def process_message(msg):
    # TODO: Process incoming WAL record
    # Print a string message if two conditions are true:
    # * If a message is for an update operation
    # * If an order status has changed from "processed" to "refunded"
    # 
    # Note: If you go though the steps in the README.md,
    # each record will contain the "payload" object two fields:
    # * `before` - a snapshot of a database record before it was updated
    # * `after` - a snapshot of a database record after it was updated
    #
    # You will need to extract the "status" column values from both records and compare their values
    # 
    # To get those field you should do something like this:
    #
    # ```py
    # before = wal_record["payload"]["before"]
    # after = wal_record["payload"]["after"]
    # ```
    #
    # But keep in mind that one or both of these fields can be None for some events.
    pass

if __name__ == "__main__":
    main()

import argparse
import json

from order import Order, ORDER_SCHEMA

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

def main():
    parser = argparse.ArgumentParser(description="Test Kafka consumer")
    parser.add_argument("--group-id", "-g", help="Consumer group ID")
    parser.add_argument("--topic-name", "-t", help="Topic name ")

    args = parser.parse_args()

    group_id = args.group_id
    topic_name = args.topic_name

    schema_registry_conf = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        json.dumps(ORDER_SCHEMA),
        lambda obj, ctx: Order.from_dict(obj),
    )

    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(config)
    consumer.subscribe([topic_name])

    print(f"Starting consumer with group ID '{group_id}'")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # No new messages
                continue
            if msg.error():
                # Error while reading
                print(f"Error encountered: {msg.error()}")
                continue

            process_message(avro_deserializer, msg)

    finally:
        consumer.close()


def process_message(avro_deserializer, msg):
    order = avro_deserializer(
        msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
    )
    if order.total_price < 250:
        return

    print(f"Received order price={order.total_price}")


if __name__ == "__main__":
    main()

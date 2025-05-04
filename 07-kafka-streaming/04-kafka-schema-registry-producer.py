import json
import random
import textwrap
import time
from datetime import datetime
from order import Order, ORDER_SCHEMA

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)


def generate_order():
    countries = [
        "USA",
        "Canada",
        "UK",
        "Germany",
        "France",
        "Australia",
        "Japan",
        "Ireland",
    ]
    return Order(
        order_id=random.randint(1000, 9999),
        customer_id='123',
        total_price=round(random.uniform(20.0, 1000.0), 2),
        customer_country=random.choice(countries),
        merchant_country=random.choice(countries),
        order_datetime=datetime.now().isoformat()
    )


def main():
    schema_registry_config = {
        "url": "http://localhost:8081"
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    avro_serializer = AvroSerializer(
        schema_registry_client,
        json.dumps(ORDER_SCHEMA),
        lambda obj, ctx: obj.to_dict()
    )

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "acks": "all"
    }

    producer = Producer(producer_config)

    topic = "orders.avro"

    def delivery_callback(err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            print(
                textwrap.dedent(
                f"""
                    Produced event to topic {msg.topic()}:
                    key = {msg.key().decode('utf-8')}
                """)
            )

    while True:
        order = generate_order()
        print(f"Sending order: {order}")

        serialized_data = avro_serializer(
            order, SerializationContext(topic, MessageField.VALUE)
        )
        producer.produce(
            topic,
            key=str(order.order_id).encode(),
            value=serialized_data,
            callback=delivery_callback,
        )

        producer.poll(0)

        time.sleep(1)


if __name__ == "__main__":
    main()

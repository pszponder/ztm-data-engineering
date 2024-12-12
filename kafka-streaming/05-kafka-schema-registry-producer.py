import json
import random
import textwrap
import time
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

order_schema = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_id", "type": "int"},
        {"name": "total_price", "type": "float"},
        {"name": "customer_country", "type": "string"},
        {"name": "merchant_country", "type": "string"},
        {"name": "order_date", "type": "string"},
    ],
}


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
    order = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 1000),
        "total_price": round(random.uniform(20.0, 1000.0), 2),
        "customer_country": random.choice(countries),
        "merchant_country": random.choice(countries),
        "order_date": datetime.now().isoformat(),
    }
    return order


def main():
    schema_registry_conf = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client, json.dumps(order_schema), lambda obj, ctx: obj
    )

    config = {"bootstrap.servers": "localhost:9092", "acks": "all"}

    producer = Producer(config)

    topic = "orders.avro"

    def delivery_callback(err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            print(
                textwrap.dedent(f"""
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
            key=str(order["customer_id"]).encode(),
            value=serialized_data,
            callback=delivery_callback,
        )

        producer.poll(1)

        time.sleep(1)


if __name__ == "__main__":
    main()

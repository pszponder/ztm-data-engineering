import argparse
import json
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


order_schema = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_id", "type": "int"},
        {"name": "total_price", "type": "float"},
        {"name": "customer_country", "type": "string"},
        {"name": "merchant_country", "type": "string"},
        {"name": "order_date", "type": "string"}
    ]
}

def main():
    parser = argparse.ArgumentParser(description='Test Kafka consumer')
    parser.add_argument('--group-id', '-g', help='Consumer group ID')
    parser.add_argument('--topic-name', '-t', help='Topic name ')
    parser.add_argument('--name', '-n', help='Name of this consumer')

    args = parser.parse_args()

    group_id = args.group_id
    topic_name = args.topic_name
    consumer_name = args.name

    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        json.dumps(order_schema),
        lambda obj, ctx: obj  # Return dict as is
    )

    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)
    consumer.subscribe([topic_name])

    print(f"[{consumer_name}] Starting consumer with group ID '{group_id}'")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # No new messages
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    # Error while reading 
                    print(f"[{consumer_name}]Error encountered: {msg.error()}")
                    continue

            process_message(consumer_name, avro_deserializer, msg)

    except KeyboardInterrupt:
        print("[{consumer_name}]Stopping consumer...")
    finally:
        consumer.close()


def process_message(consumer_name, avro_deserializer, msg):
    value = msg.value()
    try:
        order = avro_deserializer(
            msg.value(),
            SerializationContext(msg.topic(), MessageField.VALUE)
        )
        price = order.get('total_price', 0)
        if price > 250:
            print(f"[{consumer_name}] [partition={msg.partition()}] " +  
            f"Received order price={price}")
    except json.JSONDecodeError as e:
        print(f"[{consumer_name}] Failed to decode JSON: {e}")

if __name__ == '__main__':
    main()
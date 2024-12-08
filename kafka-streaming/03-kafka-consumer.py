import argparse
import json
from confluent_kafka import Consumer, KafkaError

def main():
    parser = argparse.ArgumentParser(description='Test Kafka consumer')
    parser.add_argument('--group-id', '-g', help='Consumer group ID')
    parser.add_argument('--topic-name', '-t', help='Topic name ')
    parser.add_argument('--name', '-n', help='Name of this consumer')

    args = parser.parse_args()

    group_id = args.group_id
    topic_name = args.topic_name
    consumer_name = args.name

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

            process_message(consumer_name, msg)

    except KeyboardInterrupt:
        print("[{consumer_name}]Stopping consumer...")
    finally:
        consumer.close()


def process_message(consumer_name, msg):
    value = msg.value()
    try:
        order = json.loads(value.decode('utf-8'))
        price = order.get('total_price', 0)
        if price > 250:
            print(f"[{consumer_name}] [partition={msg.partition()}] " +  
            f"Received order price={price}")
    except json.JSONDecodeError as e:
        print(f"[{consumer_name}] Failed to decode JSON: {e}")

if __name__ == '__main__':
    main()
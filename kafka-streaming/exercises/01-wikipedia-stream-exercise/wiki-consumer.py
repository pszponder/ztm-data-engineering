import os
import sys
import json
from confluent_kafka import Consumer, KafkaError


consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'wiki-consumer-group',
    'auto.offset.reset': 'earliest'
}
kafka_topic = 'wikipedia-changes'


def main():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_topic])

    print(f"Consuming messages from topic '{kafka_topic}'")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}", file=sys.stderr)
                    continue
            
            message_value = msg.value().decode('utf-8')
            # print(f"Received message from topic '{msg.topic()}': {message_value}")

            event = json.loads(message_value)

            bot = event.get('bot', False)
            minor = event.get('minor', True)
            title = event.get('title', 'Unknown')
            user = event.get('user', 'Unknown')

            if bot and not minor:
                print(f"Major bot edit detected**: User '{user}' edited '{title}'")

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

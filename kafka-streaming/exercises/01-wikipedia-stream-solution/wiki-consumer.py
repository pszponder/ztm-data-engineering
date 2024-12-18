import json
import sys

from confluent_kafka import Consumer

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "wiki-consumer-group",
    "auto.offset.reset": "earliest",
}
kafka_topic = "wikipedia-changes"


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
                print(f"Error: {msg.error()}", file=sys.stderr)
                continue

            message_value = msg.value().decode("utf-8")

            event = json.loads(message_value)

            bot = event.get("bot", False)
            minor = event.get("minor", True)
            title = event.get("title", "Unknown")
            user = event.get("user", "Unknown")

            if bot and not minor:
                print(f"Major bot edit detected: User '{user}' edited '{title}'")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

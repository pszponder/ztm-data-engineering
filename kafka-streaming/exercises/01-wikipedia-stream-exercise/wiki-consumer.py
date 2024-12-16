import json
import sys

from confluent_kafka import Consumer, KafkaError

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
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}", file=sys.stderr)
                    continue

            # TODO: Print a message about a Wikipedia edit if two conditions are true:
            # * If a change was made by a bot
            # * If a change is not minor
            # 
            # The printed messages should include the name of an author making a change and 
            # the title of a changed page
            

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

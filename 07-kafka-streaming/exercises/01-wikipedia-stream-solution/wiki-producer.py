import json
import textwrap

from confluent_kafka import Producer
from sseclient import SSEClient

producer_conf = {"bootstrap.servers": "localhost:9092"}
kafka_topic = "wikipedia-changes"


def delivery_callback(err, msg):
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(
            textwrap.dedent(f"""
        Produced event to topic {msg.topic()}:
        key = {msg.key().decode('utf-8')}
        value = {msg.value().decode('utf-8')}
        """)
        )


def main():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"

    print(
        f"Starting to consume Wikipedia recent changes from {url} and produce to Kafka topic '{kafka_topic}'..."
    )

    producer = Producer(producer_conf)
    messages = SSEClient(url)

    for event in messages:
        if event.event == "message" and event.data:
            try:
                data = json.loads(event.data)
            except json.JSONDecodeError:
                continue

            id = data.get("id")
            message = {
                "id": id,
                "type": data.get("type"),
                "title": data.get("title"),
                "user": data.get("user"),
                "bot": data.get("bot"),
                "timestamp": data.get("timestamp"),
                "comment": data.get("comment"),
                "minor": data.get("minor", False),
            }

            value = json.dumps(message)
            producer.produce(
                topic=kafka_topic,
                key=str(id),
                value=value,
                callback=delivery_callback,
            )
            producer.poll(0)

    producer.flush()


if __name__ == "__main__":
    main()

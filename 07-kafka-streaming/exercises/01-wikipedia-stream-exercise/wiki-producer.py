import json
import textwrap

from confluent_kafka import Producer
from sseclient import SSEClient

producer_conf = {"bootstrap.servers": "localhost:9092"}
kafka_topic = "wikipedia-changes"

# TODO: Before running a producer.
# 1. Install Kafka CLI tools
#
# ```
# brew install kafka
# ```
#
# 2. Start Kafka
# 
# ```
# docker-compose up
# ```
#
# 3. Create a virtual environment and install dependencies
#
# ```
# python3 -m venv venv
# source venv/bin/activate
# pip install -r requirements.txt
# ```

# TODO: Read docs about Wikipedia edit stream: https://www.mediawiki.org/wiki/Manual:RCFeed

def delivery_report(err, msg):
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

            print(data)

            # TODO: Produce a Kafka messages from a Wikistream update message
            # * Parse the input message
            # * Extract fields you need to write 
            # * Create a JSON object for a new Kafka even
            # * Write a messages to a Kafka topic
            # 
            # To test your producer, run the following command:
            #
            # ```
            # kafka-console-consumer --bootstrap-server localhost:9092 --topic wikipedia-changes --from-beginning
            # ```

    producer.flush()


if __name__ == "__main__":
    main()

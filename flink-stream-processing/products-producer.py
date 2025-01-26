from confluent_kafka import Producer
import sys
import textwrap

def delivery_callback(err, msg):
    if err:
        print("ERROR: Message failed delivery: {}".format(err))
    else:
        print(
            textwrap.dedent(
            f"""
                Produced event to topic {msg.topic()}:
                value = {msg.value().decode('utf-8')}
            """)
        )

def main():
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
    }
    producer = Producer(producer_config)

    print("Enter products's data")
    try:
        while True:
            json_line = input("> ").strip()
            if json_line:
                producer.produce(
                    "products",
                    key=None,
                    value=json_line,
                    callback=delivery_callback
                )
                producer.poll(1)
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
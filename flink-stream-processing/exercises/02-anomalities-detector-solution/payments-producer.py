import json
import random
import textwrap
import time
from datetime import datetime

from confluent_kafka import Producer


def generate_payment():
    payment_id = f"payment-{random.randint(1000, 9999)}"
    user_id = f"user-{random.randint(1, 50)}"
    merchant_id = f"merchant-{random.randint(1, 20)}"

    if random.randint(1, 10) < 2:
        amount = round(random.uniform(10.0, 10000.0), 2)
    else:
        amount = round(random.uniform(10.0, 1000.0), 2)
    payment_time = datetime.now().isoformat()

    payment_event = {
        "payment_id": payment_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": amount,
        "payment_time": payment_time
    }
    return payment_event


def main():

    config = {
        "bootstrap.servers": "localhost:9092"
    }

    producer = Producer(config)

    topic = "payments"

    def delivery_callback(err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            print(
                textwrap.dedent(
                f"""
                    Produced event to topic {msg.topic()}:
                    key = {msg.key().decode('utf-8')}
                    value = {msg.value().decode('utf-8')}
                """)
            )

    while True:
        payment = generate_payment()
        print(f"Sending payment: {payment}")

        producer.produce(
            topic,
            key=str(payment["user_id"]),
            value=json.dumps(payment),
            callback=delivery_callback,
        )

        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    main()

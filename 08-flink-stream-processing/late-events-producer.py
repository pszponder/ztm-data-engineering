import json
import random
import textwrap
import time
from datetime import datetime, timedelta

from confluent_kafka import Producer


def generate_order():
    order_id = f"order-{random.randint(1000, 9999)}"
    customer_id = f"customer-{random.randint(1, 10)}"
    product_id = f"product-{random.randint(1, 200)}"
    quantity = random.randint(1, 5)
    price = round(random.uniform(5.0, 100.0), 2)
    current_time = datetime.now()

    if random.random() < 0.2:
        late_by = random.randint(60, 120)
        event_time = current_time - timedelta(seconds=late_by)
    else:
        event_time = current_time

    order_event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "order_time": event_time.isoformat()
    }
    return order_event


def main():

    config = {
        "bootstrap.servers": "localhost:9092"
    }

    producer = Producer(config)

    topic = "late-orders"

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
        order = generate_order()
        print(f"Sending order: {order}")

        producer.produce(
            topic,
            key=str(order["customer_id"]),
            value=json.dumps(order),
            callback=delivery_callback,
        )

        producer.poll(0)

        time.sleep(1)

if __name__ == "__main__":
    main()
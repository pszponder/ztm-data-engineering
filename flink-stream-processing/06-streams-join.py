import json
from dataclasses import dataclass
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeContext
)
from pyflink.datastream.functions import CoProcessFunction
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource


@dataclass
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    price: float
    timestamp: str

@dataclass
class Customer:
    customer_id: str
    name: str
    location: str


def parse_order(line: str):
    data = json.loads(line)
    return Order(
        order_id=data.get("order_id", ""),
        customer_id=data.get("customer_id", ""),
        product_id=data.get("product_id", ""),
        quantity=int(data.get("quantity", 0)),
        price=float(data.get("price", 0.0)),
        timestamp=data.get("timestamp", "")
    )

def parse_customer(line: str):
    data = json.loads(line)
    return Customer(
        customer_id=data.get("customer_id", ""),
        name=data.get("name", "Unknown"),
        location=data.get("location", "Unknown")
    )


class OrdersCustomersCoProcess(CoProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        customer_desc = ValueStateDescriptor("customer_info", Types.PICKLED_BYTE_ARRAY())
        self.customer_state = runtime_context.get_state(customer_desc)

    def process_element1(self, value, ctx):
        customer_val = self.customer_state.value()

        if customer_val:
            enriched = {
                "order_id": value.order_id,
                "customer_id": value.customer_id,
                "product_id": value.product_id,
                "quantity": value.quantity,
                "price": value.price,
                "order_timestamp": value.timestamp,
                "customer_name": customer_val.name,
                "customer_location": customer_val.location,
            }
        else:
            enriched = {
                "order_id": value.order_id,
                "customer_id": value.customer_id,
                "product_id": value.product_id,
                "quantity": value.quantity,
                "price": value.price,
                "order_timestamp": value.timestamp,
                "customer_name": "Unknown",
                "customer_location": "Unknown",
            }

        yield json.dumps(enriched)

    def process_element2(self, value, ctx):
        self.customer_state.update(value)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    orders_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("orders") \
        .set_group_id("streams_join_consumer") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    customers_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("customers") \
        .set_group_id("streams_join_consumer") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    orders_stream = env.from_source(
        source=orders_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="orders_source"
    ).map(parse_order)

    customers_stream = env.from_source(
        source=customers_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="customers_source"
    ).map(parse_customer)

    orders_stream.print("OrdersStream")
    customers_stream.print("CustomersStream")

    keyed_orders = orders_stream.key_by(lambda o: o.customer_id)
    keyed_customers = customers_stream.key_by(lambda c: c.customer_id)

    connected = keyed_orders.connect(keyed_customers)

    enriched_stream = connected.process(
        OrdersCustomersCoProcess(),
        output_type=Types.STRING()
    )

    enriched_stream.print("EnrichedOrder")

    env.execute("Joins demo")


if __name__ == "__main__":
    main()
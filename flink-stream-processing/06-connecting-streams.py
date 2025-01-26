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
    order_time: str

@dataclass
class Product:
    product_id: str
    name: str
    category: str


def parse_order(line: str):
    data = json.loads(line)
    return Order(
        order_id=data.get("order_id", ""),
        customer_id=data.get("customer_id", ""),
        product_id=data.get("product_id", ""),
        quantity=int(data.get("quantity", 0)),
        price=float(data.get("price", 0.0)),
        order_time=data.get("order_time", "")
    )

def parse_product(line: str):
    data = json.loads(line)
    return Product(
        product_id=data.get("product_id", ""),
        name=data.get("name", "Unknown"),
        category=data.get("category", "Unknown")
    )


class OrdersProductsCoProcess(CoProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        product_desc = ValueStateDescriptor("product_info", Types.PICKLED_BYTE_ARRAY())
        self.product_state = runtime_context.get_state(product_desc)

    def process_element1(self, value, ctx):
        product = self.product_state.value()

        if product:
            enriched = {
                "order_id": value.order_id,
                "customer_id": value.customer_id,
                "product_id": value.product_id,
                "quantity": value.quantity,
                "price": value.price,
                "product_name": product.name,
                "product_category": product.category,
            }
        else:
            enriched = {
                "order_id": value.order_id,
                "customer_id": value.customer_id,
                "product_id": value.product_id,
                "quantity": value.quantity,
                "price": value.price,
                "product_name": "Unknown",
                "product_category": "Unknown",
            }

        yield json.dumps(enriched)

    def process_element2(self, value, ctx):
        self.product_state.update(value)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    orders_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("orders") \
        .set_group_id("streams_join_consumer") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    products_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("products") \
        .set_group_id("streams_join_consumer") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    orders_stream = env.from_source(
        source=orders_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="orders_source"
    ).map(parse_order)

    products_stream = env.from_source(
        source=products_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="products_source"
    ).map(parse_product)

    products_stream.print("ProductsStream")

    keyed_orders = orders_stream.key_by(lambda o: o.product_id)
    keyed_products = products_stream.key_by(lambda c: c.product_id)

    connected = keyed_orders.connect(keyed_products)

    enriched_stream = connected.process(
        OrdersProductsCoProcess(),
        output_type=Types.STRING()
    )

    enriched_stream.print("EnrichedOrder")

    env.execute("Connecting streams")


if __name__ == "__main__":
    main()
import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import (
    StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
)
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.state import ValueState, ValueStateDescriptor



@dataclass
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    price: float
    order_time: str


def parse_order(json_str: str) -> Order:
    data = json.loads(json_str)
    return Order(
        order_id=data.get("order_id", "unknown"),
        customer_id=data.get("customer_id", "unknown"),
        product_id=data.get("product_id", "unknown"),
        quantity=int(data.get("quantity", 0)),
        price=float(data.get("price", 0.0)),
        order_time=data.get("order_time", "unknown")
    )


class LoyaltyTierFunction(KeyedProcessFunction):

    TIER_1_THRESHOLD = 300.0
    TIER_2_THRESHOLD = 1000.0

    def open(self, runtime_context: RuntimeContext):
        spend_desc = ValueStateDescriptor("total_spend", Types.FLOAT())
        self.total_spend_state: ValueState = runtime_context.get_state(spend_desc)

    def process_element(self, order: Order, ctx: 'KeyedProcessFunction.Context'):
        current_spend = self.total_spend_state.value()
        if current_spend is None:
            current_spend = 0.0

        order_total = order.price * order.quantity
        new_total_spend = current_spend + order_total
        self.total_spend_state.update(new_total_spend)

        if new_total_spend >= self.TIER_1_THRESHOLD:
            yield json.dumps({
                "customer_id": order.customer_id,
                "total_spend": new_total_spend,
                "tier": 1
            })

        if new_total_spend >= self.TIER_2_THRESHOLD:
            yield json.dumps({
                "customer_id": order.customer_id,
                "total_spend": new_total_spend,
                "tier": 2
            })


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("orders") \
        .set_group_id("customers-loyalty-tiers") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    orders_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    )

    orders_stream.print("Orders")

    loyalty_stream = (
        orders_stream
        .map(parse_order)
        .key_by(lambda o: o.customer_id)
        .process(LoyaltyTierFunction(), Types.STRING())
    )

    loyalty_stream.print("LoyaltyTierEvent")

    env.execute("Loyalty Tier Tracking")

if __name__ == "__main__":
    main()
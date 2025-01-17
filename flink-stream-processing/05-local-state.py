import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment
)
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueState, ValueStateDescriptor

from pyflink.common.typeinfo import Types
from pyflink.datastream.execution_mode import RuntimeExecutionMode

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment


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
        quantity=data.get("quantity", 0),
        price=float(data.get("price", 0.0)),
        order_time=data.get("order_time", "unknown")
    )


class RunningQuantityFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor("total-quantity", Types.LONG())

        self.total_quantity_state: ValueState = runtime_context.get_state(state_desc)

    def process_element(self, value, ctx):
        current_total = self.total_quantity_state.value()
        if current_total is None:
            current_total = 0

        new_total = current_total + value.quantity
        self.total_quantity_state.update(new_total)

        result = {
            "customer_id": value.customer_id,
            "updated_quantity_total": new_total
        }
        yield json.dumps(result)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("orders") \
        .set_group_id("flink-window-aggregation-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


    orders_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    )

    windowed_stream = orders_stream \
        .map(parse_order) \
        .key_by(lambda x: x.customer_id) \
        .process(RunningQuantityFunction(), Types.STRING())

    windowed_stream.print("UpdatedUserTotals")

    env.execute("Flink managed state")


if __name__ == "__main__":
    main()
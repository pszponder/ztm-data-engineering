import json
from dataclasses import dataclass
from datetime import datetime

from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction


@dataclass
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    price: float
    order_time: str


def parse_order(json_str) -> Order:
    data = json.loads(json_str)
    return Order(
        order_id=data.get("order_id", "unknown"),
        customer_id=data.get("customer_id", "unknown"),
        product_id=data.get("product_id", "unknown"),
        quantity=data.get("quantity", 0),
        price=float(data.get("price", 0.0)),
        order_time=data.get("order_time", "unknown")
    )


class AggregateWindowFunction(ProcessWindowFunction):
    def process(self,
                key,
                context,
                elements):

        total_quantity = 0
        total_sum = 0

        for input in elements:
            total_quantity += input.quantity
            total_sum += input.quantity * input.price

        result = {
            "product_id": key,
            "total_quantity": total_quantity,
            "total_spent": round(total_sum, 2),
            "window_start": datetime.utcfromtimestamp(
                context.window().start / 1000
            ).isoformat(),
            "window_end": datetime.utcfromtimestamp(
                context.window().end / 1000
            ).isoformat(),
        }
        return [json.dumps(result)]


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
        .key_by(lambda x: x.product_id) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .process(AggregateWindowFunction(),
                 Types.STRING())

    windowed_stream.print()

    env.execute("Window-based aggregation")


if __name__ == "__main__":
    main()
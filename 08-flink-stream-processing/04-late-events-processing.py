import json
from dataclasses import dataclass
from datetime import datetime

from pyflink.common import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag, StreamExecutionEnvironment, ProcessWindowFunction, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.window import TumblingEventTimeWindows

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
    order_time_seconds = datetime.fromisoformat(data["order_time"])
    return Order(
        order_id=data.get("order_id", "unknown"),
        customer_id=data.get("customer_id", "unknown"),
        product_id=data.get("product_id", "unknown"),
        quantity=data.get("quantity", 0),
        price=float(data.get("price", 0.0)),
        order_time=data.get("order_time", "unknown")
    )


class OrderTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        dt = datetime.fromisoformat(value.order_time)
        return int(dt.timestamp() * 1000)


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
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("late-orders") \
        .set_group_id("eventtime-demo") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    )

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
        .with_timestamp_assigner(OrderTimestampAssigner())

    late_tag = OutputTag("late-events", Types.PICKLED_BYTE_ARRAY())

    windowed_stream = stream \
        .map(parse_order) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x.product_id) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
        .side_output_late_data(late_tag) \
        .process(AggregateWindowFunction(), Types.STRING())

    windowed_stream.print("Aggregated")

    late_stream = windowed_stream.get_side_output(late_tag)
    late_stream.print("LateEvents")

    env.execute("Advanced Event-Time Window Demo")


if __name__ == "__main__":
    main()
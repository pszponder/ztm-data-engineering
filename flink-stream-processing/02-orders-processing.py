import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource, KafkaRecordSerializationSchema
from pyflink.datastream.execution_mode import RuntimeExecutionMode


@dataclass
class Order:
    order_id: str
    customer_id: str
    product_id: str
    quantity: int
    price: float
    order_time: str


def parse_order(json_str):
    data = json.loads(json_str)
    return Order(
        order_id=data.get("order_id", "unknown"),
        customer_id=data.get("customer_id", "unknown"),
        product_id=data.get("product_id", "unknown"),
        quantity=data.get("quantity", 0),
        price=float(data.get("price", 0.0)),
        order_time=data.get("order_time", "unknown")
    )


def filter_high_price(order):
    return order.price > 10


def convert_order(order):

    simplified = {
        "order_id": order.order_id,
        "price": order.price,
    }

    return json.dumps(simplified)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("orders") \
        .set_group_id("flink-consumer-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    orders_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    )

    filtered_stream = orders_stream \
        .map(parse_order) \
        .filter(filter_high_price) \
        .map(convert_order, Types.STRING())

    filtered_stream.print()

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("filtered-orders")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    filtered_stream.sink_to(kafka_sink)

    env.execute("Orders stream processing")


if __name__ == "__main__":
    main()
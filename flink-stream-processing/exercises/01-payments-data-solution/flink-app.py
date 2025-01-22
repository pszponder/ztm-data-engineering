import json
from dataclasses import dataclass

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource, KafkaRecordSerializationSchema
from pyflink.datastream.execution_mode import RuntimeExecutionMode


@dataclass
class Payment:
    payment_id: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    payment_time: str


def parse_payment(json_str):
    data = json.loads(json_str)
    return Payment(
        payment_id=data.get("payment_id", "unknown"),
        user_id=data.get("user_id", "unknown"),
        merchant_id=data.get("merchant_id", "unknown"),
        amount=float(data.get("amount", 0.0)),
        currency=data.get("currency", "unknown"),
        payment_time=data.get("payment_time", "unknown")
    )


def filter_high_amount(payment):
    return payment.amount > 500


def convert_payment(payment):
    simplified = {
        "payment_id": payment.payment_id,
        "amount": payment.amount,
    }
    return json.dumps(simplified)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("payments") \
        .set_group_id("flink-consumer-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    payments_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_source"
    )

    filtered_stream = payments_stream \
        .map(parse_payment) \
        .filter(filter_high_amount) \
        .map(convert_payment, Types.STRING())

    filtered_stream.print("FilteredStream")

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("filtered-payments")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    filtered_stream.sink_to(kafka_sink)

    env.execute("Payments stream processing")


if __name__ == "__main__":
    main()